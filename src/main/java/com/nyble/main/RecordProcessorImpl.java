package com.nyble.main;

import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.util.DBUtil;
import com.nyble.utils.StringConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Duration;
import java.util.Objects;

import static com.nyble.main.App.sendToTopic;
import static com.nyble.main.App.uniqueEntityAttributes;

public class RecordProcessorImpl implements RecordProcessor<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(RecordProcessorImpl.class);
    final static String queryStr = "SELECT payload from consumers where system_id = ? and consumer_id = ?";
    final static String updateStr = "INSERT INTO consumers (system_id, consumer_id, payload, updated_at) values (?, ?, ?, now()) \n" +
            "on conflict on constraint consumers_pk do update set payload=excluded.payload, updated_at = now()";
    final static String tableUniqueEntityCriteria = "consumers_unique_entity_criterias";

    @Override
    public boolean process(ConsumerRecord<String, String> consumerRecord) {
        throw new UnsupportedOperationException("Processing single kafka consumer record in ConsumerAttributesSinktToTable not supported");
    }

    @Override
    public boolean processBatch(ConsumerRecords<String, String> consumerRecords) {

        try(Connection conn = DBUtil.getInstance().getConnection();
            PreparedStatement query = conn.prepareStatement(queryStr);
            PreparedStatement update = conn.prepareStatement(updateStr)){

            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            for(ConsumerRecord<String, String> rec : consumerRecords){
                processRecord(rec, query, update);
            }

            conn.commit();
            conn.setAutoCommit(autoCommit);

        } catch (SQLException e) {
            throw new RuntimeSqlException(e.getMessage(), e);
        }
        return true;
    }



    public void processRecord(ConsumerRecord<String, String> rec, PreparedStatement query, PreparedStatement update) throws SQLException {
        long start = System.currentTimeMillis();
        String key = rec.key();
        String value = rec.value();
        logger.debug("Key {}; value {};", key, value);
        if(key != null && !key.isEmpty()){
            logger.debug("Parsing key and value");
            ConsumerAttributesKey consumerAttributeKeyObj = (ConsumerAttributesKey) TopicObjectsFactory.fromJson(key, ConsumerAttributesKey.class);
            ConsumerAttributesValue attributeRec = (ConsumerAttributesValue) TopicObjectsFactory.fromJson(value, ConsumerAttributesValue.class);
            int systemId = consumerAttributeKeyObj.getSystemId();
            int consumerId = consumerAttributeKeyObj.getConsumerId();
            String propertyName =  attributeRec.getKey();
            String newValue = new StringConverter(attributeRec.getValue()).consumerAttributeNullEquivalence()
                    .coalesce("").get();
            query.setInt(1, systemId);
            query.setInt(2, consumerId);
            ResultSet rs = query.executeQuery();
            Consumer consumer;
            if(rs.next()){
                consumer = (Consumer) TopicObjectsFactory.fromJson(rs.getString(1), Consumer.class);
            }else{
                consumer = new Consumer();
            }
            rs.close();

            long previousUpdated = 0;
            String oldValue = null;
            if(consumer.hasProperty(propertyName)){
                previousUpdated = Long.parseLong(consumer.getTimestamp(propertyName));
                oldValue = consumer.getValue(propertyName);
            }

            if(Long.parseLong(attributeRec.getExternalSystemDate()) > previousUpdated && !Objects.equals(newValue, oldValue)){
                consumer.setProperty(propertyName, new CAttribute(newValue, attributeRec.getExternalSystemDate()));
                PGobject jsonObj = new PGobject();
                jsonObj.setType("jsonb");
                jsonObj.setValue(consumer.toJson());
                update.setInt(1, systemId);
                update.setInt(2, consumerId);
                update.setObject(3, jsonObj);
                update.executeUpdate();
                logger.info("Process until unique criteria took {} millis", (System.currentTimeMillis()-start));

                if(uniqueEntityAttributes.containsKey(propertyName)){
                    //update table
                    String tableColumnName = uniqueEntityAttributes.get(propertyName);
                    String val = (newValue.isEmpty() ? null : newValue);
                    final String updateUniqueCriteria = String.format("INSERT INTO %s (system_id, consumer_id, %s) \n" +
                            "values (%d,%d,"+(val!=null ? "'"+val+"'" : "NULL")+") \n" +
                            "ON CONFLICT ON CONSTRAINT consumers_unique_entity_criterias_pkey DO \n" +
                            "UPDATE set %s= excluded.s",
                            tableUniqueEntityCriteria, tableColumnName, systemId, consumerId, tableColumnName, tableColumnName);
                    final Statement st = update.getConnection().createStatement();
                    st.executeUpdate(updateUniqueCriteria);
                    st.close();
                }

                sendToTopic(systemId, consumerId, consumer, propertyName, oldValue, newValue);
            }


        }
        long end = System.currentTimeMillis();
        logger.info("Record processing took {} millis", (end- start));
    }
}
