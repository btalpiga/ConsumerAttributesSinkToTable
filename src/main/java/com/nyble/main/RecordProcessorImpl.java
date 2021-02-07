package com.nyble.main;

import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.util.DBUtil;
import com.nyble.utils.StringConverter;
import com.nyble.utils.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
        long start = System.currentTimeMillis();
        logger.info("start batch consumers");
        Map<String, Map<String, CAttribute>> consumers = batchConsumers(consumerRecords);
        long end = System.currentTimeMillis();
        logger.info("end batch consumers, took {}", (end - start));

        start = System.currentTimeMillis();
        logger.info("start batch commit");
        synchronized (RecordProcessorImpl.class) {
            try (Connection conn = DBUtil.getInstance().getConnection();
                 PreparedStatement query = conn.prepareStatement(queryStr);
                 PreparedStatement update = conn.prepareStatement(updateStr)) {

                boolean autoCommit = conn.getAutoCommit();
                conn.setAutoCommit(false);
                for (Map.Entry<String, Map<String, CAttribute>> consumer : consumers.entrySet()) {
                    String[] systemAndId = consumer.getKey().split("#");
                    int systemId = Integer.parseInt(systemAndId[0]);
                    int consumerId = Integer.parseInt(systemAndId[1]);
                    logger.info("start inserting consumer_id {} from system_id  {}", consumerId, systemId);
                    processConsumer(query, update, systemId, consumerId, consumer.getValue());
                    logger.info("end inserting consumer_id {} from system_id  {}", consumerId, systemId);
                }
                conn.commit();
                conn.setAutoCommit(autoCommit);

            } catch (SQLException e) {
                throw new RuntimeSqlException(e.getMessage(), e);
            }
        }
        end = System.currentTimeMillis();
        logger.info("end batch commit, took {} for {} consumers", (end-start), consumers.size());
        return true;
    }


    public Map<String, Map<String, CAttribute>> batchConsumers(ConsumerRecords<String, String> consumerRecords){
        Map<String, Map<String, CAttribute>> consumers = new HashMap<>();
        consumerRecords.forEach(consumerAttribute->{
            String key = consumerAttribute.key();
            String value = consumerAttribute.value();
            logger.debug("Key {}; value {};", key, value);
            if(key != null && !key.isEmpty()){
                logger.debug("Parsing key and value");
                ConsumerAttributesKey consumerAttributeKeyObj = (ConsumerAttributesKey) TopicObjectsFactory.fromJson(key, ConsumerAttributesKey.class);
                ConsumerAttributesValue attributeRec = (ConsumerAttributesValue) TopicObjectsFactory.fromJson(value, ConsumerAttributesValue.class);
                int systemId = consumerAttributeKeyObj.getSystemId();
                int consumerId = consumerAttributeKeyObj.getConsumerId();
                String propertyName =  attributeRec.getKey();
                String propertyValue = new StringConverter(attributeRec.getValue()).consumerAttributeNullEquivalence()
                        .coalesce("").get();
                String propertyLut = attributeRec.getExternalSystemDate();

                consumers.compute(systemId+"#"+consumerId, (mKey, mVal)->{
                    if(mVal == null){
                        mVal = new HashMap<>();
                    }
                    mVal.put(propertyName, new CAttribute(propertyValue, propertyLut));
                    return mVal;
                });
            }
        });
        return consumers;
    }

    public void processConsumer(PreparedStatement queryConsumer, PreparedStatement updateConsumer,
                                int systemId, int consumerId,
                                Map<String, CAttribute> consumerAttributes) throws SQLException {
        long start = System.currentTimeMillis();
        queryConsumer.setInt(1, systemId);
        queryConsumer.setInt(2, consumerId);
        ResultSet rs = queryConsumer.executeQuery();
        Consumer consumer;
        if(rs.next()){
            consumer = (Consumer) TopicObjectsFactory.fromJson(rs.getString(1), Consumer.class);
        }else{
            consumer = new Consumer();
        }
        rs.close();

        boolean isUpdated = false;
        boolean isDeduplicationFieldUpdated = false;
        String lastDeduplicationFieldUpdated = null;
        String lastDeduplicationOldValue = null;
        String lastDeduplicationNewValue = null;
        if(!consumer.hasProperty("systemId")){
            consumer.setProperty("systemId", new CAttribute(systemId+"", System.currentTimeMillis()+"0"));
            isUpdated = true;
        }
        if(!consumer.hasProperty("consumerId")){
            consumer.setProperty("consumerId", new CAttribute(consumerId+"", System.currentTimeMillis()+"0"));
            isUpdated = true;
        }
        for(Map.Entry<String, CAttribute> entry : consumerAttributes.entrySet()){
            String propToUpdate = entry.getKey();

            String lut = consumer.getTimestamp(propToUpdate);
            long propLastUpdated = 0;
            if(lut != null && !lut.isEmpty()){
                propLastUpdated = Long.parseLong(lut);
            }

            lut = entry.getValue().getLut();
            long newUpdateTime = System.currentTimeMillis();
            if(lut != null && !lut.isEmpty()){
                newUpdateTime = Long.parseLong(lut);
            }else{
                entry.getValue().setLut(newUpdateTime+"");
            }

            String oldValue = consumer.getValue(propToUpdate);
            String newValue = entry.getValue().getValue();
            if( (newValue.startsWith("+") || newValue.startsWith("-")) && (StringUtils.isNumerical(newValue.substring(1)))){
                int oldValueInt = Integer.parseInt(new StringConverter(oldValue).trim().nullIf("").coalesce("0").get());
                int origNewValue = Integer.parseInt(newValue);
                newValue = (oldValueInt + origNewValue)+"";
                newUpdateTime = System.currentTimeMillis();
                entry.getValue().setLut(newUpdateTime+"");
                entry.getValue().setValue(newValue+"");
                logger.debug("Received incremental value on property {} of consumer_id {} and system_id {} of amount {}, new amount = {}",
                        propToUpdate, consumerId, systemId, origNewValue, newValue);
            }

            if(!Objects.equals(oldValue, newValue) && newUpdateTime>= propLastUpdated){
                consumer.setProperty(entry.getKey(), entry.getValue());
                isUpdated = true;

                if(uniqueEntityAttributes.containsKey(propToUpdate)){
                    //update table
                    isDeduplicationFieldUpdated = true;
                    lastDeduplicationFieldUpdated = propToUpdate;
                    lastDeduplicationOldValue = oldValue;
                    lastDeduplicationNewValue = newValue;
                    String tableColumnName = uniqueEntityAttributes.get(propToUpdate);
                    String val = (newValue.isEmpty() ? null : newValue);
                    final String updateUniqueCriteria = String.format("INSERT INTO %s (system_id, consumer_id, %s) \n" +
                                    "values (%d,%d,"+(val!=null ? "'"+val+"'" : "NULL")+") \n" +
                                    "ON CONFLICT ON CONSTRAINT consumers_unique_entity_criterias_pkey DO \n" +
                                    "UPDATE set %s= excluded.%s",
                            tableUniqueEntityCriteria, tableColumnName, systemId, consumerId, tableColumnName, tableColumnName);
                    final Statement st = queryConsumer.getConnection().createStatement();
                    st.executeUpdate(updateUniqueCriteria);
                    st.close();
                    logger.debug("Updated deduplication field {} for consumer_id {} and system_id {}", propToUpdate,
                            consumerId, systemId);
                }
            }
        }

        if(isUpdated){
            PGobject jsonObj = new PGobject();
            jsonObj.setType("jsonb");
            jsonObj.setValue(consumer.toJson());
            updateConsumer.setInt(1, systemId);
            updateConsumer.setInt(2, consumerId);
            updateConsumer.setObject(3, jsonObj);
            updateConsumer.executeUpdate();

            if(isDeduplicationFieldUpdated){
                logger.debug("deduplication fields updated in consumer_id {} and system_id {}, send consumer to topic",
                        consumerId, systemId);
                sendToTopic(systemId, consumerId, consumer, lastDeduplicationFieldUpdated,
                        lastDeduplicationOldValue, lastDeduplicationNewValue);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("Consumer processing took {} millis", (end- start));
    }

}
