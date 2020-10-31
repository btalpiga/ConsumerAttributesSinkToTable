package com.nyble.main;

import com.nyble.managers.ProducerManager;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumer.ChangedProperty;
import com.nyble.topics.consumer.ConsumerKey;
import com.nyble.topics.consumer.ConsumerValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.util.DBUtil;
import com.nyble.utils.StringConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static String groupId = "consumer-attributes-to-db";
    static Logger logger = LoggerFactory.getLogger(App.class);
    static Properties consumerProps = new Properties();
    static Properties producerProperties = new Properties();
    static ProducerManager producerManager;
    static Map<String, String> uniqueEntityAttributes = new HashMap<>();
    static {
        consumerProps.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("group.id", groupId);
        consumerProps.put("max.poll.records", 100);

        producerProperties.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 5);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());

        producerManager = ProducerManager.getInstance(producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));

        uniqueEntityAttributes.put("fullName", "full_name");
        uniqueEntityAttributes.put("phone", "phone");
        uniqueEntityAttributes.put("email", "email");
        uniqueEntityAttributes.put("location", "location");
        uniqueEntityAttributes.put("birthDate", "birth_date");
        uniqueEntityAttributes.put("entityId", "entity_id");
    }


    public static void main(String[] args){

        logger.debug("Start new consumer for group {}", groupId);
        final int noOfConsumers = 4;
        final String queryStr = "SELECT payload from consumers where system_id = ? and consumer_id = ?";
        final String updateStr = "INSERT INTO consumers (system_id, consumer_id, payload, updated_at) values (?, ?, ?, now()) \n" +
                "on conflict on constraint consumers_pk do update set payload=excluded.payload, updated_at = now()";
        for(int i=0;i<noOfConsumers; i++){
            new Thread(()->{
                KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
                kafkaConsumer.subscribe(Collections.singletonList(Names.CONSUMER_ATTRIBUTES_TOPIC));
                logger.debug("Consumers connected to topic {}", Names.CONSUMER_ATTRIBUTES_TOPIC);
                try(Connection conn = DBUtil.getInstance().getConnection();
                    PreparedStatement query = conn.prepareStatement(queryStr);
                    PreparedStatement update = conn.prepareStatement(updateStr)){
                    while(true){
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(1000*10);
                        logger.info("Finished poll, records size = {}",records.count());
                        try{
                            records.forEach(record ->processRecord(record, query, update));
                        }catch(Exception e){
                            logger.error(e.getMessage(), e);
                        }
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }).start();
        }

    }

    public static void processRecord(ConsumerRecord<String, String> rec, PreparedStatement query, PreparedStatement update)  {
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
            try{
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
                    update.setInt(1, systemId);
                    update.setInt(2, consumerId);
                    PGobject jsonObj = new PGobject();
                    jsonObj.setType("jsonb");
                    jsonObj.setValue(consumer.toJson());
                    update.setObject(3, jsonObj);
                    update.executeUpdate();
                    logger.info("Process until unique criteria took {} millis", (System.currentTimeMillis()-start));
                    if(uniqueEntityAttributes.containsKey(propertyName)){
                        //update table
                        String tableColumnName = uniqueEntityAttributes.get(propertyName);
                        String val = (newValue.isEmpty() ? null : newValue);
                        final String updateUniqueCriteria = "INSERT INTO consumers_unique_entity_criterias " +
                                "(system_id, consumer_id, "+tableColumnName+") values ("+systemId+","+consumerId+","+
                                (val!=null ? "'"+val+"'" : "NULL")+") " +
                                "ON CONFLICT ON CONSTRAINT consumers_unique_entity_criterias_pkey DO " +
                                "UPDATE set "+tableColumnName+"= excluded."+tableColumnName;
                        final Statement st = update.getConnection().createStatement();
                        st.executeUpdate(updateUniqueCriteria);
                        st.close();
                    }
                    sendToTopic(systemId, consumerId, consumer, propertyName, oldValue, newValue);

                }
            } catch(Exception e){
                logger.error(e.getMessage(), e);
            }

        }
        long end = System.currentTimeMillis();
        logger.info("Record processing took {} millis", (end- start));
    }

    public static void sendToTopic(int systemId, int consumerId, Consumer fullConsumer,
                                   String attributeName, String oldValue, String newValue){
        if(!fullConsumer.hasProperty("consumerId")){
            fullConsumer.setProperty("consumerId", new CAttribute(consumerId+"", new Date().getTime()+""));
        }
        if(!fullConsumer.hasProperty("systemId")){
            fullConsumer.setProperty("systemId", new CAttribute(systemId+"", new Date().getTime()+""));
        }
        //notify that this consumer details were updated, sending last consumer state and last updated value
        ConsumerValue consumerMessageValue = new ConsumerValue(fullConsumer,
                new ChangedProperty(attributeName, oldValue, newValue));
        ConsumerKey consumerMessageKey = new ConsumerKey(systemId, consumerId);
        logger.debug("Producing consumer to topic");
        ProducerRecord<String, String> consumerMessage = new ProducerRecord<>(Names.CONSUMERS_TOPIC,
                consumerMessageKey.toJson(), consumerMessageValue.toJson());
        KafkaProducer<String, String> producerConsumerTopic = producerManager.getProducer();
        producerConsumerTopic.send(consumerMessage);
        logger.debug("Sent consumer to topic");

    }

}
