package com.nyble.main;

import com.nyble.facades.kafkaConsumer.KafkaConsumerFacade;
import com.nyble.managers.ProducerManager;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.topics.Names;
import com.nyble.topics.consumer.ChangedProperty;
import com.nyble.topics.consumer.ConsumerKey;
import com.nyble.topics.consumer.ConsumerValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.*;
import java.util.Date;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static String groupId = "consumer-attributes-to-db";
    static Logger logger = LoggerFactory.getLogger(App.class);
    static Properties consumerProps = new Properties();
    static Properties producerProperties = new Properties();
    static ProducerManager producerManager;
    static Map<String, String> uniqueEntityAttributes = new HashMap<>();
    static {
        uniqueEntityAttributes.put("fullName", "full_name");
        uniqueEntityAttributes.put("phone", "phone");
        uniqueEntityAttributes.put("email", "email");
        uniqueEntityAttributes.put("location", "location");
        uniqueEntityAttributes.put("birthDate", "birth_date");
        uniqueEntityAttributes.put("entityId", "entity_id");
    }

    public static void initProps(){
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 5);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static void main(String[] args){
        SpringApplication.run(App.class, args);

        logger.debug("Initiate kafka clients properties");
        initProps();

        logger.debug("Initiate producers");
        producerManager = ProducerManager.getInstance(producerProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));

        logger.debug("Start new consumer for group {}", groupId);
        final int noOfConsumers = 4;
        KafkaConsumerFacade<String, String> kafkaConsumers = new KafkaConsumerFacade<>(consumerProps,
                noOfConsumers, KafkaConsumerFacade.PROCESSING_TYPE_BATCH);
        kafkaConsumers.subscribe(Collections.singletonList(Names.CONSUMER_ATTRIBUTES_TOPIC));
        kafkaConsumers.startPolling(Duration.ofSeconds(10), RecordProcessorImpl.class);
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
