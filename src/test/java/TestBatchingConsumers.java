import com.nyble.facades.kafkaConsumer.KafkaConsumerFacade;
import com.nyble.main.RecordProcessorImpl;
import com.nyble.models.consumer.CAttribute;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.*;
import static org.junit.Assert.*;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

public class TestBatchingConsumers {

    RecordProcessorImpl recordProcessor;
    String topic = "consumer-attributes";
    Map<TopicPartition, List<ConsumerRecord<String, String>>> records ;
    Map<TopicPartition, Long> offsets;

    @BeforeEach
    public void setUp(){
        recordProcessor = new RecordProcessorImpl();
        records = new HashMap<>();
        offsets = new HashMap<>();
    }

    @Test
    public void testBatchConsumers(){
        String now = System.currentTimeMillis()+"";
        addConsumerRecord(new TopicPartition(topic, 0), new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "firstName", "AAAAAAA", now, now).toJson());
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);

        Map<String, Map<String, CAttribute>> consumersInRecords = recordProcessor.batchConsumers(consumerRecords);
        assertEquals(1, consumersInRecords.size());
    }

    @Test
    public void testBatchConsumers_multipleProps(){
        String now = System.currentTimeMillis()+"";
        TopicPartition tp0  = new TopicPartition(topic, 0);
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "firstName", "AAAAAAA", now, now).toJson());
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "phone", "128753648", now, now).toJson());
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 2).toJson(),
                new ConsumerAttributesValue("1", "2", "affinity_117", "5", now, now).toJson());
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);

        Map<String, Map<String, CAttribute>> consumersInRecords = recordProcessor.batchConsumers(consumerRecords);
        assertEquals(2, consumersInRecords.size());
    }

    @Test
    public void testBatchConsumers_multiConsumer_multiProps_multiPartitions(){
        String now = System.currentTimeMillis()+"";
        TopicPartition tp0  = new TopicPartition(topic, 0);
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "firstName", "AAAAAAA", now, now).toJson());
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "phone", "128753648", now, now).toJson());
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 2).toJson(),
                new ConsumerAttributesValue("1", "2", "affinity_117", "5", now, now).toJson());
        addConsumerRecord(tp0, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "firstName", "BBBB", now, now).toJson());

        TopicPartition tp1 = new TopicPartition(topic, 1);
        addConsumerRecord(tp1, new ConsumerAttributesKey(1, 1).toJson(),
                new ConsumerAttributesValue("1", "1", "affinity_125", "2", now, now).toJson());
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
        Map<String, Map<String, CAttribute>> consumersInRecords = recordProcessor.batchConsumers(consumerRecords);
        assertEquals(2, consumersInRecords.size());

        Map<String,CAttribute> firstConsumer = consumersInRecords.get("1#1");
        assertNotNull(firstConsumer);
        assertEquals("BBBB", firstConsumer.get("firstName").getValue());
        assertEquals("128753648", firstConsumer.get("phone").getValue());
        assertEquals("2", firstConsumer.get("affinity_125").getValue());

        Map<String,CAttribute> secondConsumer = consumersInRecords.get("1#2");
        assertNotNull(secondConsumer);
        assertEquals("5", secondConsumer.get("affinity_117").getValue());
    }

    private void addConsumerRecord(TopicPartition topicPartition, String consumerKeyJson, String consumerValueJson){
        long currentOffset = 0;
        if(offsets.containsKey(topicPartition)){
            currentOffset = offsets.get(topicPartition);
            currentOffset++;
        }
        offsets.put(topicPartition, currentOffset);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(),
                currentOffset, consumerKeyJson, consumerValueJson);
        ArrayList<ConsumerRecord<String, String>> rec = new ArrayList<>();
        rec.add(record);
        records.merge(topicPartition, rec, (exsList, newList)->{
            exsList.addAll(newList);
            return exsList;
        });
    }
}

