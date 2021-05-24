package kafka;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import redis.RedisPublisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ValidatedConsumer {
    private String clusterIp;
    private String topicName;

    public ValidatedConsumer() {}
    public ValidatedConsumer(String clusterIp, String topicName) {
        this.clusterIp = clusterIp;
        this.topicName = topicName;
    }

    public void consume(int timeout) {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.clusterIp);//kafka clousterIP
        props.put("group.id", "data-filter-output");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        System.out.println(props);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topicName));
        RedisPublisher publisher = new RedisPublisher();
        // consume record
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    publisher.publish("right_channel", record.value());
                }
            }
        }
        finally {
            consumer.close();
        }
    }

    public void consume() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.clusterIp);//kafka clousterIP
        props.put("group.id", "data-filter-output");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        RedisPublisher publisher = new RedisPublisher();
        // consume record
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                publisher.publish("right_channel", record.value());
            }
        }
    }
}
