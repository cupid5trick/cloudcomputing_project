package kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import com.google.gson.Gson;
import data.Record;

import redis.RedisPublisher;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ValidatedStream {
    private String input_topic;
    private String output_topic;
    private String kafkaIp;

    public ValidatedStream(String kafkaIp, String in, String out) {
        this.input_topic = in;
        this.output_topic = out;
        this.kafkaIp = kafkaIp;
    }

    public void run() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(
            this.input_topic,
                Consumed.with(
                        Serdes.String(),
                        Serdes.String()
                )
        );
        // filter illegal records
        source.filterNot(
            new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    Gson gson = new Gson();
//                    System.out.println(key);
//                    System.out.println(value);
                    Record record = gson.fromJson(value, Record.class);
                    Boolean illegal = record.getLongitude() > 130 && record.getLatitude() > 40;
                    // publish the illegal data to wrong channel
                    if (illegal) {
                        new RedisPublisher().publish("wrong_channel", value);
                    }
                    return illegal;
                }
            }
        ).to(
            output_topic,
            Produced.with(
                    Serdes.String(),
                    Serdes.String()
            )
        );

        Topology topology = builder.build();
        System.out.println(topology.describe());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "data-filter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaIp);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }
}
