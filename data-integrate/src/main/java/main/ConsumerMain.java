package main;

import com.google.gson.Gson;
import data.Record;
import hbase.HBaseInsert;
import kafka.ValidatedConsumer;
import kafka.ValidatedProducer;
import redis.RedisSubscriber;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;


public class ConsumerMain {
    private static String kafkaIp = "192.168.43.130:9092";
    private static String producerIp = "192.168.43.128:9092";
    private static String consumerIp = "192.168.43.129:9092";
    private static String inTopic = "json_input";
    private static String outTopic = "json_output";
    private static String rightChannel = "right_channel";
    private static String wrongChannel = "wrong_channel";

    // 2. 启动消费者把正确数据发送到redis正确数据的信道，错误数据由流处理器发送到错误数据的信道
    static void kafkaToRedis() {
        ValidatedConsumer consumer = new ValidatedConsumer(consumerIp, outTopic);
        consumer.consume();
    }

    // 3. 启动两个信道的redis订阅，把接收的数据存入hbase
    static void redisToHbase() {
        HBaseInsert right = new HBaseInsert("Record2");
        HBaseInsert wrong = new HBaseInsert("IllegalRecord2");
        RedisSubscriber rightSubscriber = new RedisSubscriber(
                rightChannel,
                new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        super.onMessage(channel, message);
//                        System.out.println(message);
                        Gson gson = new Gson();
                        Record record = gson.fromJson(message, Record.class);
                        right.insertSingleRecord(record);
                    }
                }
        );
        RedisSubscriber wrongSubscriber = new RedisSubscriber(
                wrongChannel,
                new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        super.onMessage(channel, message);
                        System.out.println("Illegal record" + message);
                        Gson gson = new Gson();
                        Record record = gson.fromJson(message, Record.class);
                        wrong.insertSingleRecord(record);
                    }
                }
        );
        rightSubscriber.start();
        wrongSubscriber.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        HBaseCreateOP.CreateTable("Record", Arrays.asList("info"));
//        HBaseCreateOP.CreateTable("IllegalRecord", Arrays.asList("info"));



        // 1. json to kafka
//        jsonToKafka();
        // 3. redis to hbase
        redisToHbase();
        // 2. kafka to redis
        kafkaToRedis();
    }


}
