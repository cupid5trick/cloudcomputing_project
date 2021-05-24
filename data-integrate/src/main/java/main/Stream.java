package main;

import kafka.ValidatedStream;

public class Stream {
    private static String kafkaIp = "192.168.43.128:9092";
    private static String inTopic = "json_input";
    private static String outTopic = "json_output";

    public static void main(String[] args) {
        ValidatedStream stream = new ValidatedStream(kafkaIp, inTopic, outTopic);
        Thread streamThread = new Thread() {
            @Override
            public void run() {
                stream.run();
            }
        };
        streamThread.start();
    }
}
