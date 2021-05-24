package kafka;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class ValidatedProducer {

    private String clusterIp;
    private String topicName;

    public ValidatedProducer(String clusterIp, String topicName){
        this.clusterIp = clusterIp;
        this.topicName = topicName;
    }

    public void period_send(int millis) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.clusterIp);//kafka clusterIP
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        BufferedReader br =  new BufferedReader(new FileReader("./data/record.json"));//record file path
        Producer<String, String> producer = new KafkaProducer<>(props);
        int i = 0;//message key
        String record;
        //send record to kafka
        while((record = br.readLine())!=null) {
            producer.send(new ProducerRecord<String, String>(this.topicName, Integer.toString(i), record), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                }
            });
            Thread.sleep(millis);
            i++;
        }
        producer.close();
    }

    public void send() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.clusterIp);//kafka clusterIP
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        BufferedReader br =  new BufferedReader(new FileReader("./data/record.json"));//record file path
        Producer<String, String> producer = new KafkaProducer<>(props);
        int i = 0;//message key
        String record;
        //send record to kafka
        while((record = br.readLine())!=null) {
            producer.send(new ProducerRecord<String, String>(this.topicName, Integer.toString(i), record), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        e.printStackTrace();
                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                }
            });
            i++;
        }
        producer.close();
    }

}
