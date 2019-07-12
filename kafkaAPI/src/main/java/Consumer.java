import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
    private Properties properties;
    private KafkaConsumer<String, String> kafkaConsumer = null;

    Consumer(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "10.0.0.52:9092, 10.0.0.34:9092, 10.0.0.101:9092");
        prop.put("group.id", "group-1");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("auto.offset.reset", "earliest");
        //prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.properties = prop;
    }

    public void RecvData(){
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("helloworld"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
        //kafkaConsumer.close();
    }

    public static void main(String[] args){
        Consumer consumer = new Consumer();
        consumer.RecvData();
    }
}
