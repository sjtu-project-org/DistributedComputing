import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Properties;

import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sun.net.httpserver.HttpHandler;

public class ProducerAndRecv implements HttpHandler{
    private Properties properties;
    Producer<String, String> producer = null;

    private static final String topic = "test1";

    @Override
    public void handle(HttpExchange arg0) throws IOException
    {
        System.out.println("accept an exchange from internet.....");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(arg0.getRequestBody()));

        String msg = bufferedReader.readLine();
        System.out.println(msg);
        SendMsg2Kafka(msg);

        /* TODO: add a order id generator */

        String resp = "your request message i get it!";
        arg0.sendResponseHeaders(200, resp.getBytes().length);
        OutputStream out = arg0.getResponseBody();
        out.write(resp.getBytes());
        out.flush();

        arg0.close();
    }

    ProducerAndRecv(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "10.0.0.52:9092, 10.0.0.34:9092, 10.0.0.101:9092");
        prop.put("acks", "all");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("linger.ms", 1);
        /*prop.put("buffer.memory", 33554432);*/
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.properties = prop;
    }

    public void SendMsg2Kafka(String msg){
        //create a new producer
        producer = new KafkaProducer<String, String>(this.properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        producer.send(record);

        producer.close();
    }

    public static void main(String args[]){
        ProducerAndRecv prodRecv = new ProducerAndRecv();
        /*for(int i = 0; i < 5; ++i){
            String s = "message:" + String.valueOf(i);
            prodRecv.SendMsg(s);
        }*/
    }
}
