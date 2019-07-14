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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ProducerAndRecv implements HttpHandler{
    private Properties properties;
    Producer<String, String> producer = null;

    private static final String topic = "test1";

    private static ZooKeeper zkClient;
    private static String ORDER_PATH = "/OrderIDs/Order";

    @Override
    public void handle(HttpExchange arg0) throws IOException
    {
        System.out.println("accept an exchange from internet.....");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(arg0.getRequestBody()));

        String msg = bufferedReader.readLine();
        System.out.println(msg);

        /* TODO: add a order id generator */
        String orderPath = null;
        try {
            orderPath = zkClient.create(ORDER_PATH,
                    Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(orderPath);

        } catch (KeeperException | InterruptedException e) {
            System.out.println("HTTP handler orderID wrong");
        }

        String orderId = orderPath.substring(ORDER_PATH.length() + 1);

        String Msg2Kafka = String.format("{order_id : %s, msg : %s}", orderId, msg);
        SendMsg2Kafka(Msg2Kafka);

        String resp = "your request message already received," + "orderID : "+ orderId;
        arg0.sendResponseHeaders(200, resp.getBytes().length);
        OutputStream out = arg0.getResponseBody();
        out.write(resp.getBytes());
        out.flush();

        arg0.close();
    }

    public ProducerAndRecv(ZooKeeper zk){
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
        this.zkClient = zk;
    }

    public void SendMsg2Kafka(String msg){
        //create a new producer
        producer = new KafkaProducer<String, String>(this.properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        producer.send(record);

        producer.close();
    }

    public static void main(String args[]){
        //ProducerAndRecv prodRecv = new ProducerAndRecv();
        /*for(int i = 0; i < 5; ++i){
            String s = "message:" + String.valueOf(i);
            prodRecv.SendMsg(s);
        }*/
    }
}
