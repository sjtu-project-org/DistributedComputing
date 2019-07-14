import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sun.net.httpserver.HttpHandler;
import org.apache.zookeeper.*;

public class Response2 implements HttpHandler{

    private static ZooKeeper zkClient;
    private static final String interHost = "10.0.0.52:2181";

    @Override
    public void handle(HttpExchange arg0) throws IOException
    {
        System.out.println("accept an query result from internet.....");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(arg0.getRequestBody()));
        String msg = bufferedReader.readLine();

        JSONObject json = new JSONObject();

        zkClient = new ZooKeeper(interHost, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState()== Event.KeeperState.Disconnected){
                    System.out.println("HTTP Receiver lost ZK connection");
                }
            }
        });
        Double d = -1.1;
        try {
            d = getTotalAmount(msg);
        }catch(InterruptedException | KeeperException e){
            System.out.println("getTotal error");
            e.printStackTrace();
        }

        String resp = String.valueOf(d);
        arg0.sendResponseHeaders(200, resp.getBytes().length);
        OutputStream out = arg0.getResponseBody();
        out.write(resp.getBytes());
        out.flush();
        arg0.close();
    }
    public static double getTotalAmount(String currency) throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/totalAmount/"+currency, false, null);
        return bytes2Double(data);
    }
    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }
}