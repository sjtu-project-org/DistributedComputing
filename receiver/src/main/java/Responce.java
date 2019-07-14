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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

public class Responce implements HttpHandler{
    private Properties properties;
    Producer<String, String> producer = null;

    public static final String url = "jdbc:mysql://10.0.0.37/dslab";
    public static final String name = "com.mysql.cj.jdbc.Driver";
    public static final String user = "root";
    public static final String password = "123";

    public Connection conn = null;
    public PreparedStatement pst = null;
    static ResultSet ret = null;

    @Override
    public void handle(HttpExchange arg0) throws IOException
    {
        System.out.println("accept an query result from internet.....");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(arg0.getRequestBody()));

        String orderId = bufferedReader.readLine();

        String sql = "select * from result where id = " + orderId;
        System.out.println("sql:" + sql);
        JSONObject json = new JSONObject();

        try {
            Class.forName(name);
        }catch(ClassNotFoundException e){
            System.out.println("class not found");
            e.printStackTrace();
        }
        try{
            conn = DriverManager.getConnection(url,user,password);//获取连接
            pst = conn.prepareStatement(sql);//准备执行语句
            //start
            ret = pst.executeQuery();
            System.out.println("finish query");
            while (ret.next()) {
                System.out.println("loop");
                String id = ret.getString(1);
                String user_id = ret.getString(2);
                String initiator = ret.getString(3);
                String success = ret.getString(4);
                Double paid = ret.getDouble(5);
                json.put("order_id", id);
                json.put("user_id", user_id);
                json.put("initiator", initiator);
                json.put("success", success);
                json.put("paid", paid);

                System.out.println("response:" + json.toJSONString());
            }
            ret.close();
            conn.close();
            pst.close();

        }catch (SQLException e){
            System.out.println("connect mysql failed");
            e.printStackTrace();
        }

        String resp = json.toJSONString();
        arg0.sendResponseHeaders(200, resp.getBytes().length);
        OutputStream out = arg0.getResponseBody();
        out.write(resp.getBytes());
        out.flush();
        arg0.close();
    }
}