import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;

import Lock.FairLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class HttpReceiver
{
    private static ZooKeeper zkClient;
    private static final String interHost = "10.0.0.52:2181";

    private static final String ORDERID_ROOT = "/OrderIDs";

    public static void main(String[] args) throws Exception
    {
        HttpServerProvider provider = HttpServerProvider.provider();
        HttpServer server = provider.createHttpServer(new InetSocketAddress(30361), 10);

        zkClient= new ZooKeeper(interHost, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState()== Event.KeeperState.Disconnected){
                    System.out.println("HTTP Receiver lost ZK connection");
                }
            }
        });
        //如果根节点不存在，则创建根节点
        Stat stat = zkClient.exists(ORDERID_ROOT, false);
        if (stat == null) {
            zkClient.create(ORDERID_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        ProducerAndRecv prodRecv = new ProducerAndRecv(zkClient);
        server.createContext("/", prodRecv);

        server.setExecutor(null);
        server.start();
    }
}