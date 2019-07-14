import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class HttpReceiver
{
    private static final String interHost = "10.0.0.52:2181";

    private static final String ORDERID_ROOT = "/OrderIDs";

    public static void main(String[] args) throws Exception
    {
        HttpServerProvider provider = HttpServerProvider.provider();
        HttpServer server = provider.createHttpServer(new InetSocketAddress(30368), 10);

        Responce prodRecv = new Responce();
        server.createContext("/", prodRecv);

        server.setExecutor(null);
        server.start();
    }
}