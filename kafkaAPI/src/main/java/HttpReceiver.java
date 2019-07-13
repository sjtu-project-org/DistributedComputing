import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;

public class HttpReceiver
{
    public static void main(String[] args) throws Exception
    {
        HttpServerProvider provider = HttpServerProvider.provider();
        HttpServer server = provider.createHttpServer(new InetSocketAddress(30361), 10);

        ProducerAndRecv prodRecv = new ProducerAndRecv();
        server.createContext("/", prodRecv);

        server.setExecutor(null);
        server.start();
    }
}