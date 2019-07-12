import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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