import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class mySimple {

    private static ZooKeeper authZK = null;

    /*
    * TODO: cannot use in windows
    *  why? difference between 192.168.x.x with 10.0.0.x ? */
    private static final String publicHost = "202.120.40.8:30361";

    private static final String intraHost = "10.0.0.52:2181";

    private static final String path = "/service";


    public static void main(String[] args) throws Exception {
        System.out.println("in main");

        final CountDownLatch latch = new CountDownLatch(1);
        authZK = new ZooKeeper(intraHost, 50000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("ZK connected");
                latch.countDown();
            }
        });
        latch.await();

    }
}
