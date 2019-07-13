import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class mySimple {

    private static ZooKeeper authZK = null;

    /*
     * TODO: cannot use in windows
     *  why? difference between 192.168.x.x with 10.0.0.x ? */
    private static final String publicHost = "202.120.40.8:30361";

    private static final String intraHost = "10.0.0.52:2181";

    private static final String path = "/service";

    private static Double d = 2.0;

    public static byte[] double2Bytes(double d) {
        long value = Double.doubleToRawLongBits(d);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static void testCreateNode() throws KeeperException, InterruptedException{
        String newNodeString = authZK.create("/Currency/RMB", double2Bytes(d),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        byte[] data = authZK.getData("/RMB", false, null);
        System.out.println(bytes2Double(data));
    }

    public static void setNodeData() throws KeeperException, InterruptedException{
        authZK.setData("/RMB", double2Bytes(2.5), -1);
        byte[] data = authZK.getData("/RMB", false, null);
        System.out.println(bytes2Double(data));
    }



    public static void main(String[] args) throws KeeperException, InterruptedException,Exception {
        System.out.println("in main");

        final CountDownLatch latch = new CountDownLatch(1);
        authZK = new ZooKeeper(intraHost, 50000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("ZK connected");
                try {
                    testCreateNode();
                    setNodeData();
                }
                catch (KeeperException e) {
                    e.printStackTrace();
                }
                catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                System.out.println("finish");
                latch.countDown();
            }
        });
        latch.await();

    }
}
