import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

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
    private static String[] currencyType = new String[]{"RMB", "USD", "JPY", "EUR"};
    private static final int[] currencyLow = new int[]{140, 840, 10, 630};
    private static final int[] currencyHigh = new int[]{260, 1560, 20, 1170};

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
        for (int i = 0; i< 4; i++) {
            String newNodeString = authZK.create("/Currency/RMB", double2Bytes(d), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            byte[] data = authZK.getData("/RMB", false, null);
            System.out.println(bytes2Double(data));
        }
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
                    for (int i=0; i<4; i++) {
                        authZK.create("/Currency/"+currencyType[i], double2Bytes(d), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        final int index = i;
                        final long interval = 15000;
                        new Thread(() -> {
                            try {
                                while (true) {
                                    int low = currencyLow[index];
                                    int high = currencyHigh[index];
                                    Random rand = new Random();
                                    double newValue = (rand.nextInt(high-low+1)+low) / 100.0;
                                    authZK.setData("/Currency/"+currencyType[index], double2Bytes(newValue), -1);
                                    byte[] data = authZK.getData("/Currency/"+currencyType[index], false, null);
                                    System.out.println("Currency type: "+ currencyType[index]+" currency value: "+bytes2Double(data));
                                    Thread.sleep(interval);
                                }

                            } catch (Exception e) {

                            }
                        }).start();
                    }
                    //testCreateNode();
                    //setNodeData();
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

    public static void test(String[] args) {
        for (int i=0; i<4; i++) {
            final int index = i;
            final long interval = 15000;
            new Thread(() -> {
                try {
                    while (true) {
                        int low = currencyLow[index];
                        int high = currencyHigh[index];
                        Random rand = new Random();
                        double newValue = (rand.nextInt(high-low+1)+low) / 100.0;
                        System.out.println("thread number: "+index+" The value is"+newValue);
                        Thread.sleep(interval);
                    }

                } catch (Exception e) {

                }
            }).start();
        }
    }
}
