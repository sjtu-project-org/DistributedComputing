import Lock.DistributedLock;
import Lock.ExclusiveLock;

import java.util.concurrent.CountDownLatch;

public class myLockDemo {
    private static DistributedLock lock = null;

    private static int counter = 0;

    public  static  void plus() throws InterruptedException {
        Thread.sleep(500);
        counter++;
        //System.out.println(counter);
    }

    public static int Count(){
        return counter;
    }

    public static void main(String[] args) throws Exception {
        lock = new ExclusiveLock();
        Integer var  = 0;

        final int loopTime = 20;
        CountDownLatch latch = new CountDownLatch(loopTime);

        System.out.println("begin loop");
        for (int i=0; i<loopTime; i++) {
            new Thread(() -> {
                try {
                    lock.lock();
                    plus();
                    System.out.println(Count());
                    latch.countDown();
                    lock.unlock();
                } catch (Exception e) {

                }
            }).start();
        }

        latch.await();
        System.out.println("end loop");
    }
}
