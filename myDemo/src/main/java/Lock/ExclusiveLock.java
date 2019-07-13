package Lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

enum  LockStatus {
    TRY_LOCK,
    LOCKED,
    UNLOCK
}

/*
* Exclusive-Distributed Lock
* */
public class ExclusiveLock  implements DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveLock.class);

    private static final String interHost = "10.0.0.52:2181";

    private static ZooKeeper authZK = null;

    private LockStatus lockStatus;

    private static final String LOCK_NODE_FULL_PATH = "/exclusive_lock/lock";

    private String id = String.valueOf(new Random(System.nanoTime()).nextInt(10000000));

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private CyclicBarrier lockBarrier = new CyclicBarrier(2);

    private static final long spinForTimeoutThreshold = 1000L;

    private static final long SLEEP_TIME = 100L;

    public ExclusiveLock() throws Exception {
        // connect to zookeeper
        authZK = new ZooKeeper(interHost, 50000, new LockNodeWatcher());
        lockStatus = LockStatus.UNLOCK;
        connectedSemaphore.await();
    }

    public void lock() throws Exception {
        if (lockStatus != LockStatus.UNLOCK) {
            return;
        }

        if (createLockNode()) {
            System.out.println("["+id+"]"+"get lock");
            lockStatus = LockStatus.LOCKED;
            return;
        }

        lockStatus = LockStatus.TRY_LOCK;
        lockBarrier.await();
    }

    public Boolean tryLock() {
        if (lockStatus != LockStatus.LOCKED) {
            return true;
        }

        Boolean created = createLockNode();
        lockStatus = created ? LockStatus.LOCKED : LockStatus.UNLOCK;
        return created;
    }

    public Boolean tryLock(long millisecond) throws Exception {
        long millisTimeout = millisecond;
        if (millisTimeout <= 0L) {
            return false;
        }

        final long deadline = System.currentTimeMillis() + millisTimeout;
        for (;;) {
            if (tryLock()) {
                return true;
            }

            if (millisTimeout > spinForTimeoutThreshold) {
                Thread.sleep(SLEEP_TIME);
            }

            millisTimeout = deadline - System.currentTimeMillis();
            if (millisTimeout <= 0L) {
                return false;
            }
        }
    }

    public void unlock() throws Exception {
        if (lockStatus == LockStatus.UNLOCK) {
            return;
        }

        deleteLockNode();
        lockStatus = LockStatus.UNLOCK;
        lockBarrier.reset();
        System.out.println("[" + id  + "]" + " 释放锁");
    }

    private Boolean createLockNode() {
        try {
            authZK.create(LOCK_NODE_FULL_PATH, "lock".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            return false;
        }

        return true;
    }

    private void deleteLockNode() throws KeeperException, InterruptedException {
        Stat stat = authZK.exists(LOCK_NODE_FULL_PATH, false);
        authZK.delete(LOCK_NODE_FULL_PATH, stat.getVersion());
    }

    class LockNodeWatcher implements Watcher {

        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected != event.getState()) {
                return;
            }

            // 2. 设置监视器
            try {
                authZK.exists(LOCK_NODE_FULL_PATH, this);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                connectedSemaphore.countDown();
            } else if (Event.EventType.NodeDeleted == event.getType()
                    && event.getPath().equals(LOCK_NODE_FULL_PATH)) {

                // 3. 再次尝试创建锁及诶单
                if (lockStatus == LockStatus.TRY_LOCK && createLockNode()) {
                    lockStatus = LockStatus.LOCKED;
                    try {
                        lockBarrier.await();
                        System.out.println("[" + id  + "]" + " 获取锁");
                        return;
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
