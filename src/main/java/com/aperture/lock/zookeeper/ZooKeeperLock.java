package com.aperture.lock.zookeeper;

import com.aperture.lock.Lock;
import com.aperture.lock.LockException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZooKeeperLock implements Lock {

    private ZooKeeper zooKeeper;

    private final static String LOCK_PATH = "/lock/";

    public ZooKeeperLock(String addr) {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            zooKeeper = new ZooKeeper(addr, 6000,
                    e -> {
                        if (Watcher.Event.KeeperState.SyncConnected == e.getState()) {
                            System.out.println("connect success");
                            latch.countDown();
                        }
                    });
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new LockException("connect to zk timeout");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new LockException("connect to zk failed");
        }
    }

    // todo timeout
    public boolean acquire(String key, String value, Long time) {
        String path = create(key, value);
        String first = getFirstNode(key);
        while (!path.equals(first)) {
            String precedent = getPrecedent(key, path);
            if (precedent != null) {
                CountDownLatch latch = new CountDownLatch(1);
                watchDeleted(precedent, latch);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            first = getFirstNode(key);
        }
        return true;
    }

    public boolean release(String key, String value) {
        try {
            String first = getFirstNode(key);
            byte[] bytes = zooKeeper.getData(first, false, null);
            String str = new String(bytes, "UTF-8");
            if (!value.equals(str)) {
                System.out.println("release failed");
                return false;
            }
            zooKeeper.delete(first, -1);
            return true;
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new LockException("release failed");
        }
    }

    private String create(String key, String value) {
        try {
            Stat stat = zooKeeper.exists(keyRoot(key), false);
            if (stat == null) {
                System.out.println("root not exists, creating");
                // todo auto delete
                try {
                    zooKeeper.create(keyRoot(key), null,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (Exception e) {
                    System.out.println("create root failed");
                }
            }
            return zooKeeper.create(keyPath(key),
                    value.getBytes("UTF-8"),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new LockException("create node failed");
        }
    }

    private String getPrecedent(String key, String path) {
        List<String> children = getChildren(key);
        for (int i = 0; i < children.size(); i++) {
            if (path.equals(children.get(i))) {
                if (i == 0) {
                    return null;
                }
                return children.get(i - 1);
            }
        }
        throw new LockException("cannot find children");
    }

    private String getFirstNode(String key) {
        return getChildren(key).stream()
                .findFirst()
                .orElseThrow(() -> new LockException("cannot find children"));
    }

    private List<String> getChildren(String key) {
        try {
            return zooKeeper.getChildren(keyRoot(key), false)
                    .stream()
                    .sorted()
                    .map(x -> keyRoot(key) + "/" + x)
                    .collect(Collectors.toList());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw new LockException("get children failed");
        }
    }

    private void watchDeleted(String path, CountDownLatch latch) {
        try {
            zooKeeper.exists(path, e -> {
                // todo event refine
                System.out.println("node deleted listened");
                latch.countDown();
            });
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw new LockException("add watcher failed");
        }
    }

    private String keyPath(String key) {
        return LOCK_PATH + key + "/lock";
    }

    private String keyRoot(String key) {
        return LOCK_PATH + key;
    }
}
