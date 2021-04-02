package com.aperture.lock.zookeeper;

import com.aperture.lock.Lock;
import org.junit.Test;

public class ZooKeeperLockTest {
    @Test
    public void test() {
        String key = "key5";
        new Thread(() -> {
            Lock lock = new ZooKeeperLock("127.0.0.1:2181");
            if (lock.acquire(key, "value1", 1L)) {
                System.out.println("t1 get lock");
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.release(key, "value1");
        }).start();

        new Thread(() -> {
            Lock lock = new ZooKeeperLock("127.0.0.1:2181");
            if (lock.acquire(key, "value2", 1L)) {
                System.out.println("t2 get lock");
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.release(key, "value2");
        }).start();

        new Thread(() -> {
            Lock lock = new ZooKeeperLock("127.0.0.1:2181");
            if (lock.acquire(key, "value3", 1L)) {
                System.out.println("t3 get lock");
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.release(key, "value3");
        }).start();
        while (true) {
        }
    }
}
