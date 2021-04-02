package com.aperture.lock;

public interface Lock {
    boolean acquire(String key, String value, Long time);

    boolean release(String key, String value);
}
