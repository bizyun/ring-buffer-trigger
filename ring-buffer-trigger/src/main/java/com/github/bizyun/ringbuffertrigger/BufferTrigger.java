package com.github.bizyun.ringbuffertrigger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangbiyun
 */
public interface BufferTrigger<E> extends AutoCloseable {

    boolean start();

    void enqueue(E e);

    void trigger();

    void close();

    void close(long timeout, TimeUnit timeUnit) throws TimeoutException;
}
