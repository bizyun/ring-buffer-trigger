package com.github.bizyun.ringbuffertrigger;

/**
 * @author zhangbiyun
 */
public interface RejectedEnqueueHandler<E> {

    void rejectEnqueue(E element);
}
