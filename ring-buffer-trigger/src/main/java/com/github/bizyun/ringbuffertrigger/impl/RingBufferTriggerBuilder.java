package com.github.bizyun.ringbuffertrigger.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import javax.annotation.Nonnull;

import com.github.bizyun.ringbuffertrigger.BufferTrigger;
import com.github.bizyun.ringbuffertrigger.RejectedEnqueueHandler;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author zhangbiyun
 */
public class RingBufferTriggerBuilder<E, C> {

    private static final int DEFAULT_BUFFER_SIZE = 256;

    private Supplier<? extends C> containerFactory;
    private ToIntBiFunction<? super C, ? super E> containerAdder;
    private Consumer<? super C> consumer;
    private BiConsumer<Throwable, ? super C> exceptionHandler;
    private Supplier<Integer> batchConsumeSize;
    private Executor consumerExecutor;
    private int bufferSize;
    private RejectedEnqueueHandler rejectedEnqueueHandler;
    private long delay;
    private TimeUnit delayTimeUnit;
    private String bizName;
    private boolean enableBackPressure;

    public Supplier<? extends C> getContainerFactory() {
        return containerFactory;
    }

    /**
     * 不要求容器线程安全
     */
    public RingBufferTriggerBuilder<E, C> setContainerFactory(
            @Nonnull Supplier<? extends C> containerFactory) {
        this.containerFactory = containerFactory;
        return this;
    }

    ToIntBiFunction<? super C, ? super E> getContainerAdder() {
        return containerAdder;
    }

    public RingBufferTriggerBuilder<E, C> setContainerAdderEx(@Nonnull ToIntBiFunction<? super C, ? super E> containerAdder) {
        this.containerAdder = containerAdder;
        return this;
    }

    public RingBufferTriggerBuilder<E, C> setContainerAdder(@Nonnull BiPredicate<? super C, ? super E> containerAdder) {
        this.containerAdder = (c, e) -> containerAdder.test(c, e) ? 1 : 0;
        return this;
    }

    Consumer<? super C> getConsumer() {
        return consumer;
    }

    public RingBufferTriggerBuilder<E, C> setConsumer(
            @Nonnull Consumer<? super C> consumer) {
        this.consumer = consumer;
        return this;
    }

    BiConsumer<Throwable, ? super C> getExceptionHandler() {
        return exceptionHandler;
    }

    public RingBufferTriggerBuilder<E, C> setExceptionHandler(
            @Nonnull BiConsumer<Throwable, ? super C> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    Supplier<Integer> getBatchConsumeSize() {
        return batchConsumeSize;
    }

    public RingBufferTriggerBuilder<E, C> setBatchConsumeSize(int size) {
        checkState(size > 0);
        this.batchConsumeSize = () -> size;
        return this;
    }

    public RingBufferTriggerBuilder<E, C> setBatchConsumeSize(@Nonnull Supplier<Integer> batchSize) {
        this.batchConsumeSize = batchSize;
        return this;
    }

    Executor getConsumerExecutor() {
        return consumerExecutor;
    }

    public RingBufferTriggerBuilder<E, C> setConsumerExecutor(@Nonnull Executor consumerExecutor) {
        this.consumerExecutor = consumerExecutor;
        return this;
    }

    int getBufferSize() {
        return bufferSize;
    }

    public RingBufferTriggerBuilder<E, C> setBufferSize(int bufferSize) {
        checkState(bufferSize > 0);
        this.bufferSize = bufferSize;
        return this;
    }

    RejectedEnqueueHandler getRejectedEnqueueHandler() {
        return rejectedEnqueueHandler;
    }

    public RingBufferTriggerBuilder<E, C> setRejectedEnqueueHandler(@Nonnull RejectedEnqueueHandler rejectedEnqueueHandler) {
        this.rejectedEnqueueHandler = rejectedEnqueueHandler;
        return this;
    }

    long getDelay() {
        return delay;
    }

    TimeUnit getDelayTimeUnit() {
        return delayTimeUnit;
    }

    public RingBufferTriggerBuilder<E, C> setScheduledFixDelay(long delay, @Nonnull TimeUnit timeUnit) {
        checkState(delay > 0);
        this.delay = delay;
        this.delayTimeUnit = timeUnit;
        return this;
    }

    String getBizName() {
        return bizName;
    }

    public RingBufferTriggerBuilder<E, C> setBizName(String bizName) {
        checkState(isNotBlank(bizName));
        this.bizName = bizName;
        return this;
    }

    boolean isEnableBackPressure() {
        return enableBackPressure;
    }

    public RingBufferTriggerBuilder<E, C> enableBackPressure() {
        this.enableBackPressure = true;
        return this;
    }

    public BufferTrigger<E> build() {
        ensure();
        return new RingBufferTrigger<>(this);
    }

    private void ensure() {
        checkNotNull(containerFactory);
        checkNotNull(containerAdder);
        checkNotNull(consumer);
        checkNotNull(batchConsumeSize);
        checkState(isNotBlank(bizName));

        if (exceptionHandler == null) {
            exceptionHandler = (t, c) -> { };
        }
        if (consumerExecutor == null) {
            consumerExecutor = MoreExecutors.directExecutor();
        }
        if (bufferSize <= 0) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        } else if (Integer.bitCount(bufferSize) != 1) {
            bufferSize = Integer.highestOneBit(bufferSize);
        }
        if (rejectedEnqueueHandler == null) {
            rejectedEnqueueHandler = e -> { };
        }
    }
}
