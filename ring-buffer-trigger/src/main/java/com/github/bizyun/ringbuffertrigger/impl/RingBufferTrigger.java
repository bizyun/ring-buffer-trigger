package com.github.bizyun.ringbuffertrigger.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.bizyun.ringbuffertrigger.BufferTrigger;
import com.github.bizyun.ringbuffertrigger.RejectedEnqueueHandler;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author zhangbiyun
 */
class RingBufferTrigger<E, C> implements BufferTrigger<E> {

    private static final String DISRUPTOR_CONSUMER_THREAD_NAME_FORMAT =
            "ringBufferTrigger-disruptor-consumer-%d";
    private static final String SCHEDULER_THREAD_NAME_FORMAT =
            "ringBufferTrigger-scheduler-%d";

    private static final int INIT = 0;
    private static final int STARTING = 1;
    private static final int RUNNING = 2;
    private static final int CLOSING = 3;
    private static final int CLOSED = 4;

    private final AtomicInteger status = new AtomicInteger(INIT);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final CountDownLatch shutdownWaiter = new CountDownLatch(1);

    private final Disruptor<Event<E>> disruptor;
    private final RejectedEnqueueHandler<E> rejectHandler;
    private final int bufferSize;
    private final long delay;
    private final TimeUnit delayTimeUnit;
    private final String bizName;
    private final boolean enableBackPressure;
    private ScheduledExecutorService scheduledExecutorService;

    RingBufferTrigger(RingBufferTriggerBuilder<E, C> builder) {
        this.bufferSize = builder.getBufferSize();
        this.delay = builder.getDelay();
        this.delayTimeUnit = builder.getDelayTimeUnit();
        this.enableBackPressure = builder.isEnableBackPressure();
        this.bizName = builder.getBizName();

        String nameFormat = String.format("%s-%s", this.bizName, DISRUPTOR_CONSUMER_THREAD_NAME_FORMAT);
        this.disruptor = new Disruptor<>(Event::new, bufferSize,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(false).build(),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        this.disruptor.handleEventsWith(new BatchEventHandler<>(builder,
                shutdownWaiter));
        this.rejectHandler = builder.getRejectedEnqueueHandler();
    }

    @Override
    public boolean start() {
        if (!status.compareAndSet(INIT, STARTING)) {
            return false;
        }
        disruptor.start();
        if (delay > 0) {
            this.scheduledExecutorService =
                    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                            .setNameFormat(String.format("%s-%s", bizName, SCHEDULER_THREAD_NAME_FORMAT))
                            .setDaemon(true)
                            .build());
            this.scheduledExecutorService.scheduleWithFixedDelay(this::scheduleTrigger, delay,
                    delay, delayTimeUnit);
        }
        status.set(RUNNING);
        return true;
    }

    private void scheduleTrigger() {
        lockPunishEvent((e, l) -> e.markScheduleTrigger());
    }

    @Override
    public void enqueue(E element) {
        checkRunning();
        if (element == null) {
            return;
        }

        if (!enableBackPressure) {
            RingBuffer<Event<E>> ringBuffer = disruptor.getRingBuffer();
            boolean success = ringBuffer.tryPublishEvent((e, l) -> e.setElement(element));
            if (!success) {
                rejectHandler.rejectEnqueue(element);
            }
        } else {
            lockPunishEvent((e, l) -> e.setElement(element));
        }
    }

    private void lockPunishEvent(EventTranslator<Event<E>> eventTranslator) {
        RingBuffer<Event<E>> ringBuffer = disruptor.getRingBuffer();
        boolean success = ringBuffer.tryPublishEvent(eventTranslator);
        if (success) {
            return;
        }
        lock.readLock().lock();
        try {
            if (isRunning()) {
                disruptor.publishEvent(eventTranslator);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isRunning() {
        return status.get() == RUNNING;
    }

    private void checkRunning() {
        if (status.get() != RUNNING) {
            throw new IllegalStateException("RingBufferTrigger is not running");
        }
    }

    @Override
    public void trigger() {
        checkRunning();
        lockPunishEvent((e, l) -> e.markManuallyTrigger());
    }

    @Override
    public void close() {
        if (!status.compareAndSet(RUNNING, CLOSING)) {
            return;
        }
        try {
            if (scheduledExecutorService != null) {
                MoreExecutors.shutdownAndAwaitTermination(scheduledExecutorService, 1, TimeUnit.DAYS);
            }
            lock.writeLock().lock();
            try {
                disruptor.shutdown();
            } finally {
                lock.writeLock().unlock();
            }
            Uninterruptibles.awaitUninterruptibly(shutdownWaiter);
        } finally {
            status.set(CLOSED);
        }
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) throws TimeoutException {
        if (!status.compareAndSet(RUNNING, CLOSING)) {
            return;
        }
        try {
            if (scheduledExecutorService != null) {
                MoreExecutors.shutdownAndAwaitTermination(scheduledExecutorService, timeout,
                        timeUnit);
            }
            lock.writeLock().lock();
            try {
                disruptor.shutdown(timeout, timeUnit);
            } catch (com.lmax.disruptor.TimeoutException e) {
                throw new TimeoutException(e.getMessage());
            } finally {
                lock.writeLock().unlock();
            }
            Uninterruptibles.awaitUninterruptibly(shutdownWaiter, timeout, timeUnit);
        } finally {
            status.set(CLOSED);
        }
    }
}
