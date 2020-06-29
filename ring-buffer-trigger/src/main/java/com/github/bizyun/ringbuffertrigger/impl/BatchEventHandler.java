package com.github.bizyun.ringbuffertrigger.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

/**
 * @author zhangbiyun
 */
class BatchEventHandler<E, C> implements EventHandler<Event<E>>, LifecycleAware {

    private static final Logger logger = LoggerFactory.getLogger(BatchEventHandler.class);

    private final Supplier<? extends C> containerFactory;
    private final ToIntBiFunction<? super C, ? super E> containerAdder;
    private final Consumer<? super C> bufferConsumer;
    private final Executor executor;
    private final Supplier<Integer> batchConsumeSize;
    private final BiConsumer<Throwable, ? super C> exceptionHandler;
    private final CountDownLatch shutdownLatch;

    private C container;
    private int currentSize = 0;

    public BatchEventHandler(RingBufferTriggerBuilder<E, C> builder, CountDownLatch shutdownWaiter) {
        this.containerFactory = builder.getContainerFactory();
        this.containerAdder = builder.getContainerAdder();
        this.bufferConsumer = builder.getConsumer();
        this.executor = builder.getConsumerExecutor();
        this.batchConsumeSize = builder.getBatchConsumeSize();
        this.exceptionHandler = builder.getExceptionHandler();
        this.shutdownLatch = shutdownWaiter;
    }

    @Override
    public void onEvent(Event<E> event, long eventSequence, boolean endOfBatch) throws Exception {
        if (event.isManuallyTrigger() || event.isSchedulerTrigger()) {
            event.clear();
            if (currentSize > 0) {
                doBatchConsume();
            }
            return;
        }
        E element = event.getElement();
        event.clear();
        currentSize += containerAdder.applyAsInt(getContainer(), element);
        if (currentSize >= this.batchConsumeSize.get().intValue()) {
            doBatchConsume();
        }
    }

    private C getContainer() {
        if (this.container == null) {
            this.container = containerFactory.get();
        }
        return this.container;
    }

    private void doBatchConsume() {
        final C finalContainer = this.container;
        this.container = null;
        this.currentSize = 0;
        try {
            executor.execute(() -> {
                try {
                    bufferConsumer.accept(finalContainer);
                } catch (Throwable t) {
                    try {
                        exceptionHandler.accept(t, finalContainer);
                    } catch (Throwable ex) {
                        logger.error("", ex);
                    }
                    logger.error("Ops.", t);
                }
            });
        } catch (Throwable e) {
            logger.error("Ops.", e);
        }
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onShutdown() {
        if (this.currentSize > 0) {
            doBatchConsume();
        }
        shutdownLatch.countDown();
    }
}
