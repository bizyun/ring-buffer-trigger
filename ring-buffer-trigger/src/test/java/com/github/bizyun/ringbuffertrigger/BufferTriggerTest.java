package com.github.bizyun.ringbuffertrigger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.ringbuffertrigger.impl.RingBufferTriggerBuilder;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author zhangbiyun
 * @date 2020/6/28
 */
class BufferTriggerTest {

    @Test
    void test1() {
        int batchConsumeSize = 30;
        AtomicInteger key = new AtomicInteger();
        Map<Integer, List<Long>> resultMap = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        BufferTrigger<Long> ringBufferTrigger = new RingBufferTriggerBuilder<Long, List<Long>>()
                .setContainerFactory(() -> new ArrayList<>(batchConsumeSize))
                .setContainerAdder(List::add)
                .setBatchConsumeSize(batchConsumeSize)
                .setBufferSize(16)
                .setBizName("testBizName")
                .setConsumer(list -> {
                    resultMap.put(key.getAndIncrement(), list);
                })
//                .setRejectedEnqueueHandler(e -> { })
                .enableBackPressure()
                .build();
        ringBufferTrigger.start();
        for (int i = 0; i < 100; i++) {
            final long k = i;
            executorService.execute(() -> ringBufferTrigger.enqueue(k));
        }
        MoreExecutors.shutdownAndAwaitTermination( executorService, 1, TimeUnit.MINUTES);
        ringBufferTrigger.close();

        assertEquals(4, resultMap.size());
        assertEquals(30, resultMap.get(0).size());
        assertEquals(30, resultMap.get(1).size());
        assertEquals(30, resultMap.get(2).size());
        assertEquals(10, resultMap.get(3).size());

    }

    @Test
    void test2() {
        int batchConsumeSize = 100;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger key = new AtomicInteger();
        Map<Integer, List<Long>> resultMap = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        BufferTrigger<Long> ringBufferTrigger = new RingBufferTriggerBuilder<Long, List<Long>>()
                .setContainerFactory(() -> new ArrayList<>(batchConsumeSize))
                .setContainerAdder(List::add)
                .setBatchConsumeSize(batchConsumeSize)
                .setBufferSize(1000)
                .setBizName("testBizName")
                .setConsumer(list -> {
                    resultMap.put(key.getAndIncrement(), list);
                    latch.countDown();
                })
//                .setRejectedEnqueueHandler(e -> { })
                .enableBackPressure()
                .build();
        ringBufferTrigger.start();
        for (int i = 0; i < 56; i++) {
            final long k = i;
            executorService.execute(() -> ringBufferTrigger.enqueue(k));
        }
        MoreExecutors.shutdownAndAwaitTermination( executorService, 1, TimeUnit.MINUTES);
        assertEquals(0, resultMap.size());
        ringBufferTrigger.trigger();
        Uninterruptibles.awaitUninterruptibly(latch);
        assertEquals(1, resultMap.size());
        assertEquals(56, resultMap.get(0).size());
        ringBufferTrigger.close();
    }

    @Test
    void test3() {
        int batchConsumeSize = 100;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger key = new AtomicInteger();
        Map<Integer, Map<Long, Integer>> resultMap = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        BufferTrigger<Long> ringBufferTrigger = new RingBufferTriggerBuilder<Long, Map<Long,
                Integer>>()
                .setContainerFactory(() -> new HashMap<>(batchConsumeSize))
                .setContainerAdder((map, v) -> map.put(v, 1) == null)
                .setBatchConsumeSize(batchConsumeSize)
                .setBufferSize(1000)
                .setBizName("testBizName")
                .setScheduledFixDelay(1, TimeUnit.SECONDS)
                .setConsumer(map -> {
                    resultMap.put(key.getAndIncrement(), map);
                    latch.countDown();
                })
                .enableBackPressure()
                .build();
        ringBufferTrigger.start();
        for (int i = 0; i < 56; i++) {
            final long k = i;
            executorService.execute(() -> ringBufferTrigger.enqueue(k));
        }
        MoreExecutors.shutdownAndAwaitTermination( executorService, 1, TimeUnit.MINUTES);
        Uninterruptibles.awaitUninterruptibly(latch);
        assertEquals(1, resultMap.size());
        assertEquals(56, resultMap.get(0).size());
        ringBufferTrigger.close();
    }

    @Test
    void test4() {
        int batchConsumeSize = 8;
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicInteger key = new AtomicInteger();
        AtomicInteger rejectCount = new AtomicInteger();
        Map<Integer, Map<Long, Integer>> resultMap = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        BufferTrigger<Long> ringBufferTrigger = new RingBufferTriggerBuilder<Long, Map<Long,
                Integer>>()
                .setContainerFactory(() -> new HashMap<>(batchConsumeSize))
                .setContainerAdder((map, v) -> map.put(v, 1) == null)
                .setBatchConsumeSize(batchConsumeSize)
                .setBufferSize(16)
                .setBizName("testBizName")
                .setRejectedEnqueueHandler(e -> rejectCount.getAndIncrement())
                .setConsumer(map -> {
                    latch1.countDown();
                    Uninterruptibles.awaitUninterruptibly(latch);
                    resultMap.put(key.getAndIncrement(), map);
                })
                .build();
        ringBufferTrigger.start();
        for (int i = 0; i < 8; i++) {
            final long k = i;
            executorService.execute(() -> ringBufferTrigger.enqueue(k));
        }
        Uninterruptibles.awaitUninterruptibly(latch1);
        for (int i = 0; i < 56; i++) {
            final long k = i;
            executorService.execute(() -> ringBufferTrigger.enqueue(k));
        }
        MoreExecutors.shutdownAndAwaitTermination( executorService, 1, TimeUnit.MINUTES);
        assertEquals(0, resultMap.size());
        assertEquals(41, rejectCount.get());
        latch.countDown();
        ringBufferTrigger.close();

    }
}