package com.github.bizyun.ringbuffertrigger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.ringbuffertrigger.impl.RingBufferTriggerBuilder;
import com.google.common.util.concurrent.MoreExecutors;

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
                    System.out.println(list);
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
}