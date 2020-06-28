package com.github.bizyun.ringbuffertrigger.microbench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.github.bizyun.ringbuffertrigger.BufferTrigger;
import com.github.bizyun.ringbuffertrigger.impl.RingBufferTriggerBuilder;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author zhangbiyun
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 1)
@Measurement(iterations = 3)
@Warmup(iterations = 3)
@Threads(6)
public class RingBufferTriggerBenchmarkForMap {

    private BufferTrigger<Long> ringBufferTrigger;
    private ExecutorService executorService;
    private LongAdder ringBufferLongAdder;
    private AtomicLong valueAdder;

    @Benchmark
    public void measureRingBufferTrigger() {
        ringBufferTrigger.enqueue(valueAdder.getAndIncrement());
    }

    @Setup(Level.Trial)
    public void setUp() {
        executorService = Executors.newFixedThreadPool(3);
        int batchConsumeSize = 10000;
        ringBufferTrigger = new RingBufferTriggerBuilder<Long, Map<Long, Integer>>()
                .setContainerFactory(() -> new HashMap<>(batchConsumeSize * 2))
                .setContainerAdder((map, e) -> map.put(e, 1) == null)
                .setBatchConsumeSize(batchConsumeSize)
                .setBufferSize(1000000)
                .setBizName("testBizName")
                .setConsumer(list -> this.consume(list, ringBufferLongAdder))
                .setScheduledFixDelay(1, TimeUnit.SECONDS)
                .setRejectedEnqueueHandler(e -> { })
                .enableBackPressure()
                .setConsumerExecutor(executorService)
                .build();
        ringBufferTrigger.start();
        ringBufferLongAdder = new LongAdder();
        valueAdder = new AtomicLong();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        ringBufferTrigger.close();
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.HOURS);
    }

    private void consume(Map<Long, Integer> batchMap, LongAdder longAdder) {
        longAdder.add(batchMap.size());
    }
}
