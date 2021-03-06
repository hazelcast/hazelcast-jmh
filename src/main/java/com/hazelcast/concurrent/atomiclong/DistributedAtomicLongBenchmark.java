package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;


@State(value = Scope.Thread)
@OperationsPerInvocation(DistributedAtomicLongBenchmark.OPERATIONS_PER_INVOCATION)
public class DistributedAtomicLongBenchmark {

    public static final int OPERATIONS_PER_INVOCATION = 500000;

    private HazelcastInstance hz;
    private IAtomicLong atomicLong;

    @Setup
    public void setUp() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        hz = Hazelcast.newHazelcastInstance(config);
        atomicLong = hz.getAtomicLong("atomiclong");
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void setPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            atomicLong.set(k);
        }
    }

    @Benchmark
    public void getAndSetPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            atomicLong.getAndSet(k);
        }
    }

    @Benchmark
    public void getPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            atomicLong.get();
        }
    }
}
