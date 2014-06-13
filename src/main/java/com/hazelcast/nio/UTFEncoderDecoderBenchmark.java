package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.util.UuidUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

@State(Scope.Benchmark)
public class UTFEncoderDecoderBenchmark {
    private SerializationService serializationService;
    private String uuid;

    @Setup
    public void setup() throws IOException {
        serializationService = new SerializationServiceBuilder().build();
        uuid = UuidUtil.createMemberUuid(null);
    }

    @Benchmark
    public String testUuidUtilTest() {
        Data data = serializationService.toData(uuid);
        return serializationService.toObject(data);
    }
}
