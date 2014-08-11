/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.onheapslab;

import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = {"-Xmx25G", "-Xms15G", "-XX:+UseTLAB", "-XX:+AlwaysPreTouch"})
@OperationsPerInvocation(OnheapSlabBenchmark.OPERATIONS_PER_INVOCATION)
public class OnheapSlabBenchmark {

    public static final int OPERATIONS_PER_INVOCATION = 3221200;

    //27487790

    private final Random random = new Random();

    private SerializationService serializationService;

    @Param(value = {"JDK", "SLAB", "OFFHEAP"})
    private String type;

    private Map<Integer, byte[]> map;

    @Setup(Level.Trial)
    public void benchmarkSetup() {
        serializationService = new SerializationServiceBuilder()
                .addDataSerializableFactory(1000, new EntityDataSerializableFactory())
                .setAllowUnsafe(true).setUseNativeByteOrder(true).build();
        map = createMap();
    }

    @TearDown(Level.Trial)
    public void benchmarkTeardown() {
        if (map instanceof SlapMap) {
            ((SlapMap) map).destroy();
        }
    }

    @TearDown(Level.Iteration)
    public void teardown() {
        map.clear();
    }

    public Map<Integer, byte[]> createMap() {
        Map<Integer, byte[]> map;
        if ("SLAB".equals(type)) {
            map = new SlapMap(false, OPERATIONS_PER_INVOCATION + 100);
        } else if ("OFFHEAP".equals(type)) {
            map = new SlapMap(true, OPERATIONS_PER_INVOCATION + 100);
        } else if ("JDK".equals(type)) {
            map = new HashMap<Integer, byte[]>();
        } else {
            throw new RuntimeException("Unknown map type");
        }
        return map;
    }

    @Benchmark
    public long testInternal() {
        long h = 0;
        for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
            byte[] entity = buildEntity();
            map.put(i, entity);
            byte[] e = map.get(i);
            h += e.length;
        }
        return h;
    }

    public static void main(String[] args) {
        OnheapSlabBenchmark benchmark = new OnheapSlabBenchmark();
        benchmark.type = "SLAB";
        benchmark.benchmarkSetup();
        for (int i = 0; i < 100; i++) {
            benchmark.testInternal();
            benchmark.teardown();
        }
        benchmark.benchmarkTeardown();
    }

    private byte[] buildEntity() {
        try {
            Entity entity = new Entity();
            entity.foo = new byte[1000 + random.nextInt(1000)];
            BufferObjectDataOutput objectDataOutput = serializationService.createObjectDataOutput(2100);
            objectDataOutput.writeObject(entity);
            byte[] buffer = objectDataOutput.getBuffer();
            byte[] temp = new byte[objectDataOutput.position()];
            System.arraycopy(buffer, 0, temp, 0, objectDataOutput.position());
            return temp;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final class Entity
            implements IdentifiedDataSerializable {

        private byte[] foo;

        @Override
        public void writeData(ObjectDataOutput objectDataOutput)
                throws IOException {

            objectDataOutput.writeInt(foo.length);
            objectDataOutput.write(foo);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput)
                throws IOException {

            int length = objectDataInput.readInt();
            foo = new byte[length];
            objectDataInput.readFully(foo);
        }

        @Override
        public int getFactoryId() {
            return 1000;
        }

        @Override
        public int getId() {
            return 1000;
        }
    }

    private static final class EntityDataSerializableFactory
            implements DataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new Entity();
        }
    }
}
