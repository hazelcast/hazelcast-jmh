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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.HashMap;
import java.util.Map;

@State(Scope.Benchmark)
@OperationsPerInvocation(OnheapSlabBenchmark.OPERATIONS_PER_INVOCATION)
public class OnheapSlabBenchmark {

    public static final int OPERATIONS_PER_INVOCATION = 1000000;

    @Benchmark
    public Object[] testHashMap() throws Exception {
        return testInternal(new HashMap<String, Entity>());
    }

    @Benchmark
    public Object[] testOnheapSlabMap() throws Exception {
        return testInternal(new ObheapSlabMap<String, Entity>());
    }

    private Entity[] testInternal(Map<String, Entity> map) {
        return null;
    }

    private static final class Entity {

    }

}
