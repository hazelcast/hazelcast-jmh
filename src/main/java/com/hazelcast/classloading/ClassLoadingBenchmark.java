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

package com.hazelcast.classloading;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ClassLoaderUtil;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Random;

@State(Scope.Benchmark)
@OperationsPerInvocation(ClassLoadingBenchmark.OPERATIONS_PER_INVOCATION)
public class ClassLoadingBenchmark {

    public static final int OPERATIONS_PER_INVOCATION = 1000000;

    private static final Class<?>[] ARRAY_CLASSES = //
            { //
              byte[].class, short[].class, int[].class, long[].class, float[].class, //
              double[].class, Object[].class, HazelcastInstance[].class //
            };

    private static final String[] CLASSNAMES;
    private static final int[] INDEXES = new int[OPERATIONS_PER_INVOCATION];

    static {
        CLASSNAMES = new String[ARRAY_CLASSES.length];
        for (int i = 0; i < ARRAY_CLASSES.length; i++) {
            CLASSNAMES[i] = ARRAY_CLASSES[i].getName();
        }

        Random random = new Random();
        for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
            INDEXES[i] = random.nextInt(CLASSNAMES.length);
        }
    }

    @GenerateMicroBenchmark
    public Class<?>[] executeClassLoadingWithExternalClassLoader()
            throws Exception {

        Class<?>[] result = new Class<?>[OPERATIONS_PER_INVOCATION];
        ClassLoader classLoader = new URLClassLoader(new URL[0]);
        for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
            int index = INDEXES[i];
            String className = CLASSNAMES[index];
            result[i] = ClassLoaderUtil.loadClass(classLoader, className);
        }
        return result;
    }

    @GenerateMicroBenchmark
    public Class<?>[] executeClassLoading()
            throws Exception {

        Class<?>[] result = new Class<?>[OPERATIONS_PER_INVOCATION];
        for (int i = 0; i < OPERATIONS_PER_INVOCATION; i++) {
            int index = INDEXES[i];
            String className = CLASSNAMES[index];
            result[i] = ClassLoaderUtil.loadClass(null, className);
        }
        return result;
    }
}
