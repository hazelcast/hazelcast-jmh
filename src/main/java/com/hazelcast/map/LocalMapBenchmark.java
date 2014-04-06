/*
 * Copyright (c) 2005, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Random;

import static com.hazelcast.Util.randomString;

//http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
//http://code.google.com/p/dart/wiki/Profiling
@State(value = Scope.Thread)
@OperationsPerInvocation(LocalMapBenchmark.OPERATIONS_PER_INVOCATION)
public class LocalMapBenchmark {

    public static final long OPERATIONS_PER_INVOCATION = 500000;

    private HazelcastInstance hz;
    private IMap<Integer, String> map;
    private Integer[] keys;
    private String[] values;

    @Setup
    public void setUp() {
        MapConfig mapConfig = new MapConfig("somemap");
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setBackupCount(0);
        mapConfig.setStatisticsEnabled(false);

        Config config = new Config();
        //config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        //config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        //config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.168.1.107");
        //config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        config.addMapConfig(mapConfig);
        hz = Hazelcast.newHazelcastInstance(config);

        map = hz.getMap(mapConfig.getName());

        int size = 5000;
        keys = new Integer[size];
        values = new String[size];

        for (int k = 0; k < keys.length; k++) {
            keys[k] = k;
            values[k] = randomString(500);
            map.put(keys[k], values[k]);
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @GenerateMicroBenchmark
    public void putPerformance() {
        Random random = new Random();
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            Integer key = keys[random.nextInt(keys.length)];
            String value = values[k % values.length];
            map.put(key, value);
        }
    }

    @GenerateMicroBenchmark
    public void setPerformance() {
        Random random = new Random();
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            Integer key = keys[random.nextInt(keys.length)];
            String value = values[k % values.length];
            map.set(key, value);
        }
    }

    @GenerateMicroBenchmark
    public void getPerformance() {
        Random random = new Random();
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            int x = random.nextInt(keys.length);
            map.get(x);
        }
    }
}