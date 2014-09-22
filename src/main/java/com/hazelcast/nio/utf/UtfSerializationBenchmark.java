package com.hazelcast.nio.utf;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.UTFEncoderDecoder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 01/09/14
 */
@State(Scope.Benchmark)
@OperationsPerInvocation(UtfSerializationBenchmark.OPS_PER_INV)
public class UtfSerializationBenchmark {

    public static final int OPS_PER_INV = 100;
    public static final int BUFFER_SIZE = 4 * 1024;
    public static final int SAMPLES = 1000;

    @State(Scope.Thread)
    public static class ThreadState {
        final Random rand = new Random();
        final SerializationService ss = createSerializationService();
        long total;
    }

    private String[] strings = new String[SAMPLES];
    private byte[][] utf_util_samples = new byte[SAMPLES][];
    private byte[][] utf_encoder_decoder_samples = new byte[SAMPLES][];
    private byte[][] optimized_utf_encoder_decoder_samples = new byte[SAMPLES][];

    @Param(value = {    "8", "16", "32", "64", "128", "256",
                        "512", "1024", "2048", "4096", "8192" })
    private String size;

    @Setup
    public void setUp() throws IOException {
        SerializationService ss = createSerializationService();
        BufferObjectDataOutput out = ss.createObjectDataOutput(BUFFER_SIZE);
        for (int i = 0; i < SAMPLES; i++) {
            String str = 
                    RandomStringUtils.randomAlphanumeric(Integer.parseInt(size));
            strings[i] = str;

            UTFUtil.writeUTF(out, str);
            utf_util_samples[i] = out.toByteArray();

            out.clear();

            UTFEncoderDecoder.writeUTF(out, str, new byte[1024]);
            utf_encoder_decoder_samples[i] = out.toByteArray();

            out.clear();

            OptimizedUTFEncoderDecoder.writeUTF(out, str, new byte[1024]);
            optimized_utf_encoder_decoder_samples[i] = out.toByteArray();

            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void write_UTFUtil(ThreadState state) throws IOException {
        BufferObjectDataOutput out = 
                state.ss.createObjectDataOutput(BUFFER_SIZE);
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            UTFUtil.writeUTF(out, strings[ix]);
            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void write_UTFEncodeDecoderWithSmallBuffer(ThreadState state)
            throws IOException {
        BufferObjectDataOutput out = 
                state.ss.createObjectDataOutput(BUFFER_SIZE);
        byte[] buffer = new byte[Integer.parseInt(size) / 2];

        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            UTFEncoderDecoder.writeUTF(out, strings[ix], buffer);
            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void write_UTFEncodeDecoderWithLargeBuffer(ThreadState state)
            throws IOException {
        BufferObjectDataOutput out = 
                state.ss.createObjectDataOutput(BUFFER_SIZE);
        byte[] buffer = new byte[Integer.parseInt(size) * 4];

        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            UTFEncoderDecoder.writeUTF(out, strings[ix], buffer);
            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void write_OptimizedUTFEncodeDecoderWithSmallBuffer(ThreadState state)
            throws IOException {
        BufferObjectDataOutput out = 
                state.ss.createObjectDataOutput(BUFFER_SIZE);
        byte[] buffer = new byte[Integer.parseInt(size) / 2];

        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            OptimizedUTFEncoderDecoder.writeUTF(out, strings[ix], buffer);
            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void write_OptimizedUTFEncodeDecoderWithLargeBuffer(ThreadState state)
            throws IOException {
        BufferObjectDataOutput out = 
                state.ss.createObjectDataOutput(BUFFER_SIZE);
        byte[] buffer = new byte[Integer.parseInt(size) * 4];

        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            OptimizedUTFEncoderDecoder.writeUTF(out, strings[ix], buffer);
            out.clear();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void read_UTFUtil(ThreadState state) throws IOException {
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            BufferObjectDataInput in = 
                    state.ss.createObjectDataInput(utf_util_samples[ix]);
            String s = com.hazelcast.nio.utf.UTFUtil.readUTF(in);
            state.total += s.length();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void read_UTFEncodeDecoderWithSmallBuffer(ThreadState state)
            throws IOException {
        byte[] buffer = new byte[Integer.parseInt(size) / 2];
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            BufferObjectDataInput in = 
                    state.ss.createObjectDataInput(utf_encoder_decoder_samples[ix]);
            String s = UTFEncoderDecoder.readUTF(in, buffer);
            state.total += s.length();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void read_UTFEncodeDecoderWithLargeBuffer(ThreadState state)
            throws IOException {
        byte[] buffer = new byte[Integer.parseInt(size) * 4];
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            BufferObjectDataInput in = 
                    state.ss.createObjectDataInput(utf_encoder_decoder_samples[ix]);
            String s = UTFEncoderDecoder.readUTF(in, buffer);
            state.total += s.length();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void read_OptimizedUTFEncodeDecoderWithSmallBuffer(ThreadState state)
            throws IOException {
        byte[] buffer = new byte[Integer.parseInt(size) / 2];
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            BufferObjectDataInput in = 
                    state.ss.createObjectDataInput(optimized_utf_encoder_decoder_samples[ix]);
            String s = OptimizedUTFEncoderDecoder.readUTF(in, buffer);
            state.total += s.length();
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void read_OptimizedUTFEncodeDecoderWithLargeBuffer(ThreadState state)
            throws IOException {
        byte[] buffer = new byte[Integer.parseInt(size) * 4];
        for (int i = 0; i < OPS_PER_INV; i++) {
            int ix = state.rand.nextInt(SAMPLES);
            BufferObjectDataInput in = 
                    state.ss.createObjectDataInput(optimized_utf_encoder_decoder_samples[ix]);
            String s = OptimizedUTFEncoderDecoder.readUTF(in, buffer);
            state.total += s.length();
        }
    }

    private static SerializationService createSerializationService() {
        return 
            new SerializationServiceBuilder().
                    setInitialOutputBufferSize(BUFFER_SIZE).build();
    }

}
