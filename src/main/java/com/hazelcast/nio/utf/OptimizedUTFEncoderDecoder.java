package com.hazelcast.nio.utf;

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.QuickMath;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Class to encode/decode UTF-Strings to and from byte-arrays.
 */
public final class OptimizedUTFEncoderDecoder {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    private static final OptimizedUTFEncoderDecoder INSTANCE;
    private static final long STRING_VALUE_FIELD_OFFSET;
    private static final sun.misc.Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final DefaultDataOutputUtfWriter DEFAULT_DATA_OUTPUT_UTF_WRITER =
            new DefaultDataOutputUtfWriter();
    private static final BufferedDataOutputUtfWriter BUFFERED_DATA_OUTPUT_UTF_WRITER =
            new BufferedDataOutputUtfWriter();

    static {
        INSTANCE = buildUTFUtil();
        try {
            STRING_VALUE_FIELD_OFFSET = UNSAFE.objectFieldOffset(String.class
                    .getDeclaredField("value"));
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Unable to get value field offset in String class", e);
        }
    }

    private final StringCreator stringCreator;
    private final boolean hazelcastEnterpriseActive;

    private OptimizedUTFEncoderDecoder(boolean fastStringCreator) {
        this(fastStringCreator ? buildFastStringCreator() : new DefaultStringCreator(), false);
    }

    private OptimizedUTFEncoderDecoder(StringCreator stringCreator,
                              boolean hazelcastEnterpriseActive) {
        this.stringCreator = stringCreator;
        this.hazelcastEnterpriseActive = hazelcastEnterpriseActive;
    }

    public StringCreator getStringCreator() {
        return stringCreator;
    }

    public static void writeUTF(final DataOutput out,
                                final String str,
                                final byte[] buffer) throws IOException {
        INSTANCE.writeUTF0(out, str, buffer);
    }

    public static String readUTF(final DataInput in,
                                 final byte[] buffer) throws IOException {
        return INSTANCE.readUTF0(in, buffer);
    }

    public boolean isHazelcastEnterpriseActive() {
        return hazelcastEnterpriseActive;
    }

    public void writeUTF0(final DataOutput out,
                          final String str,
                          final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) {
            return;
        }

        final DataOutputAwareUtfWriter utfWriter =
                out instanceof BufferObjectDataOutput
                        ? BUFFERED_DATA_OUTPUT_UTF_WRITER
                        : DEFAULT_DATA_OUTPUT_UTF_WRITER;
        int length = str.length();
        out.writeInt(length);
        out.writeInt(length);
        if (length > 0) {
            int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                utfWriter.writeShortUTF(out, str, beginIndex, endIndex, buffer);
            }
        }
    }

    private interface DataOutputAwareUtfWriter {

        void writeShortUTF(final DataOutput out,
                           final String str,
                           final int beginIndex,
                           final int endIndex,
                           final byte[] buffer) throws IOException;

    }

    private static class BufferedDataOutputUtfWriter implements DataOutputAwareUtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public void writeShortUTF(final DataOutput out,
                                  final String str,
                                  final int beginIndex,
                                  final int endIndex,
                                  final byte[] buffer) throws IOException {
            char[] chars = (char[]) UNSAFE.getObject(str, STRING_VALUE_FIELD_OFFSET);
            BufferObjectDataOutput bufferObjectDataOutput = (BufferObjectDataOutput) out;

            int i;
            int c;
            int bufferPos = 0;
            int utfLength = 0;
            // At most, one character can hold 3 bytes
            int maxUtfLength = chars.length * 3;

            // We save current position of buffer data output.
            // Then we write the length of UTF to here
            final int pos = bufferObjectDataOutput.position();

            // Moving position explicitly is not good way
            // since it may cause overflow exceptions for example "ByteArrayObjectDataOutput".
            // So, write dummy data and let DataOutput handle it by expanding or etc ...
            bufferObjectDataOutput.writeShort(0);
            bufferObjectDataOutput.writeBoolean(false);

            if (buffer.length >= maxUtfLength) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }
                utfLength = bufferPos;
                out.write(buffer, 0, bufferPos);
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                    utfLength++;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        utfLength += 2;
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        utfLength += 3;
                    } else {
                        // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        utfLength++;
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            }

            if (utfLength > 65535) {
                throw new UTFDataFormatException(
                        "encoded string too long:" + utfLength + " bytes");
            }

            // Write the length of UTF to saved position before
            bufferObjectDataOutput.writeShort(pos, utfLength);

            // Write the ASCII status of UTF to saved position before
            bufferObjectDataOutput.writeBoolean(pos + 2, utfLength == chars.length);
        }
        //CHECKSTYLE:ON
    }

    private static class DefaultDataOutputUtfWriter implements DataOutputAwareUtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public void writeShortUTF(final DataOutput out,
                                  final String str,
                                  final int beginIndex,
                                  final int endIndex,
                                  final byte[] buffer) throws IOException {
            char[] chars = (char[]) UNSAFE.getObject(str, STRING_VALUE_FIELD_OFFSET);

            int utfLength = calculateUtf8Length(chars, beginIndex, endIndex);
            if (utfLength > 65535) {
                throw new UTFDataFormatException(
                        "encoded string too long:" + utfLength + " bytes");
            }

            out.writeShort(utfLength);
            // We cannot determine that all characters are ASCII or not without iterating over it
            // So, we mark it as not ASCII, so all characters will be checked.
            out.writeBoolean(false);

            int i;
            int c;
            int bufferPos = 0;

            if (utfLength >= buffer.length) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                    } else {
                        // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }
                out.write(buffer, 0, bufferPos);
            }
        }
        //CHECKSTYLE:ON
    }

    public String readUTF0(final DataInput in, final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        }
        int length = in.readInt();
        int lengthCheck = in.readInt();
        if (length != lengthCheck) {
            throw new UTFDataFormatException(
                    "Length check failed, maybe broken bytestream or wrong stream position");
        }
        final char[] data = new char[length];
        if (length > 0) {
            int chunkSize = length / STRING_CHUNK_SIZE + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                readShortUTF(in, data, beginIndex, buffer);
            }
        }
        return stringCreator.buildString(data);
    }

    //CHECKSTYLE:OFF
    private void readShortUTF(final DataInput in,
                              final char[] data,
                              final int beginIndex,
                              final byte[] buffer) throws IOException {
        final int utfLength = in.readShort();
        final boolean allAscii = in.readBoolean();
        // buffer[0] is used to hold read data
        // so actual useful length of buffer is as "length - 1"
        final int minUtfLenght = Math.min(utfLength, buffer.length - 1);
        final int bufferLimit = minUtfLenght + 1;
        int readCount = 0;
        // We use buffer[0] to hold read data, so position starts from 1
        int bufferPos = 1;
        int c1 = 0;
        int c2 = 0;
        int c3 = 0;
        int cTemp = 0;
        int charArrCount = beginIndex;

        // The first readable data is at 1. index since 0. index is used to hold read data.
        in.readFully(buffer, 1, minUtfLenght);

        if (allAscii) {
            while (bufferPos != bufferLimit) {
                data[charArrCount++] = (char)(buffer[bufferPos++] & 0xFF);
            }

            for (readCount = bufferPos - 1; readCount < utfLength; readCount++) {
                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                data[charArrCount++] = (char) (buffer[0] & 0xFF);
            }
        } else {
            while (bufferPos != bufferLimit) {
                c1 = buffer[bufferPos++] & 0xFF;
                if (c1 > 127) {
                    bufferPos--;
                    break;
                }
                data[charArrCount++] = (char) c1;
            }

            readCount = bufferPos - 1;

            // Means that, 1. loop is finished since "bufferPos" is equal to "minUtfLenght"
            // and buffer capacity may be not enough to serve the requested byte.
            // So, we should get requested byte via "buffered" method by checking buffer and
            // reloading it from DataInput if it is empty.
            if (bufferPos == bufferLimit) {
                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                c1 = buffer[0] & 0xFF;
            }

            while (readCount < utfLength) {
                cTemp = c1 >> 4;
                if (cTemp >> 3 == 0) {
                    // ((cTemp & 0xF8) == 0) or (cTemp <= 7 && cTemp >= 0)
                        /* 0xxxxxxx */
                    data[charArrCount++] = (char) c1;
                    readCount++;
                } else if (cTemp == 12 || cTemp == 13) {
                        /* 110x xxxx 10xx xxxx */
                    if (readCount + 1 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c2 = buffer[0] & 0xFF;
                    if ((c2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + beginIndex + readCount + 1);
                    }
                    data[charArrCount++] = (char) (((c1 & 0x1F) << 6) | (c2 & 0x3F));
                    readCount += 2;
                } else if (cTemp == 14) {
                        /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    if (readCount + 2 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c2 = buffer[0] & 0xFF;
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c3 = buffer[0] & 0xFF;
                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (beginIndex + readCount + 1));
                    }
                    data[charArrCount++] = (char) (((c1 & 0x0F) << 12)
                            | ((c2 & 0x3F) << 6) | ((c3 & 0x3F)));
                    readCount += 3;
                } else {
                        /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + (beginIndex + readCount));
                }

                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                c1 = buffer[0] & 0xFF;
            }
        }
    }
    //CHECKSTYLE:OFF

    private static int calculateUtf8Length(final char[] chars,
                                           final int beginIndex,
                                           final int endIndex) {
        int utfLength = 0;
        for (int i = beginIndex; i < endIndex; i++) {
            int c = chars[i];
            if (c <= 0) {
                // X == 0 or 0x007F < X < 0x7FFF
                utfLength += 2;
            } else if (c > 0x007F) {
                // 0x007F < X <= 0x7FFF
                utfLength += 3;
            } else {
                // 0x0001 <= X <= 0x007F
                utfLength++;
            }
        }
        return utfLength;
    }

    private static int buffering(final byte[] buffer,
                                 final int pos,
                                 final byte value,
                                 final DataOutput out) throws IOException {
        try {
            buffer[pos] = value;
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            out.write(buffer, 0, buffer.length);
            buffer[0] = value;
            return 1;
        }
    }

    private int buffered(final byte[] buffer,
                         final int pos,
                         final int utfLength,
                         final int readCount,
                         final DataInput in) throws IOException {
        try {
            // 0. index of buffer is used to hold read data
            // so copy read data to there.
            buffer[0] = buffer[pos];
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            in.readFully(buffer, 1,
                    Math.min(buffer.length - 1, utfLength - readCount));
            // The first readable data is at 1. index since 0. index is used to
            // hold read data.
            // So the next one will be 2. index.
            buffer[0] = buffer[1];
            return 2;
        }
    }

    public static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable t) {
            Logger.
                    getLogger(OptimizedUTFEncoderDecoder.class).
                    finest("Old String constructor doesn't seem available", t);
        }
        return false;
    }

    private static OptimizedUTFEncoderDecoder buildUTFUtil() {
        try {
            Class<?> clazz =
                    Class.forName("com.hazelcast.nio.utf8.EnterpriseStringCreator");
            Method method = clazz.getDeclaredMethod("findBestStringCreator");
            return new OptimizedUTFEncoderDecoder(
                    (StringCreator) method.invoke(clazz), true);
        } catch (Throwable t) {
            Logger.
                    getLogger(OptimizedUTFEncoderDecoder.class).
                    finest("EnterpriseStringCreator not available on classpath", t);
        }
        boolean faststringEnabled =
                Boolean.parseBoolean(
                        System.getProperty("hazelcast.nio.faststring", "true"));
        return new OptimizedUTFEncoderDecoder(
                faststringEnabled
                        ? buildFastStringCreator()
                        : new DefaultStringCreator(), false);
    }

    private static StringCreator buildFastStringCreator() {
        try {
            // Give access to the package private String constructor
            Constructor<String> constructor = null;
            if (OptimizedUTFEncoderDecoder.useOldStringConstructor()) {
                constructor =
                        String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            } else {
                constructor =
                        String.class.getDeclaredConstructor(char[].class, boolean.class);
            }
            if (constructor != null) {
                constructor.setAccessible(true);
                return new FastStringCreator(constructor);
            }
        } catch (Throwable t) {
            Logger.
                    getLogger(OptimizedUTFEncoderDecoder.class).
                    finest("No fast string creator seems to available, falling back to reflection", t);
        }
        return null;
    }

    private static class DefaultStringCreator implements OptimizedUTFEncoderDecoder.StringCreator {

        @Override
        public String buildString(final char[] chars) {
            return new String(chars);
        }

    }

    private static class FastStringCreator implements OptimizedUTFEncoderDecoder.StringCreator {

        private final Constructor<String> constructor;
        private final boolean useOldStringConstructor;

        public FastStringCreator(Constructor<String> constructor) {
            this.constructor = constructor;
            this.useOldStringConstructor = constructor.getParameterTypes().length == 3;
        }

        @Override
        public String buildString(final char[] chars) {
            try {
                if (useOldStringConstructor) {
                    return constructor.newInstance(0, chars.length, chars);
                } else {
                    return constructor.newInstance(chars, Boolean.TRUE);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public interface StringCreator {

        String buildString(final char[] chars);

    }

}
