/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.util;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DIRECT_BYTES_READ_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.HEAP_BYTES_READ_KEY;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.io.hfile.trace.HFileContextAttributesBuilderConsumer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class BlockIOUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BlockIOUtils.class);
  // TODO: remove the reflection when we update to Hadoop 3.3 or above.
  private static Method byteBufferPositionedReadMethod;

  static {
    initByteBufferPositionReadableMethod();
  }

  // Disallow instantiation
  private BlockIOUtils() {

  }

  private static void initByteBufferPositionReadableMethod() {
    try {
      // long position, ByteBuffer buf
      byteBufferPositionedReadMethod =
        FSDataInputStream.class.getMethod("read", long.class, ByteBuffer.class);
    } catch (NoSuchMethodException e) {
      LOG.debug("Unable to find positioned bytebuffer read API of FSDataInputStream. "
        + "preadWithExtra() will use a temporary on-heap byte array.");
    }
  }

  public static boolean isByteBufferReadable(FSDataInputStream is) {
    InputStream cur = is.getWrappedStream();
    for (;;) {
      if ((cur instanceof FSDataInputStream)) {
        cur = ((FSDataInputStream) cur).getWrappedStream();
      } else {
        break;
      }
    }
    return cur instanceof ByteBufferReadable;
  }

  /**
   * Read length bytes into ByteBuffers directly.
   * @param buf    the destination {@link ByteBuff}
   * @param dis    the HDFS input stream which implement the ByteBufferReadable interface.
   * @param length bytes to read.
   * @throws IOException exception to throw if any error happen
   */
  public static void readFully(ByteBuff buf, FSDataInputStream dis, int length) throws IOException {
    final Span span = Span.current();
    if (!isByteBufferReadable(dis)) {
      // If InputStream does not support the ByteBuffer read, just read to heap and copy bytes to
      // the destination ByteBuff.
      byte[] heapBuf = new byte[length];
      IOUtils.readFully(dis, heapBuf, 0, length);
      span.addEvent("BlockIOUtils.readFully", getHeapBytesReadAttributes(span, length));
      copyToByteBuff(heapBuf, 0, length, buf);
      return;
    }
    int directBytesRead = 0, heapBytesRead = 0;
    ByteBuffer[] buffers = buf.nioByteBuffers();
    int remain = length;
    int idx = 0;
    ByteBuffer cur = buffers[idx];
    try {
      while (remain > 0) {
        while (!cur.hasRemaining()) {
          if (++idx >= buffers.length) {
            throw new IOException(
              "Not enough ByteBuffers to read the reminding " + remain + " " + "bytes");
          }
          cur = buffers[idx];
        }
        cur.limit(cur.position() + Math.min(remain, cur.remaining()));
        int bytesRead = dis.read(cur);
        if (bytesRead < 0) {
          throw new IOException(
            "Premature EOF from inputStream, but still need " + remain + " " + "bytes");
        }
        remain -= bytesRead;
        if (cur.isDirect()) {
          directBytesRead += bytesRead;
        } else {
          heapBytesRead += bytesRead;
        }
      }
    } finally {
      span.addEvent("BlockIOUtils.readFully",
        getDirectAndHeapBytesReadAttributes(span, directBytesRead, heapBytesRead));
    }
  }

  /**
   * Copying bytes from InputStream to {@link ByteBuff} by using an temporary heap byte[] (default
   * size is 1024 now).
   * @param in     the InputStream to read
   * @param out    the destination {@link ByteBuff}
   * @param length to read
   * @throws IOException if any io error encountered.
   */
  public static void readFullyWithHeapBuffer(InputStream in, ByteBuff out, int length)
    throws IOException {
    if (length < 0) {
      throw new IllegalArgumentException("Length must not be negative: " + length);
    }
    int heapBytesRead = 0;
    int remain = length, count;
    byte[] buffer = new byte[1024];
    try {
      while (remain > 0) {
        count = in.read(buffer, 0, Math.min(remain, buffer.length));
        if (count < 0) {
          throw new IOException(
            "Premature EOF from inputStream, but still need " + remain + " bytes");
        }
        out.put(buffer, 0, count);
        remain -= count;
        heapBytesRead += count;
      }
    } finally {
      final Span span = Span.current();
      span.addEvent("BlockIOUtils.readFullyWithHeapBuffer",
        getHeapBytesReadAttributes(span, heapBytesRead));
    }
  }

  /**
   * Read from an input stream at least <code>necessaryLen</code> and if possible,
   * <code>extraLen</code> also if available. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but specifies a number of "extra"
   * bytes to also optionally read.
   * @param in           the input stream to read from
   * @param buf          the buffer to read into
   * @param bufOffset    the destination offset in the buffer
   * @param necessaryLen the number of bytes that are absolutely necessary to read
   * @param extraLen     the number of extra bytes that would be nice to read
   * @return true if succeeded reading the extra bytes
   * @throws IOException if failed to read the necessary bytes
   */
  private static boolean readWithExtraOnHeap(InputStream in, byte[] buf, int bufOffset,
    int necessaryLen, int extraLen) throws IOException {
    int heapBytesRead = 0;
    int bytesRemaining = necessaryLen + extraLen;
    try {
      while (bytesRemaining > 0) {
        int ret = in.read(buf, bufOffset, bytesRemaining);
        if (ret < 0) {
          if (bytesRemaining <= extraLen) {
            // We could not read the "extra data", but that is OK.
            break;
          }
          throw new IOException("Premature EOF from inputStream (read " + "returned " + ret
            + ", was trying to read " + necessaryLen + " necessary bytes and " + extraLen
            + " extra bytes, " + "successfully read " + (necessaryLen + extraLen - bytesRemaining));
        }
        bufOffset += ret;
        bytesRemaining -= ret;
        heapBytesRead += ret;
      }
    } finally {
      final Span span = Span.current();
      span.addEvent("BlockIOUtils.readWithExtra", getHeapBytesReadAttributes(span, heapBytesRead));
    }
    return bytesRemaining <= 0;
  }

  /**
   * Read bytes into ByteBuffers directly, those buffers either contains the extraLen bytes or only
   * contains necessaryLen bytes, which depends on how much bytes do the last time we read.
   * @param buf          the destination {@link ByteBuff}.
   * @param dis          input stream to read.
   * @param necessaryLen bytes which we must read
   * @param extraLen     bytes which we may read
   * @return if the returned flag is true, then we've finished to read the extraLen into our
   *         ByteBuffers, otherwise we've not read the extraLen bytes yet.
   * @throws IOException if failed to read the necessary bytes.
   */
  public static boolean readWithExtra(ByteBuff buf, FSDataInputStream dis, int necessaryLen,
    int extraLen) throws IOException {
    if (!isByteBufferReadable(dis)) {
      // If InputStream does not support the ByteBuffer read, just read to heap and copy bytes to
      // the destination ByteBuff.
      byte[] heapBuf = new byte[necessaryLen + extraLen];
      boolean ret = readWithExtraOnHeap(dis, heapBuf, 0, necessaryLen, extraLen);
      copyToByteBuff(heapBuf, 0, heapBuf.length, buf);
      return ret;
    }
    int directBytesRead = 0, heapBytesRead = 0;
    ByteBuffer[] buffers = buf.nioByteBuffers();
    int bytesRead = 0;
    int remain = necessaryLen + extraLen;
    int idx = 0;
    ByteBuffer cur = buffers[idx];
    try {
      while (bytesRead < necessaryLen) {
        while (!cur.hasRemaining()) {
          if (++idx >= buffers.length) {
            throw new IOException(
              "Not enough ByteBuffers to read the reminding " + remain + "bytes");
          }
          cur = buffers[idx];
        }
        cur.limit(cur.position() + Math.min(remain, cur.remaining()));
        int ret = dis.read(cur);
        if (ret < 0) {
          throw new IOException("Premature EOF from inputStream (read returned " + ret
            + ", was trying to read " + necessaryLen + " necessary bytes and " + extraLen
            + " extra bytes, successfully read " + bytesRead);
        }
        bytesRead += ret;
        remain -= ret;
        if (cur.isDirect()) {
          directBytesRead += ret;
        } else {
          heapBytesRead += ret;
        }
      }
    } finally {
      final Span span = Span.current();
      span.addEvent("BlockIOUtils.readWithExtra",
        getDirectAndHeapBytesReadAttributes(span, directBytesRead, heapBytesRead));
    }
    return (extraLen > 0) && (bytesRead == necessaryLen + extraLen);
  }

  /**
   * Read from an input stream at least <code>necessaryLen</code> and if possible,
   * <code>extraLen</code> also if available. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but uses positional read and
   * specifies a number of "extra" bytes that would be desirable but not absolutely necessary to
   * read. If the input stream supports ByteBufferPositionedReadable, it reads to the byte buffer
   * directly, and does not allocate a temporary byte array.
   * @param buff         ByteBuff to read into.
   * @param dis          the input stream to read from
   * @param position     the position within the stream from which to start reading
   * @param necessaryLen the number of bytes that are absolutely necessary to read
   * @param extraLen     the number of extra bytes that would be nice to read
   * @return true if and only if extraLen is > 0 and reading those extra bytes was successful
   * @throws IOException if failed to read the necessary bytes
   */
  public static boolean preadWithExtra(ByteBuff buff, FSDataInputStream dis, long position,
    int necessaryLen, int extraLen) throws IOException {
    return preadWithExtra(buff, dis, position, necessaryLen, extraLen, false);
  }

  /**
   * Read from an input stream at least <code>necessaryLen</code> and if possible,
   * <code>extraLen</code> also if available. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but uses positional read and
   * specifies a number of "extra" bytes that would be desirable but not absolutely necessary to
   * read. If the input stream supports ByteBufferPositionedReadable, it reads to the byte buffer
   * directly, and does not allocate a temporary byte array.
   * @param buff         ByteBuff to read into.
   * @param dis          the input stream to read from
   * @param position     the position within the stream from which to start reading
   * @param necessaryLen the number of bytes that are absolutely necessary to read
   * @param extraLen     the number of extra bytes that would be nice to read
   * @param readAllBytes whether we must read the necessaryLen and extraLen
   * @return true if and only if extraLen is > 0 and reading those extra bytes was successful
   * @throws IOException if failed to read the necessary bytes
   */
  public static boolean preadWithExtra(ByteBuff buff, FSDataInputStream dis, long position,
    int necessaryLen, int extraLen, boolean readAllBytes) throws IOException {
    boolean preadbytebuffer = dis.hasCapability("in:preadbytebuffer");

    if (preadbytebuffer) {
      return preadWithExtraDirectly(buff, dis, position, necessaryLen, extraLen, readAllBytes);
    } else {
      return preadWithExtraOnHeap(buff, dis, position, necessaryLen, extraLen, readAllBytes);
    }
  }

  private static boolean preadWithExtraOnHeap(ByteBuff buff, FSDataInputStream dis, long position,
    int necessaryLen, int extraLen, boolean readAllBytes) throws IOException {
    int remain = necessaryLen + extraLen;
    byte[] buf = new byte[remain];
    int bytesRead = 0;
    int lengthMustRead = readAllBytes ? remain : necessaryLen;
    try {
      while (bytesRead < lengthMustRead) {
        int ret = dis.read(position + bytesRead, buf, bytesRead, remain);
        if (ret < 0) {
          throw new IOException("Premature EOF from inputStream (positional read returned " + ret
            + ", was trying to read " + necessaryLen + " necessary bytes and " + extraLen
            + " extra bytes, successfully read " + bytesRead);
        }
        bytesRead += ret;
        remain -= ret;
      }
    } finally {
      final Span span = Span.current();
      span.addEvent("BlockIOUtils.preadWithExtra", getHeapBytesReadAttributes(span, bytesRead));
    }
    copyToByteBuff(buf, 0, bytesRead, buff);
    return (extraLen > 0) && (bytesRead == necessaryLen + extraLen);
  }

  private static boolean preadWithExtraDirectly(ByteBuff buff, FSDataInputStream dis, long position,
    int necessaryLen, int extraLen, boolean readAllBytes) throws IOException {
    int directBytesRead = 0, heapBytesRead = 0;
    int remain = necessaryLen + extraLen, bytesRead = 0, idx = 0;
    ByteBuffer[] buffers = buff.nioByteBuffers();
    ByteBuffer cur = buffers[idx];
    int lengthMustRead = readAllBytes ? remain : necessaryLen;
    try {
      while (bytesRead < lengthMustRead) {
        int ret;
        while (!cur.hasRemaining()) {
          if (++idx >= buffers.length) {
            throw new IOException(
              "Not enough ByteBuffers to read the reminding " + remain + "bytes");
          }
          cur = buffers[idx];
        }
        cur.limit(cur.position() + Math.min(remain, cur.remaining()));
        try {
          ret = (Integer) byteBufferPositionedReadMethod.invoke(dis, position + bytesRead, cur);
        } catch (IllegalAccessException e) {
          throw new IOException("Unable to invoke ByteBuffer positioned read when trying to read "
            + bytesRead + " bytes from position " + position, e);
        } catch (InvocationTargetException e) {
          throw new IOException("Encountered an exception when invoking ByteBuffer positioned read"
            + " when trying to read " + bytesRead + " bytes from position " + position, e);
        }
        if (ret < 0) {
          throw new IOException("Premature EOF from inputStream (positional read returned " + ret
            + ", was trying to read " + necessaryLen + " necessary bytes and " + extraLen
            + " extra bytes, successfully read " + bytesRead);
        }
        bytesRead += ret;
        remain -= ret;
        if (cur.isDirect()) {
          directBytesRead += bytesRead;
        } else {
          heapBytesRead += bytesRead;
        }
      }
    } finally {
      final Span span = Span.current();
      span.addEvent("BlockIOUtils.preadWithExtra",
        getDirectAndHeapBytesReadAttributes(span, directBytesRead, heapBytesRead));
    }

    return (extraLen > 0) && (bytesRead == necessaryLen + extraLen);
  }

  private static int copyToByteBuff(byte[] buf, int offset, int len, ByteBuff out)
    throws IOException {
    if (offset < 0 || len < 0 || offset + len > buf.length) {
      throw new IOException("Invalid offset=" + offset + " and len=" + len + ", cap=" + buf.length);
    }
    ByteBuffer[] buffers = out.nioByteBuffers();
    int idx = 0, remain = len, copyLen;
    ByteBuffer cur = buffers[idx];
    while (remain > 0) {
      while (!cur.hasRemaining()) {
        if (++idx >= buffers.length) {
          throw new IOException("Not enough ByteBuffers to read the reminding " + remain + "bytes");
        }
        cur = buffers[idx];
      }
      copyLen = Math.min(cur.remaining(), remain);
      cur.put(buf, offset, copyLen);
      remain -= copyLen;
      offset += copyLen;
    }
    return len;
  }

  /**
   * Builds OpenTelemetry attributes to be recorded on a span for an event which reads direct and
   * heap bytes. This will short-circuit and record nothing if OpenTelemetry isn't enabled.
   */
  private static Attributes getDirectAndHeapBytesReadAttributes(Span span, int directBytesRead,
    int heapBytesRead) {
    // It's expensive to record these attributes, so we avoid the cost of doing this if the span
    // isn't going to be persisted
    if (!span.isRecording()) {
      return Attributes.empty();
    }

    final AttributesBuilder attributesBuilder = builderFromContext(Context.current());
    annotateBytesRead(attributesBuilder, directBytesRead, heapBytesRead);

    return attributesBuilder.build();
  }

  /**
   * Builds OpenTelemtry attributes to be recorded on a span for an event which reads just heap
   * bytes. This will short-circuit and record nothing if OpenTelemetry isn't enabled.
   */
  private static Attributes getHeapBytesReadAttributes(Span span, int heapBytesRead) {
    // It's expensive to record these attributes, so we avoid the cost of doing this if the span
    // isn't going to be persisted
    if (!span.isRecording()) {
      return Attributes.empty();
    }

    final AttributesBuilder attributesBuilder = builderFromContext(Context.current());
    annotateHeapBytesRead(attributesBuilder, heapBytesRead);

    return attributesBuilder.build();
  }

  /**
   * Construct a fresh {@link AttributesBuilder} from the provided {@link Context}, populated with
   * relevant attributes populated by {@link HFileContextAttributesBuilderConsumer#CONTEXT_KEY}.
   */
  private static AttributesBuilder builderFromContext(Context context) {
    final AttributesBuilder attributesBuilder = Attributes.builder();
    Optional.ofNullable(context)
      .map(val -> val.get(HFileContextAttributesBuilderConsumer.CONTEXT_KEY))
      .ifPresent(c -> c.accept(attributesBuilder));
    return attributesBuilder;
  }

  /**
   * Conditionally annotate {@code span} with the appropriate attribute when value is non-zero.
   */
  private static void annotateHeapBytesRead(AttributesBuilder attributesBuilder,
    int heapBytesRead) {
    annotateBytesRead(attributesBuilder, 0, heapBytesRead);
  }

  /**
   * Conditionally annotate {@code attributesBuilder} with appropriate attributes when values are
   * non-zero.
   */
  private static void annotateBytesRead(AttributesBuilder attributesBuilder, int directBytesRead,
    int heapBytesRead) {
    if (directBytesRead > 0) {
      attributesBuilder.put(DIRECT_BYTES_READ_KEY, directBytesRead);
    }
    if (heapBytesRead > 0) {
      attributesBuilder.put(HEAP_BYTES_READ_KEY, heapBytesRead);
    }
  }
}
