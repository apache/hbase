/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import com.google.common.base.Throwables;

import io.netty.channel.EventLoop;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBufferSupportOutputStream;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutputHelper;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * AsyncWriter for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class AsyncProtobufLogWriter extends AbstractProtobufLogWriter
    implements AsyncFSWALProvider.AsyncWriter {

  private static final Log LOG = LogFactory.getLog(AsyncProtobufLogWriter.class);

  private final EventLoop eventLoop;

  private AsyncFSOutput output;

  private static final class OutputStreamWrapper extends OutputStream
      implements ByteBufferSupportOutputStream {

    private final AsyncFSOutput out;

    private final byte[] oneByteBuf = new byte[1];

    @Override
    public void write(int b) throws IOException {
      oneByteBuf[0] = (byte) b;
      write(oneByteBuf);
    }

    public OutputStreamWrapper(AsyncFSOutput out) {
      this.out = out;
    }

    @Override
    public void write(ByteBuffer b, int off, int len) throws IOException {
      ByteBuffer bb = b.duplicate();
      bb.position(off);
      bb.limit(off + len);
      out.write(bb);
    }

    @Override
    public void writeInt(int i) throws IOException {
      out.writeInt(i);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  private OutputStream asyncOutputWrapper;

  public AsyncProtobufLogWriter(EventLoop eventLoop) {
    this.eventLoop = eventLoop;
  }

  @Override
  public void append(Entry entry) {
    int buffered = output.buffered();
    entry.setCompressionContext(compressionContext);
    try {
      entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
          .writeDelimitedTo(asyncOutputWrapper);
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    try {
      for (Cell cell : entry.getEdit().getCells()) {
        cellEncoder.write(cell);
      }
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    length.addAndGet(output.buffered() - buffered);
  }

  @Override
  public CompletableFuture<Long> sync() {
    return output.flush(false);
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.output == null) {
      return;
    }
    try {
      writeWALTrailer();
      output.close();
    } catch (Exception e) {
      LOG.warn("normal close failed, try recover", e);
      output.recoverAndClose(null);
    }
    this.output = null;
  }

  public AsyncFSOutput getOutput() {
    return this.output;
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException {
    this.output = AsyncFSOutputHelper.createOutput(fs, path, overwritable, false, replication,
      blockSize, eventLoop);
    this.asyncOutputWrapper = new OutputStreamWrapper(output);
  }

  private long write(Consumer<CompletableFuture<Long>> action) throws IOException {
    CompletableFuture<Long> future = new CompletableFuture<Long>();
    eventLoop.execute(() -> action.accept(future));
    try {
      return future.get().longValue();
    } catch (InterruptedException e) {
      InterruptedIOException ioe = new InterruptedIOException();
      ioe.initCause(e);
      throw ioe;
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException {
    return write(future -> {
      output.write(magic);
      try {
        header.writeDelimitedTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      output.flush(false).whenComplete((len, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(len);
        }
      });
    });
  }

  @Override
  protected long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic) throws IOException {
    return write(future -> {
      try {
        trailer.writeTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      output.writeInt(trailer.getSerializedSize());
      output.write(magic);
      output.flush(false).whenComplete((len, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(len);
        }
      });
    });
  }

  @Override
  protected OutputStream getOutputStreamForCellEncoder() {
    return asyncOutputWrapper;
  }
}
