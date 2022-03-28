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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ByteBufferWriter;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutputHelper;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * AsyncWriter for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class AsyncProtobufLogWriter extends AbstractProtobufLogWriter
    implements AsyncFSWALProvider.AsyncWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncProtobufLogWriter.class);

  private final EventLoopGroup eventLoopGroup;

  private final Class<? extends Channel> channelClass;

  private volatile AsyncFSOutput output;
  /**
   * Save {@link AsyncFSOutput#getSyncedLength()} when {@link #output} is closed.
   */
  private volatile long finalSyncedLength = -1;

  private static final class OutputStreamWrapper extends OutputStream
      implements ByteBufferWriter {

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
  private long waitTimeout;

  public AsyncProtobufLogWriter(EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) {
    this.eventLoopGroup = eventLoopGroup;
    this.channelClass = channelClass;
    // Reuse WAL_ROLL_WAIT_TIMEOUT here to avoid an infinite wait if somehow a wait on a future
    // never completes. The objective is the same. We want to propagate an exception to trigger
    // an abort if we seem to be hung.
    if (this.conf == null) {
      this.conf = HBaseConfiguration.create();
    }
    this.waitTimeout = this.conf.getLong(AbstractWALRoller.WAL_ROLL_WAIT_TIMEOUT,
      AbstractWALRoller.DEFAULT_WAL_ROLL_WAIT_TIMEOUT);
  }

  /*
   * @return class name which is recognized by hbase-1.x to avoid ProtobufLogReader throwing error:
   *   IOException: Got unknown writer class: AsyncProtobufLogWriter
   */
  @Override
  protected String getWriterClassName() {
    return "ProtobufLogWriter";
  }

  @Override
  public void append(Entry entry) {
    int buffered = output.buffered();
    try {
      entry.getKey().
        getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
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
  public CompletableFuture<Long> sync(boolean forceSync) {
    return output.flush(forceSync);
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
    /**
     * We have to call {@link AsyncFSOutput#getSyncedLength()}
     * after {@link AsyncFSOutput#close()} to get the final length
     * synced to underlying filesystem because {@link AsyncFSOutput#close()}
     * may also flush some data to underlying filesystem.
     */
    this.finalSyncedLength = this.output.getSyncedLength();
    this.output = null;
  }

  public AsyncFSOutput getOutput() {
    return this.output;
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize, StreamSlowMonitor monitor) throws IOException,
      StreamLacksCapabilityException {
    this.output = AsyncFSOutputHelper.createOutput(fs, path, overwritable, false, replication,
        blockSize, eventLoopGroup, channelClass, monitor);
    this.asyncOutputWrapper = new OutputStreamWrapper(output);
  }

  @Override
  protected void closeOutput() {
    if (this.output != null) {
      try {
        this.output.close();
      } catch (IOException e) {
        LOG.warn("Close output failed", e);
      }
    }
  }
  
  private long writeWALMetadata(Consumer<CompletableFuture<Long>> action) throws IOException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    action.accept(future);
    try {
      return future.get(waitTimeout, TimeUnit.MILLISECONDS).longValue();
    } catch (InterruptedException e) {
      InterruptedIOException ioe = new InterruptedIOException();
      ioe.initCause(e);
      throw ioe;
    } catch (ExecutionException | TimeoutException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException {
    return writeWALMetadata(future -> {
      output.write(magic);
      try {
        header.writeDelimitedTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      addListener(output.flush(false), (len, error) -> {
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
    return writeWALMetadata(future -> {
      try {
        trailer.writeTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      output.writeInt(trailer.getSerializedSize());
      output.write(magic);
      addListener(output.flush(false), (len, error) -> {
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

  @Override
  public long getSyncedLength() {
   /**
    * The statement "this.output = null;" in {@link AsyncProtobufLogWriter#close}
    * is a sync point, if output is null, then finalSyncedLength must set,
    * so we can return finalSyncedLength, else we return output.getSyncedLength
    */
    AsyncFSOutput outputToUse = this.output;
    if(outputToUse == null) {
        long finalSyncedLengthToUse = this.finalSyncedLength;
        assert finalSyncedLengthToUse >= 0;
        return finalSyncedLengthToUse;
    }
    return outputToUse.getSyncedLength();
  }
}
