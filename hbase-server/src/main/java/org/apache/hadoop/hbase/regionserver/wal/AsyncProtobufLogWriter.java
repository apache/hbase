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
import com.google.common.primitives.Ints;

import io.netty.channel.EventLoop;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.channels.CompletionHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
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
public class AsyncProtobufLogWriter extends AbstractProtobufLogWriter implements
    AsyncFSWALProvider.AsyncWriter {

  private static final Log LOG = LogFactory.getLog(AsyncProtobufLogWriter.class);

  private static final class BlockingCompletionHandler implements CompletionHandler<Long, Void> {

    private long size;

    private Throwable error;

    private boolean finished;

    @Override
    public void completed(Long result, Void attachment) {
      synchronized (this) {
        size = result.longValue();
        finished = true;
        notifyAll();
      }
    }

    @Override
    public void failed(Throwable exc, Void attachment) {
      synchronized (this) {
        error = exc;
        finished = true;
        notifyAll();
      }
    }

    public long get() throws IOException {
      synchronized (this) {
        while (!finished) {
          try {
            wait();
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
        }
        if (error != null) {
          Throwables.propagateIfPossible(error, IOException.class);
          throw new RuntimeException(error);
        }
        return size;
      }
    }
  }

  private final EventLoop eventLoop;

  private AsyncFSOutput output;

  private ByteArrayOutputStream buf;

  public AsyncProtobufLogWriter(EventLoop eventLoop) {
    this.eventLoop = eventLoop;
  }

  @Override
  public void append(Entry entry) {
    buf.reset();
    entry.setCompressionContext(compressionContext);
    try {
      entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
          .writeDelimitedTo(buf);
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    length.addAndGet(buf.size());
    output.write(buf.getBuffer(), 0, buf.size());
    try {
      for (Cell cell : entry.getEdit().getCells()) {
        buf.reset();
        cellEncoder.write(cell);
        length.addAndGet(buf.size());
        output.write(buf.getBuffer(), 0, buf.size());
      }
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  @Override
  public <A> void sync(CompletionHandler<Long, A> handler, A attachment) {
    output.flush(attachment, handler, false);
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
    this.buf = new ByteArrayOutputStream();
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException {
    buf.reset();
    header.writeDelimitedTo(buf);
    final BlockingCompletionHandler handler = new BlockingCompletionHandler();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        output.write(ProtobufLogReader.PB_WAL_MAGIC);
        output.write(buf.getBuffer(), 0, buf.size());
        output.flush(null, handler, false);
      }
    });
    return handler.get();
  }

  @Override
  protected long writeWALTrailerAndMagic(WALTrailer trailer, final byte[] magic)
      throws IOException {
    buf.reset();
    trailer.writeTo(buf);
    final BlockingCompletionHandler handler = new BlockingCompletionHandler();
    eventLoop.execute(new Runnable() {
      public void run() {
        output.write(buf.getBuffer(), 0, buf.size());
        output.write(Ints.toByteArray(buf.size()));
        output.write(magic);
        output.flush(null, handler, false);
      }
    });
    return handler.get();
  }

  @Override
  protected OutputStream getOutputStreamForCellEncoder() {
    return buf;
  }
}
