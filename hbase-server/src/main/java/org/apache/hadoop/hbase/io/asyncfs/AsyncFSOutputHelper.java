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
package org.apache.hadoop.hbase.io.asyncfs;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.EventLoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Helper class for creating AsyncFSOutput.
 */
@InterfaceAudience.Private
public final class AsyncFSOutputHelper {

  private AsyncFSOutputHelper() {
  }

  /**
   * Create {@link FanOutOneBlockAsyncDFSOutput} for {@link DistributedFileSystem}, and a simple
   * implementation for other {@link FileSystem} which wraps around a {@link FSDataOutputStream}.
   */
  public static AsyncFSOutput createOutput(FileSystem fs, Path f, boolean overwrite,
      boolean createParent, short replication, long blockSize, final EventLoop eventLoop)
      throws IOException {
    if (fs instanceof DistributedFileSystem) {
      return FanOutOneBlockAsyncDFSOutputHelper.createOutput((DistributedFileSystem) fs, f,
        overwrite, createParent, replication, blockSize, eventLoop);
    }
    final FSDataOutputStream fsOut;
    int bufferSize = fs.getConf().getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    if (createParent) {
      fsOut = fs.create(f, overwrite, bufferSize, replication, blockSize, null);
    } else {
      fsOut = fs.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, null);
    }
    final ExecutorService flushExecutor =
        Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("AsyncFSOutputFlusher-" + f.toString().replace("%", "%%")).build());
    return new AsyncFSOutput() {

      private final ByteArrayOutputStream out = new ByteArrayOutputStream();

      @Override
      public void write(final byte[] b, final int off, final int len) {
        if (eventLoop.inEventLoop()) {
          out.write(b, off, len);
        } else {
          eventLoop.submit(new Runnable() {
            public void run() {
              out.write(b, off, len);
            }
          }).syncUninterruptibly();
        }
      }

      @Override
      public void write(byte[] b) {
        write(b, 0, b.length);
      }

      @Override
      public void recoverAndClose(CancelableProgressable reporter) throws IOException {
        fsOut.close();
      }

      @Override
      public DatanodeInfo[] getPipeline() {
        return new DatanodeInfo[0];
      }

      @Override
      public <A> void flush(final A attachment, final CompletionHandler<Long, ? super A> handler,
          final boolean sync) {
        flushExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              synchronized (out) {
                out.writeTo(fsOut);
                out.reset();
              }
            } catch (final IOException e) {
              eventLoop.execute(new Runnable() {

                @Override
                public void run() {
                  handler.failed(e, attachment);
                }
              });
              return;
            }
            try {
              if (sync) {
                fsOut.hsync();
              } else {
                fsOut.hflush();
              }
              final long pos = fsOut.getPos();
              eventLoop.execute(new Runnable() {

                @Override
                public void run() {
                  handler.completed(pos, attachment);
                }
              });
            } catch (final IOException e) {
              eventLoop.execute(new Runnable() {

                @Override
                public void run() {
                  handler.failed(e, attachment);
                }
              });
            }
          }
        });
      }

      @Override
      public void close() throws IOException {
        try {
          flushExecutor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
              synchronized (out) {
                out.writeTo(fsOut);
                out.reset();
              }
              return null;
            }
          }).get();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        } catch (ExecutionException e) {
          Throwables.propagateIfPossible(e.getCause(), IOException.class);
          throw new IOException(e.getCause());
        } finally {
          flushExecutor.shutdown();
        }
        fsOut.close();
      }

      @Override
      public int buffered() {
        return out.size();
      }
    };
  }
}
