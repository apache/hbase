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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * An {@link AsyncFSOutput} wraps a {@link FSDataOutputStream}.
 */
@InterfaceAudience.Private
public class WrapperAsyncFSOutput implements AsyncFSOutput {

  private final FSDataOutputStream out;

  private ByteArrayOutputStream buffer = new ByteArrayOutputStream();

  private final ExecutorService executor;

  private volatile long syncedLength = 0;

  public WrapperAsyncFSOutput(Path file, FSDataOutputStream out) {
    this.out = out;
    this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("AsyncFSOutputFlusher-" + file.toString().replace("%", "%%")).build());
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    buffer.write(b, off, len);
  }

  @Override
  public void writeInt(int i) {
    buffer.writeInt(i);
  }

  @Override
  public void write(ByteBuffer bb) {
    buffer.write(bb, bb.position(), bb.remaining());
  }

  @Override
  public int buffered() {
    return buffer.size();
  }

  @Override
  public DatanodeInfo[] getPipeline() {
    return new DatanodeInfo[0];
  }

  private void flush0(CompletableFuture<Long> future, ByteArrayOutputStream buffer, boolean sync) {
    try {
      if (buffer.size() > 0) {
        out.write(buffer.getBuffer(), 0, buffer.size());
        if (sync) {
          out.hsync();
        } else {
          out.hflush();
        }
      }
      long pos = out.getPos();
      /**
       * This flush0 method could only be called by single thread, so here we could
       * safely overwrite without any synchronization.
       */
      this.syncedLength = pos;
      future.complete(pos);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return;
    }
  }

  @Override
  public CompletableFuture<Long> flush(boolean sync) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    ByteArrayOutputStream buffer = this.buffer;
    this.buffer = new ByteArrayOutputStream();
    executor.execute(() -> flush0(future, buffer, sync));
    return future;
  }

  @Override
  public void recoverAndClose(CancelableProgressable reporter) throws IOException {
    executor.shutdown();
    out.close();
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(buffer.size() == 0, "should call flush first before calling close");
    executor.shutdown();
    out.close();
  }

  @Override
  public boolean isBroken() {
    return false;
  }

  @Override
  public long getSyncedLength() {
    return this.syncedLength;
  }
}
