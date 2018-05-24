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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

class DualAsyncFSWALForTest extends DualAsyncFSWAL {

  private boolean localBroken;

  private boolean remoteBroken;

  private CountDownLatch arrive;

  private CountDownLatch resume;

  private final class MyCombinedAsyncWriter implements AsyncWriter {

    private final AsyncWriter localWriter;

    private final AsyncWriter remoteWriter;

    public MyCombinedAsyncWriter(AsyncWriter localWriter, AsyncWriter remoteWriter) {
      this.localWriter = localWriter;
      this.remoteWriter = remoteWriter;
    }

    @Override
    public long getLength() {
      return localWriter.getLength();
    }

    @Override
    public void close() throws IOException {
      Closeables.close(localWriter, true);
      Closeables.close(remoteWriter, true);
    }

    @Override
    public CompletableFuture<Long> sync() {
      CompletableFuture<Long> localFuture;
      CompletableFuture<Long> remoteFuture;
      if (!localBroken) {
        localFuture = localWriter.sync();
      } else {
        localFuture = new CompletableFuture<>();
        localFuture.completeExceptionally(new IOException("Inject error"));
      }
      if (!remoteBroken) {
        remoteFuture = remoteWriter.sync();
      } else {
        remoteFuture = new CompletableFuture<>();
        remoteFuture.completeExceptionally(new IOException("Inject error"));
      }
      return CompletableFuture.allOf(localFuture, remoteFuture).thenApply(v -> {
        return localFuture.getNow(0L);
      });
    }

    @Override
    public void append(Entry entry) {
      if (!localBroken) {
        localWriter.append(entry);
      }
      if (!remoteBroken) {
        remoteWriter.append(entry);
      }
    }
  }

  public DualAsyncFSWALForTest(FileSystem fs, FileSystem remoteFs, Path rootDir, Path remoteWalDir,
      String logDir, String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
    super(fs, remoteFs, rootDir, remoteWalDir, logDir, archiveDir, conf, listeners, failIfWALExists,
      prefix, suffix, eventLoopGroup, channelClass);
  }

  @Override
  protected AsyncWriter createCombinedAsyncWriter(AsyncWriter localWriter,
      AsyncWriter remoteWriter) {
    return new MyCombinedAsyncWriter(localWriter, remoteWriter);
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    if (arrive != null) {
      arrive.countDown();
      try {
        resume.await();
      } catch (InterruptedException e) {
      }
    }
    if (localBroken || remoteBroken) {
      throw new IOException("WAL broken");
    }
    return super.createWriterInstance(path);
  }

  public void setLocalBroken() {
    this.localBroken = true;
  }

  public void setRemoteBroken() {
    this.remoteBroken = true;
  }

  public void suspendLogRoll() {
    arrive = new CountDownLatch(1);
    resume = new CountDownLatch(1);
  }

  public void waitUntilArrive() throws InterruptedException {
    arrive.await();
  }

  public void resumeLogRoll() {
    resume.countDown();
  }
}
