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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

public class BrokenRemoteAsyncFSWALProvider extends AsyncFSWALProvider {

  static class BrokenRemoteAsyncFSWAL extends AsyncFSWAL {

    private final class MyCombinedAsyncWriter implements WALProvider.AsyncWriter {

      private final WALProvider.AsyncWriter localWriter;

      private final WALProvider.AsyncWriter remoteWriter;

      // remoteWriter on the first
      public MyCombinedAsyncWriter(WALProvider.AsyncWriter localWriter,
        WALProvider.AsyncWriter remoteWriter) {
        this.localWriter = localWriter;
        this.remoteWriter = remoteWriter;
      }

      @Override
      public long getLength() {
        return localWriter.getLength();
      }

      @Override
      public long getSyncedLength() {
        return this.localWriter.getSyncedLength();
      }

      @Override
      public void close() throws IOException {
        Closeables.close(localWriter, true);
        Closeables.close(remoteWriter, true);
      }

      @Override
      public CompletableFuture<Long> sync(boolean forceSync) {
        CompletableFuture<Long> localFuture;
        CompletableFuture<Long> remoteFuture;

        if (!localBroken) {
          localFuture = localWriter.sync(forceSync);
        } else {
          localFuture = new CompletableFuture<>();
          localFuture.completeExceptionally(new IOException("Inject error"));
        }
        if (!remoteBroken) {
          remoteFuture = remoteWriter.sync(forceSync);
        } else {
          remoteFuture = new CompletableFuture<>();
          remoteFuture.completeExceptionally(new IOException("Inject error"));
        }
        return CompletableFuture.allOf(localFuture, remoteFuture).thenApply(v -> {
          return localFuture.getNow(0L);
        });
      }

      @Override
      public void append(WAL.Entry entry) {
        if (!localBroken) {
          localWriter.append(entry);
        }
        if (!remoteBroken) {
          remoteWriter.append(entry);
        }
      }
    }

    private volatile boolean localBroken;

    private volatile boolean remoteBroken;

    private CountDownLatch arrive;

    private CountDownLatch resume;

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

    public BrokenRemoteAsyncFSWAL(FileSystem fs, Abortable abortable, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, FileSystem remoteFs, Path remoteWALDir,
      EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass,
      StreamSlowMonitor monitor) throws FailedLogCloseException, IOException {
      super(fs, abortable, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix, remoteFs, remoteWALDir, eventLoopGroup, channelClass, monitor);
    }

    @Override
    protected WALProvider.AsyncWriter createCombinedWriter(WALProvider.AsyncWriter localWriter,
      WALProvider.AsyncWriter remoteWriter) {
      return new MyCombinedAsyncWriter(localWriter, remoteWriter);
    }

    @Override
    protected WALProvider.AsyncWriter createWriterInstance(FileSystem fs, Path path)
      throws IOException {
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
      return super.createWriterInstance(fs, path);
    }
  }

  @Override
  protected AsyncFSWAL createWAL() throws IOException {
    return new BrokenRemoteAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
      CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.getFactoryId()),
      getWALArchiveDirectoryName(conf, factory.getFactoryId()), conf, listeners, true, logPrefix,
      META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, null, null,
      eventLoopGroup, channelClass,
      factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));

  }

  @Override
  protected WAL createRemoteWAL(RegionInfo region, FileSystem remoteFs, Path remoteWALDir,
    String prefix, String suffix) throws IOException {
    return new BrokenRemoteAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
      CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.getFactoryId()),
      getWALArchiveDirectoryName(conf, factory.getFactoryId()), conf, listeners, true, prefix,
      suffix, remoteFs, remoteWALDir, eventLoopGroup, channelClass,
      factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));
  }

}
