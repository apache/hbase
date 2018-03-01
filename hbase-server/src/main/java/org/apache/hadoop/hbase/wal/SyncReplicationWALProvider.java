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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getWALArchiveDirectoryName;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getWALDirectoryName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.regionserver.PeerActionListener;
import org.apache.hadoop.hbase.replication.regionserver.SyncReplicationPeerInfoProvider;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Streams;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

/**
 * The special {@link WALProvider} for synchronous replication.
 * <p>
 * It works like an interceptor, when getting WAL, first it will check if the given region should be
 * replicated synchronously, if so it will return a special WAL for it, otherwise it will delegate
 * the request to the normal {@link WALProvider}.
 */
@InterfaceAudience.Private
public class SyncReplicationWALProvider implements WALProvider, PeerActionListener {

  private static final Logger LOG = LoggerFactory.getLogger(SyncReplicationWALProvider.class);

  private static final String LOG_SUFFIX = ".syncrep";

  private final WALProvider provider;

  private SyncReplicationPeerInfoProvider peerInfoProvider;

  private WALFactory factory;

  private Configuration conf;

  private List<WALActionsListener> listeners = new ArrayList<>();

  private EventLoopGroup eventLoopGroup;

  private Class<? extends Channel> channelClass;

  private AtomicBoolean initialized = new AtomicBoolean(false);

  private final ConcurrentMap<String, DualAsyncFSWAL> peerId2WAL = new ConcurrentHashMap<>();

  private final KeyLocker<String> createLock = new KeyLocker<>();

  SyncReplicationWALProvider(WALProvider provider) {
    this.provider = provider;
  }

  public void setPeerInfoProvider(SyncReplicationPeerInfoProvider peerInfoProvider) {
    this.peerInfoProvider = peerInfoProvider;
  }

  @Override
  public void init(WALFactory factory, Configuration conf, String providerId) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    provider.init(factory, conf, providerId);
    this.conf = conf;
    this.factory = factory;
    Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass =
      NettyAsyncFSWALConfigHelper.getEventLoopConfig(conf);
    eventLoopGroup = eventLoopGroupAndChannelClass.getFirst();
    channelClass = eventLoopGroupAndChannelClass.getSecond();
  }

  private String getLogPrefix(String peerId) {
    return factory.factoryId + WAL_FILE_NAME_DELIMITER + peerId;
  }

  private DualAsyncFSWAL createWAL(String peerId, String remoteWALDir) throws IOException {
    Path remoteWALDirPath = new Path(remoteWALDir);
    FileSystem remoteFs = remoteWALDirPath.getFileSystem(conf);
    return new DualAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), remoteFs,
      CommonFSUtils.getWALRootDir(conf), new Path(remoteWALDirPath, peerId),
      getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
      conf, listeners, true, getLogPrefix(peerId), LOG_SUFFIX, eventLoopGroup, channelClass);
  }

  private DualAsyncFSWAL getWAL(String peerId, String remoteWALDir) throws IOException {
    DualAsyncFSWAL wal = peerId2WAL.get(peerId);
    if (wal != null) {
      return wal;
    }
    Lock lock = createLock.acquireLock(peerId);
    try {
      wal = peerId2WAL.get(peerId);
      if (wal == null) {
        wal = createWAL(peerId, remoteWALDir);
        peerId2WAL.put(peerId, wal);
        wal.init();
      }
      return wal;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public WAL getWAL(RegionInfo region) throws IOException {
    if (region == null) {
      return provider.getWAL(null);
    }
    Optional<Pair<String, String>> peerIdAndRemoteWALDir =
      peerInfoProvider.getPeerIdAndRemoteWALDir(region);
    if (peerIdAndRemoteWALDir.isPresent()) {
      Pair<String, String> pair = peerIdAndRemoteWALDir.get();
      return getWAL(pair.getFirst(), pair.getSecond());
    } else {
      return provider.getWAL(region);
    }
  }

  private Stream<WAL> getWALStream() {
    return Streams.concat(peerId2WAL.values().stream(), provider.getWALs().stream());
  }

  @Override
  public List<WAL> getWALs() {
    return getWALStream().collect(Collectors.toList());
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (DualAsyncFSWAL wal : peerId2WAL.values()) {
      try {
        wal.shutdown();
      } catch (IOException e) {
        LOG.error("Shutdown WAL failed", e);
        failure = e;
      }
    }
    provider.shutdown();
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (DualAsyncFSWAL wal : peerId2WAL.values()) {
      try {
        wal.close();
      } catch (IOException e) {
        LOG.error("Close WAL failed", e);
        failure = e;
      }
    }
    provider.close();
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public long getNumLogFiles() {
    return peerId2WAL.size() + provider.getNumLogFiles();
  }

  @Override
  public long getLogFileSize() {
    return peerId2WAL.values().stream().mapToLong(DualAsyncFSWAL::getLogFileSize).sum() +
      provider.getLogFileSize();
  }

  private void safeClose(WAL wal) {
    if (wal != null) {
      try {
        wal.close();
      } catch (IOException e) {
        LOG.error("Close WAL failed", e);
      }
    }
  }

  @Override
  public void addWALActionsListener(WALActionsListener listener) {
    listeners.add(listener);
    provider.addWALActionsListener(listener);
  }

  @Override
  public void peerSyncReplicationStateChange(String peerId, SyncReplicationState from,
      SyncReplicationState to, int stage) {
    // TODO: stage 0
    if (from == SyncReplicationState.ACTIVE && to == SyncReplicationState.DOWNGRADE_ACTIVE &&
      stage == 1) {
      safeClose(peerId2WAL.remove(peerId));
    }
  }
}
