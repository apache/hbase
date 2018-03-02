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
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
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

  private SyncReplicationPeerInfoProvider peerInfoProvider =
    new DefaultSyncReplicationPeerInfoProvider();

  private WALFactory factory;

  private Configuration conf;

  private List<WALActionsListener> listeners = new ArrayList<>();

  private EventLoopGroup eventLoopGroup;

  private Class<? extends Channel> channelClass;

  private AtomicBoolean initialized = new AtomicBoolean(false);

  // when switching from A to DA, we will put a Optional.empty into this map if there is no WAL for
  // the peer yet. When getting WAL from this map the caller should know that it should not use
  // DualAsyncFSWAL any more.
  private final ConcurrentMap<String, Optional<DualAsyncFSWAL>> peerId2WAL =
    new ConcurrentHashMap<>();

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
    return new DualAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf),
      ReplicationUtils.getRemoteWALFileSystem(conf, remoteWALDir),
      CommonFSUtils.getWALRootDir(conf),
      ReplicationUtils.getRemoteWALDirForPeer(remoteWALDir, peerId),
      getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
      conf, listeners, true, getLogPrefix(peerId), LOG_SUFFIX, eventLoopGroup, channelClass);
  }

  private DualAsyncFSWAL getWAL(String peerId, String remoteWALDir) throws IOException {
    Optional<DualAsyncFSWAL> opt = peerId2WAL.get(peerId);
    if (opt != null) {
      return opt.orElse(null);
    }
    Lock lock = createLock.acquireLock(peerId);
    try {
      opt = peerId2WAL.get(peerId);
      if (opt != null) {
        return opt.orElse(null);
      }
      DualAsyncFSWAL wal = createWAL(peerId, remoteWALDir);
      boolean succ = false;
      try {
        wal.init();
        succ = true;
      } finally {
        if (!succ) {
          wal.close();
        }
      }
      peerId2WAL.put(peerId, Optional.of(wal));
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
    WAL wal = null;
    Optional<Pair<String, String>> peerIdAndRemoteWALDir =
        peerInfoProvider.getPeerIdAndRemoteWALDir(region.getTable());
    if (peerIdAndRemoteWALDir.isPresent()) {
      Pair<String, String> pair = peerIdAndRemoteWALDir.get();
      wal = getWAL(pair.getFirst(), pair.getSecond());
    }
    return wal != null ? wal : provider.getWAL(region);
  }

  private Stream<WAL> getWALStream() {
    return Streams.concat(
      peerId2WAL.values().stream().filter(Optional::isPresent).map(Optional::get),
      provider.getWALs().stream());
  }

  @Override
  public List<WAL> getWALs() {
    return getWALStream().collect(Collectors.toList());
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (Optional<DualAsyncFSWAL> wal : peerId2WAL.values()) {
      if (wal.isPresent()) {
        try {
          wal.get().shutdown();
        } catch (IOException e) {
          LOG.error("Shutdown WAL failed", e);
          failure = e;
        }
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
    for (Optional<DualAsyncFSWAL> wal : peerId2WAL.values()) {
      if (wal.isPresent()) {
        try {
          wal.get().close();
        } catch (IOException e) {
          LOG.error("Close WAL failed", e);
          failure = e;
        }
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
    return peerId2WAL.values().stream().filter(Optional::isPresent).map(Optional::get)
      .mapToLong(DualAsyncFSWAL::getLogFileSize).sum() + provider.getLogFileSize();
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
    if (from == SyncReplicationState.ACTIVE && to == SyncReplicationState.DOWNGRADE_ACTIVE) {
      if (stage == 0) {
        Lock lock = createLock.acquireLock(peerId);
        try {
          Optional<DualAsyncFSWAL> opt = peerId2WAL.get(peerId);
          if (opt != null) {
            opt.ifPresent(DualAsyncFSWAL::skipRemoteWal);
          } else {
            // add a place holder to tell the getWAL caller do not use DualAsyncFSWAL any more.
            peerId2WAL.put(peerId, Optional.empty());
          }
        } finally {
          lock.unlock();
        }
      } else if (stage == 1) {
        peerId2WAL.remove(peerId).ifPresent(this::safeClose);
      }
    }
  }

  private static class DefaultSyncReplicationPeerInfoProvider
      implements SyncReplicationPeerInfoProvider {

    @Override
    public Optional<Pair<String, String>> getPeerIdAndRemoteWALDir(TableName table) {
      return Optional.empty();
    }

    @Override
    public boolean checkState(TableName table,
        BiPredicate<SyncReplicationState, SyncReplicationState> checker) {
      return false;
    }
  }
}
