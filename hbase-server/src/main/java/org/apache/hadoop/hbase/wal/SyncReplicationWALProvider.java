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

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getWALArchiveDirectoryName;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getWALDirectoryName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
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

  // only for injecting errors for testcase, do not use it for other purpose.
  @VisibleForTesting
  public static final String DUAL_WAL_IMPL = "hbase.wal.sync.impl";

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

  // Use a timestamp to make it identical. That means, after we transit the peer to DA/S and then
  // back to A, the log prefix will be changed. This is used to simplify the implementation for
  // replication source, where we do not need to consider that a terminated shipper could be added
  // back.
  private String getLogPrefix(String peerId) {
    return factory.factoryId + "-" + EnvironmentEdgeManager.currentTime() + "-" + peerId;
  }

  private DualAsyncFSWAL createWAL(String peerId, String remoteWALDir) throws IOException {
    Class<? extends DualAsyncFSWAL> clazz =
      conf.getClass(DUAL_WAL_IMPL, DualAsyncFSWAL.class, DualAsyncFSWAL.class);
    try {
      Constructor<?> constructor = null;
      for (Constructor<?> c : clazz.getDeclaredConstructors()) {
        if (c.getParameterCount() > 0) {
          constructor = c;
          break;
        }
      }
      if (constructor == null) {
        throw new IllegalArgumentException("No valid constructor provided for class " + clazz);
      }
      constructor.setAccessible(true);
      return (DualAsyncFSWAL) constructor.newInstance(
        CommonFSUtils.getWALFileSystem(conf),
        ReplicationUtils.getRemoteWALFileSystem(conf, remoteWALDir),
        CommonFSUtils.getWALRootDir(conf),
        ReplicationUtils.getPeerRemoteWALDir(remoteWALDir, peerId),
        getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
        conf, listeners, true, getLogPrefix(peerId), ReplicationUtils.SYNC_WAL_SUFFIX,
        eventLoopGroup, channelClass);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getTargetException();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new RuntimeException(cause);
    }
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
    if (from == SyncReplicationState.ACTIVE) {
      if (stage == 0) {
        Lock lock = createLock.acquireLock(peerId);
        try {
          Optional<DualAsyncFSWAL> opt = peerId2WAL.get(peerId);
          if (opt != null) {
            opt.ifPresent(w -> w.skipRemoteWAL(to == SyncReplicationState.STANDBY));
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

  private static final Pattern LOG_PREFIX_PATTERN = Pattern.compile(".*-\\d+-(.+)");

  /**
   * <p>
   * Returns the peer id if the wal file name is in the special group for a sync replication peer.
   * </p>
   * <p>
   * The prefix format is &lt;factoryId&gt;-&lt;ts&gt;-&lt;peerId&gt;.
   * </p>
   */
  public static Optional<String> getSyncReplicationPeerIdFromWALName(String name) {
    if (!name.endsWith(ReplicationUtils.SYNC_WAL_SUFFIX)) {
      // fast path to return earlier if the name is not for a sync replication peer.
      return Optional.empty();
    }
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(name);
    Matcher matcher = LOG_PREFIX_PATTERN.matcher(logPrefix);
    if (matcher.matches()) {
      return Optional.of(matcher.group(1));
    } else {
      return Optional.empty();
    }
  }

  @VisibleForTesting
  WALProvider getWrappedProvider() {
    return provider;
  }

}
