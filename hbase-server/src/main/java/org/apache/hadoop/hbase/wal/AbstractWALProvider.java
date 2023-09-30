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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.regionserver.PeerActionListener;
import org.apache.hadoop.hbase.replication.regionserver.SyncReplicationPeerInfoProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IOExceptionConsumer;
import org.apache.hadoop.hbase.util.IOExceptionRunnable;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Streams;

/**
 * Base class for a WAL Provider.
 * <p>
 * We will put some common logic here, especially for sync replication implementation, as it must do
 * some hacks before the normal wal creation operation.
 * <p>
 * All {@link WALProvider} implementations should extends this class instead of implement
 * {@link WALProvider} directly, except {@link DisabledWALProvider}.
 */
@InterfaceAudience.Private
public abstract class AbstractWALProvider implements WALProvider, PeerActionListener {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWALProvider.class);

  // should be package private; more visible for use in AbstractFSWAL
  public static final String WAL_FILE_NAME_DELIMITER = ".";

  protected WALFactory factory;
  protected Configuration conf;
  protected List<WALActionsListener> listeners = new ArrayList<>();
  protected String providerId;
  protected AtomicBoolean initialized = new AtomicBoolean(false);
  // for default wal provider, logPrefix won't change
  protected String logPrefix;
  protected Abortable abortable;

  // when switching from A to DA, we will put a Optional.empty into this map if there is no WAL for
  // the peer yet. When getting WAL from this map the caller should know that it should not use
  // the remote WAL any more.
  private final ConcurrentMap<String, Optional<WAL>> peerId2WAL = new ConcurrentHashMap<>();

  private final KeyLocker<String> createLock = new KeyLocker<>();

  // we need to have this because when getting meta wal, there is no peer info provider yet.
  private SyncReplicationPeerInfoProvider peerInfoProvider = new SyncReplicationPeerInfoProvider() {

    @Override
    public Optional<Pair<String, String>> getPeerIdAndRemoteWALDir(TableName table) {
      return Optional.empty();
    }

    @Override
    public boolean checkState(TableName table,
      BiPredicate<SyncReplicationState, SyncReplicationState> checker) {
      return false;
    }

  };

  @Override
  public final void init(WALFactory factory, Configuration conf, String providerId,
    Abortable server) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.factory = factory;
    this.conf = conf;
    this.abortable = server;
    doInit(factory, conf, providerId);
  }

  protected final void initWAL(WAL wal) throws IOException {
    boolean succ = false;
    try {
      wal.init();
      succ = true;
    } finally {
      if (!succ) {
        safeClose(wal);
      }
    }
  }

  // Use a timestamp to make it identical. That means, after we transit the peer to DA/S and then
  // back to A, the log prefix will be changed. This is used to simplify the implementation for
  // replication source, where we do not need to consider that a terminated shipper could be added
  // back.
  private String getRemoteWALPrefix(String peerId) {
    return factory.factoryId + "-" + EnvironmentEdgeManager.currentTime() + "-" + peerId;
  }

  private WAL getRemoteWAL(RegionInfo region, String peerId, String remoteWALDir)
    throws IOException {
    Optional<WAL> opt = peerId2WAL.get(peerId);
    if (opt != null) {
      return opt.orElse(null);
    }
    Lock lock = createLock.acquireLock(peerId);
    try {
      opt = peerId2WAL.get(peerId);
      if (opt != null) {
        return opt.orElse(null);
      }
      WAL wal = createRemoteWAL(region, ReplicationUtils.getRemoteWALFileSystem(conf, remoteWALDir),
        ReplicationUtils.getPeerRemoteWALDir(remoteWALDir, peerId), getRemoteWALPrefix(peerId),
        ReplicationUtils.SYNC_WAL_SUFFIX);
      initWAL(wal);
      peerId2WAL.put(peerId, Optional.of(wal));
      return wal;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public final WAL getWAL(RegionInfo region) throws IOException {
    if (region == null) {
      return getWAL0(null);
    }
    // deal with sync replication
    Optional<Pair<String, String>> peerIdAndRemoteWALDir =
      peerInfoProvider.getPeerIdAndRemoteWALDir(region.getTable());
    if (peerIdAndRemoteWALDir.isPresent()) {
      Pair<String, String> pair = peerIdAndRemoteWALDir.get();
      WAL wal = getRemoteWAL(region, pair.getFirst(), pair.getSecond());
      if (wal != null) {
        return wal;
      }
    }
    // fallback to normal WALProvider logic
    return getWAL0(region);
  }

  @Override
  public final List<WAL> getWALs() {
    return Streams
      .concat(peerId2WAL.values().stream().filter(Optional::isPresent).map(Optional::get),
        getWALs0().stream())
      .collect(Collectors.toList());
  }

  @Override
  public PeerActionListener getPeerActionListener() {
    return this;
  }

  @Override
  public void peerSyncReplicationStateChange(String peerId, SyncReplicationState from,
    SyncReplicationState to, int stage) {
    if (from == SyncReplicationState.ACTIVE) {
      if (stage == 0) {
        Lock lock = createLock.acquireLock(peerId);
        try {
          Optional<WAL> opt = peerId2WAL.get(peerId);
          if (opt != null) {
            opt.ifPresent(w -> w.skipRemoteWAL(to == SyncReplicationState.STANDBY));
          } else {
            // add a place holder to tell the getWAL caller do not use the remote WAL any more.
            peerId2WAL.put(peerId, Optional.empty());
          }
        } finally {
          lock.unlock();
        }
      } else if (stage == 1) {
        peerId2WAL.remove(peerId).ifPresent(AbstractWALProvider::safeClose);
      }
    }
  }

  @Override
  public void setSyncReplicationPeerInfoProvider(SyncReplicationPeerInfoProvider provider) {
    this.peerInfoProvider = provider;
  }

  private static void safeClose(WAL wal) {
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
  }

  private void cleanup(IOExceptionConsumer<WAL> cleanupWAL, IOExceptionRunnable finalCleanup)
    throws IOException {
    MultipleIOException.Builder builder = new MultipleIOException.Builder();
    for (Optional<WAL> wal : peerId2WAL.values()) {
      if (wal.isPresent()) {
        try {
          cleanupWAL.accept(wal.get());
        } catch (IOException e) {
          LOG.error("cleanup WAL failed", e);
          builder.add(e);
        }
      }
    }
    try {
      finalCleanup.run();
    } catch (IOException e) {
      LOG.error("cleanup WAL failed", e);
      builder.add(e);
    }
    if (!builder.isEmpty()) {
      throw builder.build();
    }
  }

  @Override
  public final void shutdown() throws IOException {
    cleanup(WAL::shutdown, this::shutdown0);
  }

  @Override
  public final void close() throws IOException {
    cleanup(WAL::close, this::close0);
  }

  private Stream<AbstractFSWAL<?>> remoteWALStream() {
    return peerId2WAL.values().stream().filter(Optional::isPresent).map(Optional::get)
      .filter(w -> w instanceof AbstractFSWAL).map(w -> (AbstractFSWAL<?>) w);
  }

  @Override
  public final long getNumLogFiles() {
    return remoteWALStream().mapToLong(AbstractFSWAL::getNumLogFiles).sum() + getNumLogFiles0();
  }

  @Override
  public final long getLogFileSize() {
    return remoteWALStream().mapToLong(AbstractFSWAL::getLogFileSize).sum() + getLogFileSize0();
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

  protected abstract WAL createRemoteWAL(RegionInfo region, FileSystem remoteFs, Path remoteWALDir,
    String prefix, String suffix) throws IOException;

  protected abstract void doInit(WALFactory factory, Configuration conf, String providerId)
    throws IOException;

  protected abstract WAL getWAL0(RegionInfo region) throws IOException;

  protected abstract List<WAL> getWALs0();

  protected abstract void shutdown0() throws IOException;

  protected abstract void close0() throws IOException;

  protected abstract long getNumLogFiles0();

  protected abstract long getLogFileSize0();

}
