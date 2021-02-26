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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// imports for things that haven't moved from regionserver.wal yet.

/**
 * No-op implementation of {@link WALProvider} used when the WAL is disabled.
 *
 * Should only be used when severe data loss is acceptable.
 *
 */
@InterfaceAudience.Private
class DisabledWALProvider implements WALProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DisabledWALProvider.class);

  WAL disabled;

  @Override
  public void init(WALFactory factory, Configuration conf, String providerId, Abortable abortable)
      throws IOException {
    if (null != disabled) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    if (null == providerId) {
      providerId = "defaultDisabled";
    }
    disabled = new DisabledWAL(new Path(CommonFSUtils.getWALRootDir(conf), providerId), conf, null);
  }

  @Override
  public List<WAL> getWALs() {
    List<WAL> wals = new ArrayList<>(1);
    wals.add(disabled);
    return wals;
  }

  @Override
  public WAL getWAL(RegionInfo region) throws IOException {
    return disabled;
  }

  @Override
  public void close() throws IOException {
    disabled.close();
  }

  @Override
  public void shutdown() throws IOException {
    disabled.shutdown();
  }

  private static class DisabledWAL implements WAL {
    protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();
    protected final Path path;
    protected final WALCoprocessorHost coprocessorHost;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public DisabledWAL(final Path path, final Configuration conf,
        final List<WALActionsListener> listeners) {
      this.coprocessorHost = new WALCoprocessorHost(this, conf);
      this.path = path;
      if (null != listeners) {
        for(WALActionsListener listener : listeners) {
          registerWALActionsListener(listener);
        }
      }
    }

    @Override
    public void registerWALActionsListener(final WALActionsListener listener) {
      listeners.add(listener);
    }

    @Override
    public boolean unregisterWALActionsListener(final WALActionsListener listener) {
      return listeners.remove(listener);
    }

    @Override
    public Map<byte[], List<byte[]>> rollWriter() {
      if (!listeners.isEmpty()) {
        for (WALActionsListener listener : listeners) {
          listener.logRollRequested(WALActionsListener.RollRequestReason.ERROR);
        }
        for (WALActionsListener listener : listeners) {
          try {
            listener.preLogRoll(path, path);
          } catch (IOException exception) {
            LOG.debug("Ignoring exception from listener.", exception);
          }
        }
        for (WALActionsListener listener : listeners) {
          try {
            listener.postLogRoll(path, path);
          } catch (IOException exception) {
            LOG.debug("Ignoring exception from listener.", exception);
          }
        }
      }
      return null;
    }

    @Override
    public Map<byte[], List<byte[]>> rollWriter(boolean force) {
      return rollWriter();
    }

    @Override
    public void shutdown() {
      if(closed.compareAndSet(false, true)) {
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener listener : this.listeners) {
            listener.logCloseRequested();
          }
        }
      }
    }

    @Override
    public void close() {
      shutdown();
    }

    @Override
    public long appendData(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException {
      return append(info, key, edits, true);
    }

    @Override
    public long appendMarker(RegionInfo info, WALKeyImpl key, WALEdit edits)
      throws IOException {
      return append(info, key, edits, false);
    }

    private long append(RegionInfo info, WALKeyImpl key, WALEdit edits, boolean inMemstore)
        throws IOException {
      WriteEntry writeEntry = key.getMvcc().begin();
      if (!edits.isReplay()) {
        for (Cell cell : edits.getCells()) {
          PrivateCellUtil.setSequenceId(cell, writeEntry.getWriteNumber());
        }
      }
      key.setWriteEntry(writeEntry);
      if (!this.listeners.isEmpty()) {
        final long start = System.nanoTime();
        long len = 0;
        for (Cell cell : edits.getCells()) {
          len += PrivateCellUtil.estimatedSerializedSizeOf(cell);
        }
        final long elapsed = (System.nanoTime() - start) / 1000000L;
        for (WALActionsListener listener : this.listeners) {
          listener.postAppend(len, elapsed, key, edits);
        }
      }
      return -1;
    }

    @Override
    public void updateStore(byte[] encodedRegionName, byte[] familyName,
        Long sequenceid, boolean onlyIfGreater) { return; }

    @Override
    public void sync() {
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener listener : this.listeners) {
          listener.postSync(0L, 0);
        }
      }
    }

    @Override
    public void sync(long txid) {
      sync();
    }

    @Override
    public Long startCacheFlush(final byte[] encodedRegionName, Map<byte[], Long>
        flushedFamilyNamesToSeq) {
      return startCacheFlush(encodedRegionName, flushedFamilyNamesToSeq.keySet());
    }

    @Override
    public Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> flushedFamilyNames) {
      if (closed.get()) return null;
      return HConstants.NO_SEQNUM;
    }

    @Override
    public void completeCacheFlush(final byte[] encodedRegionName, long maxFlushedSeqId) {
    }

    @Override
    public void abortCacheFlush(byte[] encodedRegionName) {
    }

    @Override
    public WALCoprocessorHost getCoprocessorHost() {
      return coprocessorHost;
    }

    @Override
    public long getEarliestMemStoreSeqNum(byte[] encodedRegionName) {
      return HConstants.NO_SEQNUM;
    }

    @Override
    public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
      return HConstants.NO_SEQNUM;
    }

    @Override
    public String toString() {
      return "WAL disabled.";
    }

    @Override
    public OptionalLong getLogFileSizeIfBeingWritten(Path path) {
      return OptionalLong.empty();
    }
  }

  @Override
  public long getNumLogFiles() {
    return 0;
  }

  @Override
  public long getLogFileSize() {
    return 0;
  }

  @Override
  public void addWALActionsListener(WALActionsListener listener) {
    disabled.registerWALActionsListener(listener);
  }
}
