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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * The MemStore holds in-memory modifications to the Store.  Modifications
 * are {@link Cell}s.  When asked to flush, current memstore is moved
 * to snapshot and is cleared.  We continue to serve edits out of new memstore
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 *  <p>
 * The MemStore functions should not be called in parallel. Callers should hold
 *  write and read locks. This is done in {@link HStore}.
 *  </p>
 *
 * TODO: Adjust size of the memstore when we remove items because they have
 * been deleted.
 * TODO: With new KVSLS, need to make sure we update HeapSize with difference
 * in KV size.
 */
@InterfaceAudience.Private
public class DefaultMemStore extends AbstractMemStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMemStore.class);

  public final static long DEEP_OVERHEAD = ClassSize.align(AbstractMemStore.DEEP_OVERHEAD);
  public final static long FIXED_OVERHEAD = ClassSize.align(AbstractMemStore.FIXED_OVERHEAD);
  /**
   * Default constructor. Used for tests.
   */
  public DefaultMemStore() {
    this(HBaseConfiguration.create(), CellComparator.getInstance());
  }

  /**
   * Constructor.
   * @param c Comparator
   */
  public DefaultMemStore(final Configuration conf, final CellComparator c) {
    super(conf, c);
  }

  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot(long)}
   */
  @Override
  public MemStoreSnapshot snapshot() {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!this.snapshot.isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      if (!this.active.isEmpty()) {
        ImmutableSegment immutableSegment = SegmentFactory.instance().
            createImmutableSegment(this.active);
        this.snapshot = immutableSegment;
        resetActive();
      }
    }
    return new MemStoreSnapshot(this.snapshotId, this.snapshot);
  }

  /**
   * On flush, how much memory we will clear from the active cell set.
   *
   * @return size of data that is going to be flushed from active set
   */
  @Override
  public MemStoreSize getFlushableSize() {
    MemStoreSize snapshotSize = getSnapshotSize();
    return snapshotSize.getDataSize() > 0 ? snapshotSize
        : new MemStoreSize(active.getMemStoreSize());
  }

  @Override
  protected long keySize() {
    return this.active.keySize();
  }

  @Override
  protected long heapSize() {
    return this.active.heapSize();
  }

  @Override
  /*
   * Scanners are ordered from 0 (oldest) to newest in increasing order.
   */
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    List<KeyValueScanner> list = new ArrayList<>();
    long order = snapshot.getNumOfSegments();
    order = addToScanners(active, readPt, order, list);
    addToScanners(snapshot.getAllSegments(), readPt, order, list);
    return list;
  }

  @Override
  protected List<Segment> getSegments() throws IOException {
    List<Segment> list = new ArrayList<>(2);
    list.add(this.active);
    list.add(this.snapshot);
    return list;
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    return getLowest(
        getNextRow(cell, this.active.getCellSet()),
        getNextRow(cell, this.snapshot.getCellSet()));
  }

  @Override public void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfMoreRecent) {
  }

  @Override
  public MemStoreSize size() {
    return new MemStoreSize(active.getMemStoreSize());
  }

  /**
   * Check whether anything need to be done based on the current active set size
   * Nothing need to be done for the DefaultMemStore
   */
  @Override
  protected void checkActiveSize() {
    return;
  }

  @Override
  public long preFlushSeqIDEstimation() {
    return HConstants.NO_SEQNUM;
  }

  @Override public boolean isSloppy() {
    return false;
  }

  /**
   * Code to help figure if our approximation of object heap sizes is close
   * enough.  See hbase-900.  Fills memstores then waits so user can heap
   * dump and bring up resultant hprof in something like jprofiler which
   * allows you get 'deep size' on objects.
   * @param args main args
   */
  public static void main(String [] args) {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
      runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    DefaultMemStore memstore1 = new DefaultMemStore();
    // TODO: x32 vs x64
    final int count = 10000;
    byte [] fam = Bytes.toBytes("col");
    byte [] qf = Bytes.toBytes("umn");
    byte [] empty = new byte[0];
    MemStoreSizing memstoreSizing = new MemStoreSizing();
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty), memstoreSizing);
    }
    LOG.info("memstore1 estimated size="
        + (memstoreSizing.getDataSize() + memstoreSizing.getHeapSize()));
    for (int i = 0; i < count; i++) {
      memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty), memstoreSizing);
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)="
        + (memstoreSizing.getDataSize() + memstoreSizing.getHeapSize()));
    // Make a variably sized memstore.
    DefaultMemStore memstore2 = new DefaultMemStore();
    memstoreSizing = new MemStoreSizing();
    for (int i = 0; i < count; i++) {
      memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, new byte[i]), memstoreSizing);
    }
    LOG.info("memstore2 estimated size="
        + (memstoreSizing.getDataSize() + memstoreSizing.getHeapSize()));
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    LOG.info("Exiting.");
  }
}
