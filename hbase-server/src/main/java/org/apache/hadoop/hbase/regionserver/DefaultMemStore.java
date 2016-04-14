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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
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
  private static final Log LOG = LogFactory.getLog(DefaultMemStore.class);

  /**
   * Default constructor. Used for tests.
   */
  public DefaultMemStore() {
    this(HBaseConfiguration.create(), CellComparator.COMPARATOR);
  }

  /**
   * Constructor.
   * @param c Comparator
   */
  public DefaultMemStore(final Configuration conf, final CellComparator c) {
    super(conf, c);
  }

  void dump() {
    super.dump(LOG);
  }

  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot(long)}
   * @param flushOpSeqId the sequence id that is attached to the flush operation in the wal
   */
  @Override
  public MemStoreSnapshot snapshot(long flushOpSeqId) {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!getSnapshot().isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      if (!getActive().isEmpty()) {
        ImmutableSegment immutableSegment = SegmentFactory.instance().
            createImmutableSegment(getConfiguration(), getActive());
        setSnapshot(immutableSegment);
        setSnapshotSize(keySize());
        resetCellSet();
      }
    }
    return new MemStoreSnapshot(this.snapshotId, getSnapshot());
  }

  @Override
  protected List<SegmentScanner> getListOfScanners(long readPt) throws IOException {
    List<SegmentScanner> list = new ArrayList<SegmentScanner>(2);
    list.add(0, getActive().getSegmentScanner(readPt));
    list.add(1, getSnapshot().getSegmentScanner(readPt));
    return list;
  }

  @Override
  protected List<Segment> getListOfSegments() throws IOException {
    List<Segment> list = new ArrayList<Segment>(2);
    list.add(0, getActive());
    list.add(1, getSnapshot());
    return list;
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    return getLowest(
        getNextRow(cell, getActive().getCellSet()),
        getNextRow(cell, getSnapshot().getCellSet()));
  }

  @Override public void updateLowestUnflushedSequenceIdInWal(boolean onlyIfMoreRecent) {
  }

  /**
   * @return Total memory occupied by this MemStore.
   */
  @Override
  public long size() {
    return heapSize();
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
  public void finalizeFlush() {
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
    long size = 0;
    final int count = 10000;
    byte [] fam = Bytes.toBytes("col");
    byte [] qf = Bytes.toBytes("umn");
    byte [] empty = new byte[0];
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    DefaultMemStore memstore2 = new DefaultMemStore();
    for (int i = 0; i < count; i++) {
      size += memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, new byte[i]));
    }
    LOG.info("memstore2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    LOG.info("Exiting.");
  }
}