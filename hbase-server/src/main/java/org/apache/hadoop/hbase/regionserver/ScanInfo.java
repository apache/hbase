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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import com.google.common.annotations.VisibleForTesting;

/**
 * Immutable information for scans over a store.
 */
// Has to be public for PartitionedMobCompactor to access; ditto on tests making use of a few of
// the accessors below. Shutdown access. TODO
@VisibleForTesting
@InterfaceAudience.Private
public class ScanInfo {
  private byte[] family;
  private int minVersions;
  private int maxVersions;
  private long ttl;
  private KeepDeletedCells keepDeletedCells;
  private long timeToPurgeDeletes;
  private CellComparator comparator;
  private long tableMaxRowSize;
  private boolean usePread;
  private long cellsPerTimeoutCheck;
  private boolean parallelSeekEnabled;
  private final Configuration conf;

  public static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
      + (2 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_INT)
      + (4 * Bytes.SIZEOF_LONG) + (3 * Bytes.SIZEOF_BOOLEAN));

  /**
   * @param conf
   * @param family {@link HColumnDescriptor} describing the column family
   * @param ttl Store's TTL (in ms)
   * @param timeToPurgeDeletes duration in ms after which a delete marker can
   *        be purged during a major compaction.
   * @param comparator The store's comparator
   */
  public ScanInfo(final Configuration conf, final HColumnDescriptor family, final long ttl,
      final long timeToPurgeDeletes, final CellComparator comparator) {
    this(conf, family.getName(), family.getMinVersions(), family.getMaxVersions(), ttl, family
        .getKeepDeletedCells(), timeToPurgeDeletes, comparator);
  }

  /**
   * @param conf
   * @param family Name of this store's column family
   * @param minVersions Store's MIN_VERSIONS setting
   * @param maxVersions Store's VERSIONS setting
   * @param ttl Store's TTL (in ms)
   * @param timeToPurgeDeletes duration in ms after which a delete marker can
   *        be purged during a major compaction.
   * @param keepDeletedCells Store's keepDeletedCells setting
   * @param comparator The store's comparator
   */
  public ScanInfo(final Configuration conf, final byte[] family, final int minVersions,
      final int maxVersions, final long ttl, final KeepDeletedCells keepDeletedCells,
      final long timeToPurgeDeletes, final CellComparator comparator) {
    this.family = family;
    this.minVersions = minVersions;
    this.maxVersions = maxVersions;
    this.ttl = ttl;
    this.keepDeletedCells = keepDeletedCells;
    this.timeToPurgeDeletes = timeToPurgeDeletes;
    this.comparator = comparator;
    this.tableMaxRowSize =
      conf.getLong(HConstants.TABLE_MAX_ROWSIZE_KEY, HConstants.TABLE_MAX_ROWSIZE_DEFAULT);
    this.usePread = conf.getBoolean("hbase.storescanner.use.pread", false);
    long perHeartbeat =
      conf.getLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
        StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);
    this.cellsPerTimeoutCheck = perHeartbeat > 0?
        perHeartbeat: StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;
    this.parallelSeekEnabled =
      conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false);
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  long getTableMaxRowSize() {
    return this.tableMaxRowSize;
  }

  boolean isUsePread() {
    return this.usePread;
  }

  long getCellsPerTimeoutCheck() {
    return this.cellsPerTimeoutCheck;
  }

  boolean isParallelSeekEnabled() {
    return this.parallelSeekEnabled;
  }

  byte[] getFamily() {
    return family;
  }

  int getMinVersions() {
    return minVersions;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public long getTtl() {
    return ttl;
  }

  KeepDeletedCells getKeepDeletedCells() {
    return keepDeletedCells;
  }

  public long getTimeToPurgeDeletes() {
    return timeToPurgeDeletes;
  }

  public CellComparator getComparator() {
    return comparator;
  }
}