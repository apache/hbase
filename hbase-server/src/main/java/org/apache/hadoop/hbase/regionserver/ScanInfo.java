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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Immutable information for scans over a store.
 */
@InterfaceAudience.Private
public class ScanInfo {
  private byte[] family;
  private int minVersions;
  private int maxVersions;
  private long ttl;
  private KeepDeletedCells keepDeletedCells;
  private long timeToPurgeDeletes;
  private KVComparator comparator;

  public static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
      + (2 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_INT)
      + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_BOOLEAN);

  /**
   * @param family {@link HColumnDescriptor} describing the column family
   * @param ttl Store's TTL (in ms)
   * @param timeToPurgeDeletes duration in ms after which a delete marker can
   *        be purged during a major compaction.
   * @param comparator The store's comparator
   */
  public ScanInfo(final HColumnDescriptor family, final long ttl, final long timeToPurgeDeletes,
      final KVComparator comparator) {
    this(family.getName(), family.getMinVersions(), family.getMaxVersions(), ttl, family
        .getKeepDeletedCells(), timeToPurgeDeletes, comparator);
  }

  /**
   * @param family Name of this store's column family
   * @param minVersions Store's MIN_VERSIONS setting
   * @param maxVersions Store's VERSIONS setting
   * @param ttl Store's TTL (in ms)
   * @param timeToPurgeDeletes duration in ms after which a delete marker can
   *        be purged during a major compaction.
   * @param keepDeletedCells Store's keepDeletedCells setting
   * @param comparator The store's comparator
   */
  public ScanInfo(final byte[] family, final int minVersions, final int maxVersions,
      final long ttl, final KeepDeletedCells keepDeletedCells, final long timeToPurgeDeletes,
      final KVComparator comparator) {
    this.family = family;
    this.minVersions = minVersions;
    this.maxVersions = maxVersions;
    this.ttl = ttl;
    this.keepDeletedCells = keepDeletedCells;
    this.timeToPurgeDeletes = timeToPurgeDeletes;
    this.comparator = comparator;
  }

  public byte[] getFamily() {
    return family;
  }

  public int getMinVersions() {
    return minVersions;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public long getTtl() {
    return ttl;
  }

  public KeepDeletedCells getKeepDeletedCells() {
    return keepDeletedCells;
  }

  public long getTimeToPurgeDeletes() {
    return timeToPurgeDeletes;
  }

  public KVComparator getComparator() {
    return comparator;
  }
}
