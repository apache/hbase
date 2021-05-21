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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This class gives you the ability to change the max versions and TTL options before opening a
 * scanner for a Store. And also gives you some information for the scan.
 * <p>
 * Changing max versions, min versins, KeepDeletedCells, and TTL are usually safe even
 * for flush/compaction, so here we provide a way to do it for you. If you want to do other
 * complicated operations such as filtering, please wrap
 * the {@link InternalScanner} in the {@code preCompact} and {@code preFlush} methods in
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}.
 * <p>
 * For user scans, we also provide this class as a parameter in the {@code preStoreScannerOpen}
 * method in {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}. You can use it to change
 * the inherent properties for a Store. For example, even if you use {@code Scan.readAllVersions},
 * you still can not read two versions if the max versions property of the Store is one. You need to
 * set the max versions to a value greater than two in {@code preStoreScannerOpen}.
 * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preFlushScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext,
 *      Store, ScanOptions, FlushLifeCycleTracker)
 * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preCompactScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext,
 *      Store, ScanType, ScanOptions,
 *      org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker,
 *      org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest)
 * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preStoreScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext,
 *      Store, ScanOptions)
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface ScanOptions {

  int getMaxVersions();

  void setMaxVersions(int maxVersions);

  default void readAllVersions() {
    setMaxVersions(Integer.MAX_VALUE);
  }

  long getTTL();

  void setTTL(long ttl);

  void setKeepDeletedCells(KeepDeletedCells keepDeletedCells);

  KeepDeletedCells getKeepDeletedCells();

  int getMinVersions();

  void setMinVersions(int minVersions);

  long getTimeToPurgeDeletes();

  void setTimeToPurgeDeletes(long ttl);

  /**
   * Returns a copy of the Scan object. Modifying it will have no effect.
   */
  Scan getScan();
}
