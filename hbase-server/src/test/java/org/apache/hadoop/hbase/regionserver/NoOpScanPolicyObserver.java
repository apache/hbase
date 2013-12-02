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
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TestFromClientSideWithCoprocessor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * RegionObserver that just reimplements the default behavior,
 * in order to validate that all the necessary APIs for this are public
 * This observer is also used in {@link TestFromClientSideWithCoprocessor} and
 * {@link TestCompactionWithCoprocessor} to make sure that a wide range
 * of functionality still behaves as expected.
 */
public class NoOpScanPolicyObserver extends BaseRegionObserver {
  /**
   * Reimplement the default behavior
   */
  @Override
  public InternalScanner preFlushScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    ScanInfo oldSI = store.getScanInfo();
    ScanInfo scanInfo = new ScanInfo(store.getFamily(), oldSI.getTtl(),
        oldSI.getTimeToPurgeDeletes(), oldSI.getComparator());
    Scan scan = new Scan();
    scan.setMaxVersions(oldSI.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, Collections.singletonList(memstoreScanner),
        ScanType.COMPACT_RETAIN_DELETES, store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
  }

  /**
   * Reimplement the default behavior
   */
  @Override
  public InternalScanner preCompactScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    // this demonstrates how to override the scanners default behavior
    ScanInfo oldSI = store.getScanInfo();
    ScanInfo scanInfo = new ScanInfo(store.getFamily(), oldSI.getTtl(),
        oldSI.getTimeToPurgeDeletes(), oldSI.getComparator());
    Scan scan = new Scan();
    scan.setMaxVersions(oldSI.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, scanners, scanType, 
        store.getSmallestReadPoint(), earliestPutTs);
  }

  @Override
  public KeyValueScanner preStoreScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, final Scan scan, final NavigableSet<byte[]> targetCols, KeyValueScanner s)
      throws IOException {
    HRegion r = c.getEnvironment().getRegion();
    return scan.isReversed() ? new ReversedStoreScanner(store,
        store.getScanInfo(), scan, targetCols, r.getReadpoint(scan
            .getIsolationLevel())) : new StoreScanner(store,
        store.getScanInfo(), scan, targetCols, r.getReadpoint(scan
            .getIsolationLevel()));
  }
}
