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
    Store.ScanInfo oldSI = store.getScanInfo();
    Store.ScanInfo scanInfo = new Store.ScanInfo(store.getFamily(), oldSI.getTtl(),
        oldSI.getTimeToPurgeDeletes(), oldSI.getComparator());
    Scan scan = new Scan();
    scan.setMaxVersions(oldSI.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, Collections.singletonList(memstoreScanner),
        ScanType.MINOR_COMPACT, store.getHRegion().getSmallestReadPoint(),
        HConstants.OLDEST_TIMESTAMP);
  }

  /**
   * Reimplement the default behavior
   */
  @Override
  public InternalScanner preCompactScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    // this demonstrates how to override the scanners default behavior
    Store.ScanInfo oldSI = store.getScanInfo();
    Store.ScanInfo scanInfo = new Store.ScanInfo(store.getFamily(), oldSI.getTtl(),
        oldSI.getTimeToPurgeDeletes(), oldSI.getComparator());
    Scan scan = new Scan();
    scan.setMaxVersions(oldSI.getMaxVersions());
    return new StoreScanner(store, scanInfo, scan, scanners, scanType, store.getHRegion()
        .getSmallestReadPoint(), earliestPutTs);
  }

  @Override
  public KeyValueScanner preStoreScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, final Scan scan, final NavigableSet<byte[]> targetCols, KeyValueScanner s)
      throws IOException {
    return new StoreScanner(store, store.getScanInfo(), scan, targetCols);
  }
}
