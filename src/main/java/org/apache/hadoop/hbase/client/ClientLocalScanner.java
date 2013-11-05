/**
 * Copyright 2013 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtilities;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.RegionContext;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;


/**
 * ResultScanner used to read KeyValue pairs from similar to the ClientScanner
 * but by reading data bypassing the RegionServer. This can be used to enable
 * reading directly from the HFiles and help reduce the RegionServer's CPU
 * utilization.
 *
 * This scanner snapshots the HFiles, reads data merges the KV's and returns
 * to the user. This can be used in applications which have intensive scan
 * load.
 * Side Effects : Creates HDFS Hardlinks which will have to be deleted in
 * case the scan fails without closing properly. The application logic should
 * should have mechanism to delete the hardlinks.
 *
 * This scanner is based on HTable.ClientScanner and resembles in most parts.
 */
public class ClientLocalScanner extends ResultScannerImpl {
  private InternalScanner currentScanner;
  private Map<byte[], Store> stores =
      new ConcurrentSkipListMap<byte [], Store>(Bytes.BYTES_RAWCOMPARATOR);
  /*
   * Threadpool for doing scanner prefetches
   */
  public static final ThreadPoolExecutor scanPrefetchThreadPool;
  private static int numHandlers = 20;
  private final boolean areHardlinksCreated;
  // Initializing the numHandlers statically since the thread pool can be
  // reused across different scan operations on the same client.
  static {
    scanPrefetchThreadPool =
        Threads.getBlockingThreadPool(numHandlers, 60, TimeUnit.SECONDS,
            new DaemonThreadFactory("scan-prefetch-"));
  }

  protected ClientLocalScanner(final Scan scan, final HTable htable,
      final boolean areHardlinksCreated) {
    super(scan, htable);
    // The seek + read functionality will be used in this case since
    // scanning large files is faster using seek + read.
    Store.isPread = false;
    this.areHardlinksCreated = areHardlinksCreated;
  }

  private void flushRegionAndWaitForFlush(HRegionInfo info, HServerAddress addr)
      throws IOException
  {
    HTable table = this.htable;
    try {
      long window = table.getConfiguration().getLong(
          HConstants.CLIENT_LOCAL_SCANNER_FLUSH_ACCEPTABLE_STALENESS_MS,
          HConstants.DEFAULT_CLIENT_LOCAL_SCANNER_FLUSH_ACCEPTABLE_STALENESS_MS);
      table.flushRegionForRow(info.getStartKey(), window);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates and initializes the stores in the table.
   * @throws IOException
   */
  public void openStoresOnClient() throws IOException {
    final HRegionInfo info = this.currentRegion;
    this.currentRegion.getTableDesc().setReadOnly(true);
    HTable table = this.htable;
    final Configuration conf = table.getConfiguration();
    conf.setBoolean(HConstants.CLIENT_SIDE_SCAN, true);
    boolean flushAndWait = conf.getBoolean(
        HConstants.CLIENT_LOCAL_SCANNER_FLUSH_AND_WAIT,
        HConstants.DEFAULT_CLIENT_LOCAL_SCANNER_FLUSH_AND_WAIT);
    if (flushAndWait) {
      flushRegionAndWaitForFlush(this.currentRegion,
        table.getRegionsInfo().get(info));
    }
    Path rootDir = FSUtils.getRootDir(conf);
    final Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    final FileSystem fs = FileSystem.get(conf);

    // Load in all the applicable HStores.
    Collection<HColumnDescriptor> families =
      info.getTableDesc().getFamilies();
    families = filterFamilies(families, scan);

    HRegionUtilities.parallelStoreOpener(info, conf, families, tableDir,
        fs, this.stores, this.areHardlinksCreated);
    HConnectionManager.deleteAllZookeeperConnections();
  }

  private Collection<HColumnDescriptor> filterFamilies(
      Collection<HColumnDescriptor> families, Scan scan) {
    Set<byte[]> tmpSet = scan.getFamilyMap().keySet();
    Collection<HColumnDescriptor> ret = new HashSet<HColumnDescriptor>();
    for (HColumnDescriptor column : families) {
      if (tmpSet.contains(column.getName())) {
        ret.add(column);
      }
    }
    return ret;
  }

  /**
   * Cleans the previous scanners that was being scanned.
   */
  protected void cleanUpPreviousScanners() {
    // Close the previous scanner if it's open
    closeCurrentScanner();
  }

  /**
   * Creates and initializes the scanner for the next region.
   * @param localStartKey : The start key for the next scanner.
   * @return true is the scanners were opened properly on the new HRegion.
   * false otherwise
   * @throws IOException
   * @throws Exception
   */
  protected boolean doRealOpenScanners(byte[] localStartKey)
      throws IOException {
    try {
      this.currentRegion =
          htable.getRegionLocation(localStartKey).getRegionInfo();
      // Open a scanner on the region server starting at the
      // beginning of the region
      try {
        HRegionUtilities.checkAndAddFamilies(this.scan, this.currentRegion);
      } catch (NoSuchColumnFamilyException e) {
        throw new IOException(e);
      }
      openStoresOnClient();
      this.currentScanner = getScanner();
    } catch (IOException e) {
      close();
      throw e;
    }
    return true;
  }

  /**
   * Initializes the RegionScanner given the "stores" is initialized and
   * populated.
   * This function was taken from the HRegion code.
   * @return RegionScanner which can be polled for key values matching the
   * scan criteria.
   * @throws IOException
   */
  protected InternalScanner getScanner() throws IOException {
    RegionContext context = new RegionContext(stores, this.currentRegion);
    return new RegionScanner(this.scan, null, context,
        ClientLocalScanner.scanPrefetchThreadPool);
  }

  /**
   * Produces the next nbRows from the current scanner.
   * @param nbRows : Number of rows to return.
   * @return
   * @throws IOException
   */
  public Result[] nextOnRS(int nbRows) throws IOException {
    try {
      RegionScanner s = (RegionScanner)this.currentScanner;
      return s.nextRows(nbRows, HRegion.METRIC_NEXTSIZE);
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  /**
   * Reads the current region server for the next caching number of rows.
   * It might progress the region server in case there were no values found
   * in the current region server.
   * @throws IOException
   */
  public void cacheNextResults() throws IOException {
    Result [] values = null;
    boolean foundResults = false;
    do {
      try {
        // Server returns a null values if scanning is to stop.  Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        values = nextOnRS(caching);
      } catch (IOException e) {
        throw e;
      }
      lastNextCallTimeStamp = System.currentTimeMillis();
      if (values != null && values.length > 0) {
        foundResults = true;
        for (Result rs : values) {
          cache.add(rs);
        }
      }
    } while (!foundResults && nextScanner(this.caching, values == null));
  }

  protected void closeCurrentScanner() {
    try {
      for (Store store : stores.values()) {
        store.close(areHardlinksCreated);
      }
      stores.clear();
      if (currentScanner != null) {
        this.currentScanner.close();
      }
    } catch (IOException ioe) {
      CLIENT_LOG.error("Could not close the ClientLocalScanner properly");
    }
  }
}
