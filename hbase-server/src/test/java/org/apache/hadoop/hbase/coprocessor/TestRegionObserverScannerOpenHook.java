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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestRegionObserverScannerOpenHook {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionObserverScannerOpenHook.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  static final Path DIR = UTIL.getDataTestDir();

  @Rule
  public TestName name = new TestName();

  public static class NoDataFilter extends FilterBase {

    @Override
    public ReturnCode filterCell(final Cell ignored) {
      return ReturnCode.SKIP;
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
      return true;
    }

    @Override
    public boolean filterRow() throws IOException {
      return true;
    }
  }

  /**
   * Do the default logic in {@link RegionObserver} interface.
   */
  public static class EmptyRegionObsever implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }

  /**
   * Don't return any data from a scan by creating a custom {@link StoreScanner}.
   */
  public static class NoDataFromScan implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
        List<Cell> result) throws IOException {
      c.bypass();
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
        throws IOException {
      scan.setFilter(new NoDataFilter());
    }
  }

  private static final InternalScanner NO_DATA = new InternalScanner() {

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
      return false;
    }

    @Override
    public void close() throws IOException {}
  };
  /**
   * Don't allow any data in a flush by creating a custom {@link StoreScanner}.
   */
  public static class NoDataFromFlush implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
      return NO_DATA;
    }
  }

  /**
   * Don't allow any data to be written out in the compaction by creating a custom
   * {@link StoreScanner}.
   */
  public static class NoDataFromCompaction implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
      return NO_DATA;
    }
  }

  HRegion initHRegion(byte[] tableName, String callingMethod, Configuration conf,
      byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    Path path = new Path(DIR + callingMethod);
    WAL wal = HBaseTestingUtility.createWal(conf, path, info);
    HRegion r = HRegion.createHRegion(info, path, conf, htd, wal);
    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    RegionCoprocessorHost host = new RegionCoprocessorHost(r, null, conf);
    r.setCoprocessorHost(host);
    return r;
  }

  @Test
  public void testRegionObserverScanTimeStacking() throws Exception {
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] TABLE = Bytes.toBytes(getClass().getName());
    byte[] A = Bytes.toBytes("A");
    byte[][] FAMILIES = new byte[][] { A };

    // Use new HTU to not overlap with the DFS cluster started in #CompactionStacking
    Configuration conf = new HBaseTestingUtility().getConfiguration();
    HRegion region = initHRegion(TABLE, getClass().getName(), conf, FAMILIES);
    RegionCoprocessorHost h = region.getCoprocessorHost();
    h.load(NoDataFromScan.class, Coprocessor.PRIORITY_HIGHEST, conf);
    h.load(EmptyRegionObsever.class, Coprocessor.PRIORITY_USER, conf);

    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    region.put(put);

    Get get = new Get(ROW);
    Result r = region.get(get);
    assertNull(
      "Got an unexpected number of rows - "
        + "no data should be returned with the NoDataFromScan coprocessor. Found: " + r,
      r.listCells());
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testRegionObserverFlushTimeStacking() throws Exception {
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] TABLE = Bytes.toBytes(getClass().getName());
    byte[] A = Bytes.toBytes("A");
    byte[][] FAMILIES = new byte[][] { A };

    // Use new HTU to not overlap with the DFS cluster started in #CompactionStacking
    Configuration conf = new HBaseTestingUtility().getConfiguration();
    HRegion region = initHRegion(TABLE, getClass().getName(), conf, FAMILIES);
    RegionCoprocessorHost h = region.getCoprocessorHost();
    h.load(NoDataFromFlush.class, Coprocessor.PRIORITY_HIGHEST, conf);
    h.load(EmptyRegionObsever.class, Coprocessor.PRIORITY_USER, conf);

    // put a row and flush it to disk
    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    region.put(put);
    region.flush(true);
    Get get = new Get(ROW);
    Result r = region.get(get);
    assertNull(
      "Got an unexpected number of rows - "
        + "no data should be returned with the NoDataFromScan coprocessor. Found: " + r,
      r.listCells());
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  /*
   * Custom HRegion which uses CountDownLatch to signal the completion of compaction
   */
  public static class CompactionCompletionNotifyingRegion extends HRegion {
    private static volatile CountDownLatch compactionStateChangeLatch = null;

    @SuppressWarnings("deprecation")
    public CompactionCompletionNotifyingRegion(Path tableDir, WAL log,
        FileSystem fs, Configuration confParam, RegionInfo info,
        TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }

    public CountDownLatch getCompactionStateChangeLatch() {
      if (compactionStateChangeLatch == null) {
        compactionStateChangeLatch = new CountDownLatch(1);
      }
      return compactionStateChangeLatch;
    }

    @Override
    public boolean compact(CompactionContext compaction, HStore store,
      ThroughputController throughputController) throws IOException {
      boolean ret = super.compact(compaction, store, throughputController);
      if (ret) {
        compactionStateChangeLatch.countDown();
      }
      return ret;
    }

    @Override
    public boolean compact(CompactionContext compaction, HStore store,
        ThroughputController throughputController, User user) throws IOException {
      boolean ret = super.compact(compaction, store, throughputController, user);
      if (ret) compactionStateChangeLatch.countDown();
      return ret;
    }
  }

  /**
   * Unfortunately, the easiest way to test this is to spin up a mini-cluster since we want to do
   * the usual compaction mechanism on the region, rather than going through the backdoor to the
   * region
   */
  @Test
  public void testRegionObserverCompactionTimeStacking() throws Exception {
    // setup a mini cluster so we can do a real compaction on a region
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(HConstants.REGION_IMPL, CompactionCompletionNotifyingRegion.class, HRegion.class);
    conf.setInt("hbase.hstore.compaction.min", 2);
    UTIL.startMiniCluster();
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] A = Bytes.toBytes("A");
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    desc.addFamily(new HColumnDescriptor(A));
    desc.addCoprocessor(EmptyRegionObsever.class.getName(), null, Coprocessor.PRIORITY_USER, null);
    desc.addCoprocessor(NoDataFromCompaction.class.getName(), null, Coprocessor.PRIORITY_HIGHEST,
      null);

    Admin admin = UTIL.getAdmin();
    admin.createTable(desc);

    Table table = UTIL.getConnection().getTable(desc.getTableName());

    // put a row and flush it to disk
    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    table.put(put);

    HRegionServer rs = UTIL.getRSForFirstRegionInTable(desc.getTableName());
    List<HRegion> regions = rs.getRegions(desc.getTableName());
    assertEquals("More than 1 region serving test table with 1 row", 1, regions.size());
    Region region = regions.get(0);
    admin.flushRegion(region.getRegionInfo().getRegionName());
    CountDownLatch latch = ((CompactionCompletionNotifyingRegion)region)
        .getCompactionStateChangeLatch();

    // put another row and flush that too
    put = new Put(Bytes.toBytes("anotherrow"));
    put.addColumn(A, A, A);
    table.put(put);
    admin.flushRegion(region.getRegionInfo().getRegionName());

    // run a compaction, which normally would should get rid of the data
    // wait for the compaction checker to complete
    latch.await();
    // check both rows to ensure that they aren't there
    Get get = new Get(ROW);
    Result r = table.get(get);
    assertNull(
      "Got an unexpected number of rows - "
        + "no data should be returned with the NoDataFromScan coprocessor. Found: " + r,
      r.listCells());

    get = new Get(Bytes.toBytes("anotherrow"));
    r = table.get(get);
    assertNull(
      "Got an unexpected number of rows - "
        + "no data should be returned with the NoDataFromScan coprocessor Found: " + r,
      r.listCells());

    table.close();
    UTIL.shutdownMiniCluster();
  }
}
