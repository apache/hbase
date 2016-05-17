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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Here we test to make sure that scans return the expected Results when the server is sending the
 * Client heartbeat messages. Heartbeat messages are essentially keep-alive messages (they prevent
 * the scanner on the client side from timing out). A heartbeat message is sent from the server to
 * the client when the server has exceeded the time limit during the processing of the scan. When
 * the time limit is reached, the server will return to the Client whatever Results it has
 * accumulated (potentially empty).
 */
@Category(MediumTests.class)
public class TestScannerHeartbeatMessages {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static TableName TABLE_NAME = TableName.valueOf("testScannerHeartbeatMessagesTable");

  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  private static int NUM_FAMILIES = 3;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 3;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 128;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);


  private static int SERVER_TIMEOUT = 6000;

  // Time, in milliseconds, that the client will wait for a response from the server before timing
  // out. This value is used server side to determine when it is necessary to send a heartbeat
  // message to the client
  private static int CLIENT_TIMEOUT = SERVER_TIMEOUT / 3;

  // By default, at most one row's worth of cells will be retrieved before the time limit is reached
  private static int DEFAULT_ROW_SLEEP_TIME = CLIENT_TIMEOUT / 5;
  // By default, at most cells for two column families are retrieved before the time limit is
  // reached
  private static int DEFAULT_CF_SLEEP_TIME = DEFAULT_ROW_SLEEP_TIME / NUM_FAMILIES;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ((Log4JLogger) ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) HeartbeatRPCServices.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.setStrings(HConstants.REGION_IMPL, HeartbeatHRegion.class.getName());
    conf.setStrings(HConstants.REGION_SERVER_IMPL, HeartbeatHRegionServer.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, SERVER_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, SERVER_TIMEOUT);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1);

    // Check the timeout condition after every cell
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 1);
    TEST_UTIL.startMiniCluster(1);

    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);
    ht.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CLIENT_TIMEOUT);
    return ht;
  }

  /**
   * Make puts to put the input value into each combination of row, family, and qualifier
   * @param rows
   * @param families
   * @param qualifiers
   * @param value
   * @return
   * @throws IOException
   */
  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    disableSleeping();
  }

  @After
  public void teardownAfterTest() throws Exception {
    disableSleeping();
  }

  /**
   * Test a variety of scan configurations to ensure that they return the expected Results when
   * heartbeat messages are necessary. These tests are accumulated under one test case to ensure
   * that they don't run in parallel. If the tests ran in parallel, they may conflict with each
   * other due to changing static variables
   */
  @Test
  public void testScannerHeartbeatMessages() throws Exception {
    testImportanceOfHeartbeats(testHeartbeatBetweenRows());
    testImportanceOfHeartbeats(testHeartbeatBetweenColumnFamilies());
    testImportanceOfHeartbeats(testHeartbeatWithSparseFilter());
  }

  /**
   * Run the test callable when heartbeats are enabled/disabled. We expect all tests to only pass
   * when heartbeat messages are enabled (otherwise the test is pointless). When heartbeats are
   * disabled, the test should throw an exception.
   * @param testCallable
   * @throws InterruptedException
   */
  public void testImportanceOfHeartbeats(Callable<Void> testCallable) throws InterruptedException {
    HeartbeatRPCServices.heartbeatsEnabled = true;

    try {
      testCallable.call();
    } catch (Exception e) {
      fail("Heartbeat messages are enabled, exceptions should NOT be thrown. Exception trace:"
          + ExceptionUtils.getStackTrace(e));
    }

    HeartbeatRPCServices.heartbeatsEnabled = false;
    try {
      testCallable.call();
    } catch (Exception e) {
      return;
    } finally {
      HeartbeatRPCServices.heartbeatsEnabled = true;
    }
    fail("Heartbeats messages are disabled, an exception should be thrown. If an exception "
        + " is not thrown, the test case is not testing the importance of heartbeat messages");
  }

  /**
   * Test the case that the time limit for the scan is reached after each full row of cells is
   * fetched.
   * @throws Exception
   */
  public Callable<Void> testHeartbeatBetweenRows() throws Exception {
    return new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        // Configure the scan so that it can read the entire table in a single RPC. We want to test
        // the case where a scan stops on the server side due to a time limit
        Scan scan = new Scan();
        scan.setMaxResultSize(Long.MAX_VALUE);
        scan.setCaching(Integer.MAX_VALUE);

        testEquivalenceOfScanWithHeartbeats(scan, DEFAULT_ROW_SLEEP_TIME, -1, false);
        return null;
      }
    };
  }

  /**
   * Test the case that the time limit for scans is reached in between column families
   * @throws Exception
   */
  public Callable<Void> testHeartbeatBetweenColumnFamilies() throws Exception {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Configure the scan so that it can read the entire table in a single RPC. We want to test
        // the case where a scan stops on the server side due to a time limit
        Scan baseScan = new Scan();
        baseScan.setMaxResultSize(Long.MAX_VALUE);
        baseScan.setCaching(Integer.MAX_VALUE);

        // Copy the scan before each test. When a scan object is used by a scanner, some of its
        // fields may be changed such as start row
        Scan scanCopy = new Scan(baseScan);
        testEquivalenceOfScanWithHeartbeats(scanCopy, -1, DEFAULT_CF_SLEEP_TIME, false);
        scanCopy = new Scan(baseScan);
        testEquivalenceOfScanWithHeartbeats(scanCopy, -1, DEFAULT_CF_SLEEP_TIME, true);
        return null;
      }
    };
  }

  public static class SparseFilter extends FilterBase{

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
      try {
        Thread.sleep(CLIENT_TIMEOUT/2 + 10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return Bytes.equals(CellUtil.cloneRow(v), ROWS[NUM_ROWS - 1]) ?
          ReturnCode.INCLUDE :
          ReturnCode.SKIP;
    }

    public static Filter parseFrom(final byte [] pbBytes){
      return new SparseFilter();
    }
  }

  /**
   * Test the case that there is a filter which filters most of cells
   * @throws Exception
   */
  public Callable<Void> testHeartbeatWithSparseFilter() throws Exception {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Scan scan = new Scan();
        scan.setMaxResultSize(Long.MAX_VALUE);
        scan.setCaching(Integer.MAX_VALUE);
        scan.setFilter(new SparseFilter());
        ResultScanner scanner = TABLE.getScanner(scan);
        int num = 0;
        while (scanner.next() != null) {
          num++;
        }
        assertEquals(1, num);
        scanner.close();

        scan = new Scan();
        scan.setMaxResultSize(Long.MAX_VALUE);
        scan.setCaching(Integer.MAX_VALUE);
        scan.setFilter(new SparseFilter());
        scan.setAllowPartialResults(true);
        scanner = TABLE.getScanner(scan);
        num = 0;
        while (scanner.next() != null) {
          num++;
        }
        assertEquals(NUM_FAMILIES * NUM_QUALIFIERS, num);
        scanner.close();

        return null;
      }
    };
  }

  /**
   * Test the equivalence of a scan versus the same scan executed when heartbeat messages are
   * necessary
   * @param scan The scan configuration being tested
   * @param rowSleepTime The time to sleep between fetches of row cells
   * @param cfSleepTime The time to sleep between fetches of column family cells
   * @param sleepBeforeCf set to true when column family sleeps should occur before the cells for
   *          that column family are fetched
   * @throws Exception
   */
  public void testEquivalenceOfScanWithHeartbeats(final Scan scan, int rowSleepTime,
      int cfSleepTime, boolean sleepBeforeCf) throws Exception {
    disableSleeping();
    final ResultScanner scanner = TABLE.getScanner(scan);
    final ResultScanner scannerWithHeartbeats = TABLE.getScanner(scan);

    Result r1 = null;
    Result r2 = null;

    while ((r1 = scanner.next()) != null) {
      // Enforce the specified sleep conditions during calls to the heartbeat scanner
      configureSleepTime(rowSleepTime, cfSleepTime, sleepBeforeCf);
      r2 = scannerWithHeartbeats.next();
      disableSleeping();

      assertTrue(r2 != null);
      try {
        Result.compareResults(r1, r2);
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }

    assertTrue(scannerWithHeartbeats.next() == null);
    scanner.close();
    scannerWithHeartbeats.close();
  }

  /**
   * Helper method for setting the time to sleep between rows and column families. If a sleep time
   * is negative then that sleep will be disabled
   * @param rowSleepTime
   * @param cfSleepTime
   */
  private static void configureSleepTime(int rowSleepTime, int cfSleepTime, boolean sleepBeforeCf) {
    HeartbeatHRegion.sleepBetweenRows = rowSleepTime > 0;
    HeartbeatHRegion.rowSleepTime = rowSleepTime;

    HeartbeatHRegion.sleepBetweenColumnFamilies = cfSleepTime > 0;
    HeartbeatHRegion.columnFamilySleepTime = cfSleepTime;
    HeartbeatHRegion.sleepBeforeColumnFamily = sleepBeforeCf;
  }

  /**
   * Disable the sleeping mechanism server side.
   */
  private static void disableSleeping() {
    HeartbeatHRegion.sleepBetweenRows = false;
    HeartbeatHRegion.sleepBetweenColumnFamilies = false;
  }

  /**
   * Custom HRegionServer instance that instantiates {@link HeartbeatRPCServices} in place of
   * {@link RSRpcServices} to allow us to toggle support for heartbeat messages
   */
  private static class HeartbeatHRegionServer extends HRegionServer {
    public HeartbeatHRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    public HeartbeatHRegionServer(Configuration conf, CoordinatedStateManager csm)
        throws IOException {
      super(conf, csm);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new HeartbeatRPCServices(this);
    }
  }

  /**
   * Custom RSRpcServices instance that allows heartbeat support to be toggled
   */
  private static class HeartbeatRPCServices extends RSRpcServices {
    private static boolean heartbeatsEnabled = true;

    public HeartbeatRPCServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ScanResponse scan(RpcController controller, ScanRequest request) 
        throws ServiceException {
      ScanRequest.Builder builder = ScanRequest.newBuilder(request);
      builder.setClientHandlesHeartbeats(heartbeatsEnabled);
      return super.scan(controller, builder.build());
    }
  }

  /**
   * Custom HRegion class that instantiates {@link RegionScanner}s with configurable sleep times
   * between fetches of row Results and/or column family cells. Useful for emulating an instance
   * where the server is taking a long time to process a client's scan request
   */
  private static class HeartbeatHRegion extends HRegion {
    // Row sleeps occur AFTER each row worth of cells is retrieved.
    private static int rowSleepTime = DEFAULT_ROW_SLEEP_TIME;
    private static boolean sleepBetweenRows = false;

    // The sleep for column families can be initiated before or after we fetch the cells for the
    // column family. If the sleep occurs BEFORE then the time limits will be reached inside
    // StoreScanner while we are fetching individual cells. If the sleep occurs AFTER then the time
    // limit will be reached inside RegionScanner after all the cells for a column family have been
    // retrieved.
    private static boolean sleepBeforeColumnFamily = false;
    private static int columnFamilySleepTime = DEFAULT_CF_SLEEP_TIME;
    private static boolean sleepBetweenColumnFamilies = false;

    public HeartbeatHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
        HRegionInfo regionInfo, HTableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public HeartbeatHRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
        HTableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    private static void columnFamilySleep() {
      if (HeartbeatHRegion.sleepBetweenColumnFamilies) {
        try {
          Thread.sleep(HeartbeatHRegion.columnFamilySleepTime);
        } catch (InterruptedException e) {
        }
      }
    }

    private static void rowSleep() {
      try {
        if (HeartbeatHRegion.sleepBetweenRows) {
          Thread.sleep(HeartbeatHRegion.rowSleepTime);
        }
      } catch (InterruptedException e) {
      }
    }

    // Instantiate the custom heartbeat region scanners
    @Override
    protected RegionScanner instantiateRegionScanner(Scan scan,
        List<KeyValueScanner> additionalScanners) throws IOException {
      if (scan.isReversed()) {
        if (scan.getFilter() != null) {
          scan.getFilter().setReversed(true);
        }
        return new HeartbeatReversedRegionScanner(scan, additionalScanners, this);
      }
      return new HeartbeatRegionScanner(scan, additionalScanners, this);
    }
  }

  /**
   * Custom ReversedRegionScanner that can be configured to sleep between retrievals of row Results
   * and/or column family cells
   */
  private static class HeartbeatReversedRegionScanner extends ReversedRegionScannerImpl {
    HeartbeatReversedRegionScanner(Scan scan, List<KeyValueScanner> additionalScanners,
        HRegion region) throws IOException {
      super(scan, additionalScanners, region);
    }

    @Override
    public boolean nextRaw(List<Cell> outResults, ScannerContext context)
        throws IOException {
      boolean moreRows = super.nextRaw(outResults, context);
      HeartbeatHRegion.rowSleep();
      return moreRows;
    }

    @Override
    protected void initializeKVHeap(List<KeyValueScanner> scanners,
        List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
      this.storeHeap = new HeartbeatReversedKVHeap(scanners, region.getCellCompartor());
      if (!joinedScanners.isEmpty()) {
        this.joinedHeap = new HeartbeatReversedKVHeap(joinedScanners, region.getCellCompartor());
      }
    }
  }

  /**
   * Custom RegionScanner that can be configured to sleep between retrievals of row Results and/or
   * column family cells
   */
  private static class HeartbeatRegionScanner extends RegionScannerImpl {
    HeartbeatRegionScanner(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region)
        throws IOException {
      region.super(scan, additionalScanners, region);
    }

    @Override
    public boolean nextRaw(List<Cell> outResults, ScannerContext context)
        throws IOException {
      boolean moreRows = super.nextRaw(outResults, context);
      HeartbeatHRegion.rowSleep();
      return moreRows;
    }

    @Override
    protected void initializeKVHeap(List<KeyValueScanner> scanners,
        List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
      this.storeHeap = new HeartbeatKVHeap(scanners, region.getCellCompartor());
      if (!joinedScanners.isEmpty()) {
        this.joinedHeap = new HeartbeatKVHeap(joinedScanners, region.getCellCompartor());
      }
    }
  }

  /**
   * Custom KV Heap that can be configured to sleep/wait in between retrievals of column family
   * cells. Useful for testing
   */
  private static final class HeartbeatKVHeap extends KeyValueHeap {
    public HeartbeatKVHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
        throws IOException {
      super(scanners, comparator);
    }

    HeartbeatKVHeap(List<? extends KeyValueScanner> scanners, KVScannerComparator comparator)
        throws IOException {
      super(scanners, comparator);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext context)
        throws IOException {
      if (HeartbeatHRegion.sleepBeforeColumnFamily) HeartbeatHRegion.columnFamilySleep();
      boolean moreRows = super.next(result, context);
      if (!HeartbeatHRegion.sleepBeforeColumnFamily) HeartbeatHRegion.columnFamilySleep();
      return moreRows;
    }
  }

  /**
   * Custom reversed KV Heap that can be configured to sleep in between retrievals of column family
   * cells.
   */
  private static final class HeartbeatReversedKVHeap extends ReversedKeyValueHeap {
    public HeartbeatReversedKVHeap(List<? extends KeyValueScanner> scanners, 
        CellComparator comparator) throws IOException {
      super(scanners, comparator);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext context)
        throws IOException {
      if (HeartbeatHRegion.sleepBeforeColumnFamily) HeartbeatHRegion.columnFamilySleep();
      boolean moreRows = super.next(result, context);
      if (!HeartbeatHRegion.sleepBeforeColumnFamily) HeartbeatHRegion.columnFamilySleep();
      return moreRows;
    }
  }
}
