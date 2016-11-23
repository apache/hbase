/**
 * Copyright The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class, ClientTests.class})
public class TestFromClientSide3 {
  private static final Log LOG = LogFactory.getLog(TestFromClientSide3.class);
  private final static HBaseTestingUtility TEST_UTIL
    = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static Random random = new Random();
  private static int SLAVES = 3;
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static final byte[] ANOTHERROW = Bytes.toBytes("anotherrow");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private final static byte[] COL_QUAL = Bytes.toBytes("f1");
  private final static byte[] VAL_BYTES = Bytes.toBytes("v1");
  private final static byte[] ROW_BYTES = Bytes.toBytes("r1");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd: TEST_UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TEST_UTIL.deleteTable(htd.getTableName());
  }
  }

  private void randomCFPuts(Table table, byte[] row, byte[] family, int nPuts)
      throws Exception {
    Put put = new Put(row);
    for (int i = 0; i < nPuts; i++) {
      byte[] qualifier = Bytes.toBytes(random.nextInt());
      byte[] value = Bytes.toBytes(random.nextInt());
      put.addColumn(family, qualifier, value);
    }
    table.put(put);
  }

  private void performMultiplePutAndFlush(HBaseAdmin admin, Table table,
      byte[] row, byte[] family, int nFlushes, int nPuts)
  throws Exception {

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(table.getName())) {
      // connection needed for poll-wait
      HRegionLocation loc = locator.getRegionLocation(row, true);
      AdminProtos.AdminService.BlockingInterface server =
        ((ClusterConnection) admin.getConnection()).getAdmin(loc.getServerName());
      byte[] regName = loc.getRegionInfo().getRegionName();

      for (int i = 0; i < nFlushes; i++) {
        randomCFPuts(table, row, family, nPuts);
        List<String> sf = ProtobufUtil.getStoreFiles(server, regName, FAMILY);
        int sfCount = sf.size();

        admin.flush(table.getName());
      }
    }
  }

  // override the config settings at the CF level and ensure priority
  @Test(timeout = 60000)
  public void testAdvancedConfigOverride() throws Exception {
    /*
     * Overall idea: (1) create 3 store files and issue a compaction. config's
     * compaction.min == 3, so should work. (2) Increase the compaction.min
     * toggle in the HTD to 5 and modify table. If we use the HTD value instead
     * of the default config value, adding 3 files and issuing a compaction
     * SHOULD NOT work (3) Decrease the compaction.min toggle in the HCD to 2
     * and modify table. The CF schema should override the Table schema and now
     * cause a minor compaction.
     */
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.min", 3);

    TableName tableName = TableName.valueOf("testAdvancedConfigOverride");
    Table hTable = TEST_UTIL.createTable(tableName, FAMILY, 10);
    Admin admin = TEST_UTIL.getHBaseAdmin();
    ClusterConnection connection = (ClusterConnection) TEST_UTIL.getConnection();

    // Create 3 store files.
    byte[] row = Bytes.toBytes(random.nextInt());
    performMultiplePutAndFlush((HBaseAdmin) admin, hTable, row, FAMILY, 3, 100);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      // Verify we have multiple store files.
      HRegionLocation loc = locator.getRegionLocation(row, true);
      byte[] regionName = loc.getRegionInfo().getRegionName();
      AdminProtos.AdminService.BlockingInterface server = connection.getAdmin(loc.getServerName());
      assertTrue(ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() > 1);

      // Issue a compaction request
      admin.compact(tableName);

      // poll wait for the compactions to happen
      for (int i = 0; i < 10 * 1000 / 40; ++i) {
        // The number of store files after compaction should be lesser.
        loc = locator.getRegionLocation(row, true);
        if (!loc.getRegionInfo().isOffline()) {
          regionName = loc.getRegionInfo().getRegionName();
          server = connection.getAdmin(loc.getServerName());
          if (ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() <= 1) {
            break;
          }
        }
        Thread.sleep(40);
      }
      // verify the compactions took place and that we didn't just time out
      assertTrue(ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() <= 1);

      // change the compaction.min config option for this table to 5
      LOG.info("hbase.hstore.compaction.min should now be 5");
      HTableDescriptor htd = new HTableDescriptor(hTable.getTableDescriptor());
      htd.setValue("hbase.hstore.compaction.min", String.valueOf(5));
      admin.modifyTable(tableName, htd);
      Pair<Integer, Integer> st;
      while (null != (st = admin.getAlterStatus(tableName)) && st.getFirst() > 0) {
        LOG.debug(st.getFirst() + " regions left to update");
        Thread.sleep(40);
      }
      LOG.info("alter status finished");

      // Create 3 more store files.
      performMultiplePutAndFlush((HBaseAdmin) admin, hTable, row, FAMILY, 3, 10);

      // Issue a compaction request
      admin.compact(tableName);

      // This time, the compaction request should not happen
      Thread.sleep(10 * 1000);
      loc = locator.getRegionLocation(row, true);
      regionName = loc.getRegionInfo().getRegionName();
      server = connection.getAdmin(loc.getServerName());
      int sfCount = ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size();
      assertTrue(sfCount > 1);

      // change an individual CF's config option to 2 & online schema update
      LOG.info("hbase.hstore.compaction.min should now be 2");
      HColumnDescriptor hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
      hcd.setValue("hbase.hstore.compaction.min", String.valueOf(2));
      htd.modifyFamily(hcd);
      admin.modifyTable(tableName, htd);
      while (null != (st = admin.getAlterStatus(tableName)) && st.getFirst() > 0) {
        LOG.debug(st.getFirst() + " regions left to update");
        Thread.sleep(40);
      }
      LOG.info("alter status finished");

      // Issue a compaction request
      admin.compact(tableName);

      // poll wait for the compactions to happen
      for (int i = 0; i < 10 * 1000 / 40; ++i) {
        loc = locator.getRegionLocation(row, true);
        regionName = loc.getRegionInfo().getRegionName();
        try {
          server = connection.getAdmin(loc.getServerName());
          if (ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() < sfCount) {
            break;
          }
        } catch (Exception e) {
          LOG.debug("Waiting for region to come online: " + regionName);
        }
        Thread.sleep(40);
      }

      // verify the compaction took place and that we didn't just time out
      assertTrue(ProtobufUtil.getStoreFiles(
        server, regionName, FAMILY).size() < sfCount);

      // Finally, ensure that we can remove a custom config value after we made it
      LOG.info("Removing CF config value");
      LOG.info("hbase.hstore.compaction.min should now be 5");
      hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
      hcd.setValue("hbase.hstore.compaction.min", null);
      htd.modifyFamily(hcd);
      admin.modifyTable(tableName, htd);
      while (null != (st = admin.getAlterStatus(tableName)) && st.getFirst() > 0) {
        LOG.debug(st.getFirst() + " regions left to update");
        Thread.sleep(40);
      }
      LOG.info("alter status finished");
      assertNull(hTable.getTableDescriptor().getFamily(FAMILY).getValue(
          "hbase.hstore.compaction.min"));
    }
  }

  @Test
  public void testHTableBatchWithEmptyPut ()throws Exception {
      Table table = TEST_UTIL.createTable(TableName.valueOf("testHTableBatchWithEmptyPut"),
          new byte[][] { FAMILY });
    try {
      List actions = (List) new ArrayList();
      Object[] results = new Object[2];
      // create an empty Put
      Put put1 = new Put(ROW);
      actions.add(put1);

      Put put2 = new Put(ANOTHERROW);
      put2.addColumn(FAMILY, QUALIFIER, VALUE);
      actions.add(put2);

      table.batch(actions, results);
      fail("Empty Put should have failed the batch call");
    } catch (IllegalArgumentException iae) {

    } finally {
      table.close();
    }
  }

  @Test
  public void testHTableExistsMethodSingleRegionSingleGet() throws Exception {
      // Test with a single region table.
      Table table = TEST_UTIL.createTable(
          TableName.valueOf("testHTableExistsMethodSingleRegionSingleGet"),
          new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertEquals(exist, false);

    table.put(put);

    exist = table.exists(get);
    assertEquals(exist, true);
  }

  public void testHTableExistsMethodSingleRegionMultipleGets() throws Exception {
    Table table = TEST_UTIL.createTable(TableName.valueOf(
        "testHTableExistsMethodSingleRegionMultipleGets"), new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    List<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ROW));
    gets.add(null);
    gets.add(new Get(ANOTHERROW));

    boolean[] results = table.existsAll(gets);
    assertEquals(results[0], true);
    assertEquals(results[1], false);
    assertEquals(results[2], false);
  }

  @Test
  public void testHTableExistsBeforeGet() throws Exception {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testHTableExistsBeforeGet"),
        new byte[][] { FAMILY });
    try {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      Get get = new Get(ROW);

      boolean exist = table.exists(get);
      assertEquals(true, exist);

      Result result = table.get(get);
      assertEquals(false, result.isEmpty());
      assertTrue(Bytes.equals(VALUE, result.getValue(FAMILY, QUALIFIER)));
    } finally {
      table.close();
    }
  }

  @Test
  public void testHTableExistsAllBeforeGet() throws Exception {
    final byte[] ROW2 = Bytes.add(ROW, Bytes.toBytes("2"));
    Table table = TEST_UTIL.createTable(
        TableName.valueOf("testHTableExistsAllBeforeGet"), new byte[][] { FAMILY });
    try {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      put = new Put(ROW2);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      Get get = new Get(ROW);
      Get get2 = new Get(ROW2);
      ArrayList<Get> getList = new ArrayList(2);
      getList.add(get);
      getList.add(get2);

      boolean[] exists = table.existsAll(getList);
      assertEquals(true, exists[0]);
      assertEquals(true, exists[1]);

      Result[] result = table.get(getList);
      assertEquals(false, result[0].isEmpty());
      assertTrue(Bytes.equals(VALUE, result[0].getValue(FAMILY, QUALIFIER)));
      assertEquals(false, result[1].isEmpty());
      assertTrue(Bytes.equals(VALUE, result[1].getValue(FAMILY, QUALIFIER)));
    } finally {
      table.close();
    }
  }

  @Test
  public void testHTableExistsMethodMultipleRegionsSingleGet() throws Exception {
    Table table = TEST_UTIL.createTable(
      TableName.valueOf("testHTableExistsMethodMultipleRegionsSingleGet"), new byte[][] { FAMILY },
      1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertEquals(exist, false);

    table.put(put);

    exist = table.exists(get);
    assertEquals(exist, true);
  }

  @Test
  public void testHTableExistsMethodMultipleRegionsMultipleGets() throws Exception {
    Table table = TEST_UTIL.createTable(
      TableName.valueOf("testHTableExistsMethodMultipleRegionsMultipleGets"),
      new byte[][] { FAMILY }, 1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put (put);

    List<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ANOTHERROW));
    gets.add(new Get(Bytes.add(ROW, new byte[] { 0x00 })));
    gets.add(new Get(ROW));
    gets.add(new Get(Bytes.add(ANOTHERROW, new byte[] { 0x00 })));

    LOG.info("Calling exists");
    boolean[] results = table.existsAll(gets);
    assertEquals(results[0], false);
    assertEquals(results[1], false);
    assertEquals(results[2], true);
    assertEquals(results[3], false);

    // Test with the first region.
    put = new Put(new byte[] { 0x00 });
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<Get>();
    gets.add(new Get(new byte[] { 0x00 }));
    gets.add(new Get(new byte[] { 0x00, 0x00 }));
    results = table.existsAll(gets);
    assertEquals(results[0], true);
    assertEquals(results[1], false);

    // Test with the last region
    put = new Put(new byte[] { (byte) 0xff, (byte) 0xff });
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<Get>();
    gets.add(new Get(new byte[] { (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }));
    results = table.existsAll(gets);
    assertEquals(results[0], false);
    assertEquals(results[1], true);
    assertEquals(results[2], false);
  }

  @Test
  public void testGetEmptyRow() throws Exception {
    //Create a table and put in 1 row
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("test")));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());

    Put put = new Put(ROW_BYTES);
    put.addColumn(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(put);

    //Try getting the row with an empty row key
    Result res = null;
    try {
      res = table.get(new Get(new byte[0]));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected.
    }
    assertTrue(res == null);
    res = table.get(new Get(Bytes.toBytes("r1-not-exist")));
    assertTrue(res.isEmpty() == true);
    res = table.get(new Get(ROW_BYTES));
    assertTrue(Arrays.equals(res.getValue(FAMILY, COL_QUAL), VAL_BYTES));
    table.close();
  }

  @Test
  public void testConnectionDefaultUsesCodec() throws Exception {
    ClusterConnection con = (ClusterConnection) TEST_UTIL.getConnection();
    assertTrue(con.hasCellBlockSupport());
  }

  @Test(timeout = 60000)
  public void testPutWithPreBatchMutate() throws Exception {
    TableName tableName = TableName.valueOf("testPutWithPreBatchMutate");
    testPreBatchMutate(tableName, () -> {
      try {
        Table t = TEST_UTIL.getConnection().getTable(tableName);
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        t.put(put);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  @Test(timeout = 60000)
  public void testRowMutationsWithPreBatchMutate() throws Exception {
    TableName tableName = TableName.valueOf("testRowMutationsWithPreBatchMutate");
    testPreBatchMutate(tableName, () -> {
      try {
        RowMutations rm = new RowMutations(ROW, 1);
        Table t = TEST_UTIL.getConnection().getTable(tableName);
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        rm.add(put);
        t.mutateRow(rm);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  private void testPreBatchMutate(TableName tableName, Runnable rn)throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(WatiingForScanObserver.class.getName());
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getAdmin().createTable(desc);
    ExecutorService service = Executors.newFixedThreadPool(2);
    service.execute(rn);
    final List<Cell> cells = new ArrayList<>();
    service.execute(() -> {
      try {
        // waiting for update.
        TimeUnit.SECONDS.sleep(3);
        Table t = TEST_UTIL.getConnection().getTable(tableName);
        Scan scan = new Scan();
        try (ResultScanner scanner = t.getScanner(scan)) {
          for (Result r : scanner) {
            cells.addAll(Arrays.asList(r.rawCells()));
          }
        }
      } catch (IOException | InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    });
    service.shutdown();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    assertEquals("The write is blocking by RegionObserver#postBatchMutate"
      + ", so the data is invisible to reader", 0, cells.size());
    TEST_UTIL.deleteTable(tableName);
  }

  @Test(timeout = 30000)
  public void testLockLeakWithDelta() throws Exception, Throwable {
    TableName tableName = TableName.valueOf("testLockLeakWithDelta");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(WatiingForMultiMutationsObserver.class.getName());
    desc.setConfiguration("hbase.rowlock.wait.duration", String.valueOf(5000));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getAdmin().createTable(desc);
    // new a connection for lower retry number.
    Configuration copy = new Configuration(TEST_UTIL.getConfiguration());
    copy.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection con = ConnectionFactory.createConnection(copy)) {
      HRegion region = (HRegion) find(tableName);
      region.setTimeoutForWriteLock(10);
      ExecutorService putService = Executors.newSingleThreadExecutor();
      putService.execute(() -> {
        try (Table table = con.getTable(tableName)) {
          Put put = new Put(ROW);
          put.addColumn(FAMILY, QUALIFIER, VALUE);
          // the put will be blocked by WatiingForMultiMutationsObserver.
          table.put(put);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService appendService = Executors.newSingleThreadExecutor();
      appendService.execute(() -> {
        Append append = new Append(ROW);
        append.add(FAMILY, QUALIFIER, VALUE);
        try (Table table = con.getTable(tableName)) {
          table.append(append);
          fail("The APPEND should fail because the target lock is blocked by previous put");
        } catch (Throwable ex) {
        }
      });
      appendService.shutdown();
      appendService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WatiingForMultiMutationsObserver observer = find(tableName, WatiingForMultiMutationsObserver.class);
      observer.latch.countDown();
      putService.shutdown();
      putService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      try (Table table = con.getTable(tableName)) {
        Result r = table.get(new Get(ROW));
        assertFalse(r.isEmpty());
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), VALUE));
      }
    }
    HRegion region = (HRegion) find(tableName);
    int readLockCount = region.getReadLockCount();
    LOG.info("readLockCount:" + readLockCount);
    assertEquals(0, readLockCount);
  }

  @Test(timeout = 30000)
  public void testMultiRowMutations() throws Exception, Throwable {
    TableName tableName = TableName.valueOf("testMultiRowMutations");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(MultiRowMutationEndpoint.class.getName());
    desc.addCoprocessor(WatiingForMultiMutationsObserver.class.getName());
    desc.setConfiguration("hbase.rowlock.wait.duration", String.valueOf(5000));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getAdmin().createTable(desc);
    // new a connection for lower retry number.
    Configuration copy = new Configuration(TEST_UTIL.getConfiguration());
    copy.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection con = ConnectionFactory.createConnection(copy)) {
      byte[] row = Bytes.toBytes("ROW-0");
      byte[] rowLocked= Bytes.toBytes("ROW-1");
      byte[] value0 = Bytes.toBytes("VALUE-0");
      byte[] value1 = Bytes.toBytes("VALUE-1");
      byte[] value2 = Bytes.toBytes("VALUE-2");
      assertNoLocks(tableName);
      ExecutorService putService = Executors.newSingleThreadExecutor();
      putService.execute(() -> {
        try (Table table = con.getTable(tableName)) {
          Put put0 = new Put(rowLocked);
          put0.addColumn(FAMILY, QUALIFIER, value0);
          // the put will be blocked by WatiingForMultiMutationsObserver.
          table.put(put0);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService cpService = Executors.newSingleThreadExecutor();
      cpService.execute(() -> {
        Put put1 = new Put(row);
        Put put2 = new Put(rowLocked);
        put1.addColumn(FAMILY, QUALIFIER, value1);
        put2.addColumn(FAMILY, QUALIFIER, value2);
        try (Table table = con.getTable(tableName)) {
          MultiRowMutationProtos.MutateRowsRequest request
            = MultiRowMutationProtos.MutateRowsRequest.newBuilder()
              .addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
                      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT, put1))
              .addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
                      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT, put2))
              .build();
          table.coprocessorService(MultiRowMutationProtos.MultiRowMutationService.class,
            ROW, ROW,
            (MultiRowMutationProtos.MultiRowMutationService exe) -> {
              ServerRpcController controller = new ServerRpcController();
              CoprocessorRpcUtils.BlockingRpcCallback<MultiRowMutationProtos.MutateRowsResponse>
                rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
              exe.mutateRows(controller, request, rpcCallback);
              return rpcCallback.get();
            });
          fail("This cp should fail because the target lock is blocked by previous put");
        } catch (Throwable ex) {
        }
      });
      cpService.shutdown();
      cpService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WatiingForMultiMutationsObserver observer = find(tableName, WatiingForMultiMutationsObserver.class);
      observer.latch.countDown();
      putService.shutdown();
      putService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      try (Table table = con.getTable(tableName)) {
        Get g0 = new Get(row);
        Get g1 = new Get(rowLocked);
        Result r0 = table.get(g0);
        Result r1 = table.get(g1);
        assertTrue(r0.isEmpty());
        assertFalse(r1.isEmpty());
        assertTrue(Bytes.equals(r1.getValue(FAMILY, QUALIFIER), value0));
      }
      assertNoLocks(tableName);
    }
  }

  private static void assertNoLocks(final TableName tableName) throws IOException, InterruptedException {
    HRegion region = (HRegion) find(tableName);
    assertEquals(0, region.getLockedRows().size());
  }
  private static Region find(final TableName tableName)
      throws IOException, InterruptedException {
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    List<Region> regions = rs.getOnlineRegions(tableName);
    assertEquals(1, regions.size());
    return regions.get(0);
  }

  private static <T extends RegionObserver> T find(final TableName tableName,
          Class<T> clz) throws IOException, InterruptedException {
    Region region = find(tableName);
    Coprocessor cp = region.getCoprocessorHost().findCoprocessor(clz.getName());
    assertTrue("The cp instance should be " + clz.getName()
            + ", current instance is " + cp.getClass().getName(), clz.isInstance(cp));
    return clz.cast(cp);
  }

  public static class WatiingForMultiMutationsObserver extends BaseRegionObserver {
    final CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
            final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      try {
        latch.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }
  }

  public static class WatiingForScanObserver extends BaseRegionObserver {
    private final CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
            final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      try {
        // waiting for scanner
        latch.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
            final Scan scan, final RegionScanner s) throws IOException {
      latch.countDown();
      return s;
    }
  }
}
