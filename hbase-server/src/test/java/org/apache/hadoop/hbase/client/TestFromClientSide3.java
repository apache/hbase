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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({LargeTests.class, ClientTests.class})
public class TestFromClientSide3 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFromClientSide3.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFromClientSide3.class);
  private final static HBaseTestingUtility TEST_UTIL
    = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static Random random = new Random();
  private static int SLAVES = 3;
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] ANOTHERROW = Bytes.toBytes("anotherrow");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");
  private static final byte[] COL_QUAL = Bytes.toBytes("f1");
  private static final byte[] VAL_BYTES = Bytes.toBytes("v1");
  private static final byte[] ROW_BYTES = Bytes.toBytes("r1");

  @Rule
  public TestName name = new TestName();

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

  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : TEST_UTIL.getAdmin().listTableDescriptors()) {
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

  private void performMultiplePutAndFlush(HBaseAdmin admin, Table table, byte[] row, byte[] family,
      int nFlushes, int nPuts) throws Exception {
    for (int i = 0; i < nFlushes; i++) {
      randomCFPuts(table, row, family, nPuts);
      admin.flush(table.getName());
    }
  }

  private static List<Cell> toList(ResultScanner scanner) {
    try {
      List<Cell> cells = new ArrayList<>();
      for (Result r : scanner) {
        cells.addAll(r.listCells());
      }
      return cells;
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testScanAfterDeletingSpecifiedRow() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
            .build();
    TEST_UTIL.getAdmin().createTable(desc);
    byte[] row = Bytes.toBytes("SpecifiedRow");
    byte[] value0 = Bytes.toBytes("value_0");
    byte[] value1 = Bytes.toBytes("value_1");
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      t.put(put);
      Delete d = new Delete(row);
      t.delete(d);
      put = new Put(row);
      put.addColumn(FAMILY, null, value0);
      t.put(put);
      put = new Put(row);
      put.addColumn(FAMILY, null, value1);
      t.put(put);
      List<Cell> cells = toList(t.getScanner(new Scan()));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(t.getScanner(new Scan().addFamily(FAMILY)));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(t.getScanner(new Scan().addColumn(FAMILY, QUALIFIER)));
      assertEquals(0, cells.size());

      TEST_UTIL.getAdmin().flush(tableName);
      cells = toList(t.getScanner(new Scan()));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(t.getScanner(new Scan().addFamily(FAMILY)));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(t.getScanner(new Scan().addColumn(FAMILY, QUALIFIER)));
      assertEquals(0, cells.size());
    }
  }

  @Test
  public void testScanAfterDeletingSpecifiedRowV2() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
            .build();
    TEST_UTIL.getAdmin().createTable(desc);
    byte[] row = Bytes.toBytes("SpecifiedRow");
    byte[] qual0 = Bytes.toBytes("qual0");
    byte[] qual1 = Bytes.toBytes("qual1");
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      Delete d = new Delete(row);
      t.delete(d);

      Put put = new Put(row);
      put.addColumn(FAMILY, null, VALUE);
      t.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qual1, qual1);
      t.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qual0, qual0);
      t.put(put);

      Result r = t.get(new Get(row));
      assertEquals(3, r.size());
      assertEquals("testValue", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      assertEquals("qual0", Bytes.toString(CellUtil.cloneValue(r.rawCells()[1])));
      assertEquals("qual1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[2])));

      TEST_UTIL.getAdmin().flush(tableName);
      r = t.get(new Get(row));
      assertEquals(3, r.size());
      assertEquals("testValue", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      assertEquals("qual0", Bytes.toString(CellUtil.cloneValue(r.rawCells()[1])));
      assertEquals("qual1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[2])));
    }
  }

  private int getStoreFileCount(Admin admin, ServerName serverName, RegionInfo region)
      throws IOException {
    for (RegionMetrics metrics : admin.getRegionMetrics(serverName, region.getTable())) {
      if (Bytes.equals(region.getRegionName(), metrics.getRegionName())) {
        return metrics.getStoreFileCount();
      }
    }
    return 0;
  }

  // override the config settings at the CF level and ensure priority
  @Test
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

    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table hTable = TEST_UTIL.createTable(tableName, FAMILY, 10);
    Admin admin = TEST_UTIL.getAdmin();

    // Create 3 store files.
    byte[] row = Bytes.toBytes(random.nextInt());
    performMultiplePutAndFlush((HBaseAdmin) admin, hTable, row, FAMILY, 3, 100);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      // Verify we have multiple store files.
      HRegionLocation loc = locator.getRegionLocation(row, true);
      assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) > 1);

      // Issue a compaction request
      admin.compact(tableName);

      // poll wait for the compactions to happen
      for (int i = 0; i < 10 * 1000 / 40; ++i) {
        // The number of store files after compaction should be lesser.
        loc = locator.getRegionLocation(row, true);
        if (!loc.getRegion().isOffline()) {
          if (getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) <= 1) {
            break;
          }
        }
        Thread.sleep(40);
      }
      // verify the compactions took place and that we didn't just time out
      assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) <= 1);

      // change the compaction.min config option for this table to 5
      LOG.info("hbase.hstore.compaction.min should now be 5");
      HTableDescriptor htd = new HTableDescriptor(hTable.getDescriptor());
      htd.setValue("hbase.hstore.compaction.min", String.valueOf(5));
      admin.modifyTable(htd);
      LOG.info("alter status finished");

      // Create 3 more store files.
      performMultiplePutAndFlush((HBaseAdmin) admin, hTable, row, FAMILY, 3, 10);

      // Issue a compaction request
      admin.compact(tableName);

      // This time, the compaction request should not happen
      Thread.sleep(10 * 1000);
      loc = locator.getRegionLocation(row, true);
      int sfCount = getStoreFileCount(admin, loc.getServerName(), loc.getRegion());
      assertTrue(sfCount > 1);

      // change an individual CF's config option to 2 & online schema update
      LOG.info("hbase.hstore.compaction.min should now be 2");
      HColumnDescriptor hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
      hcd.setValue("hbase.hstore.compaction.min", String.valueOf(2));
      htd.modifyFamily(hcd);
      admin.modifyTable(htd);
      LOG.info("alter status finished");

      // Issue a compaction request
      admin.compact(tableName);

      // poll wait for the compactions to happen
      for (int i = 0; i < 10 * 1000 / 40; ++i) {
        loc = locator.getRegionLocation(row, true);
        try {
          if (getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) < sfCount) {
            break;
          }
        } catch (Exception e) {
          LOG.debug("Waiting for region to come online: " +
            Bytes.toStringBinary(loc.getRegion().getRegionName()));
        }
        Thread.sleep(40);
      }

      // verify the compaction took place and that we didn't just time out
      assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) < sfCount);

      // Finally, ensure that we can remove a custom config value after we made it
      LOG.info("Removing CF config value");
      LOG.info("hbase.hstore.compaction.min should now be 5");
      hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
      hcd.setValue("hbase.hstore.compaction.min", null);
      htd.modifyFamily(hcd);
      admin.modifyTable(htd);
      LOG.info("alter status finished");
      assertNull(hTable.getDescriptor().getColumnFamily(FAMILY).getValue(
          Bytes.toBytes("hbase.hstore.compaction.min")));
    }
  }

  @Test
  public void testHTableBatchWithEmptyPut ()throws Exception {
      Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
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

  // Test Table.batch with large amount of mutations against the same key.
  // It used to trigger read lock's "Maximum lock count exceeded" Error.
  @Test
  public void testHTableWithLargeBatch() throws Exception {
    Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
        new byte[][] { FAMILY });
    int sixtyFourK = 64 * 1024;
    try {
      List actions = new ArrayList();
      Object[] results = new Object[(sixtyFourK + 1) * 2];

      for (int i = 0; i < sixtyFourK + 1; i ++) {
        Put put1 = new Put(ROW);
        put1.addColumn(FAMILY, QUALIFIER, VALUE);
        actions.add(put1);

        Put put2 = new Put(ANOTHERROW);
        put2.addColumn(FAMILY, QUALIFIER, VALUE);
        actions.add(put2);
      }

      table.batch(actions, results);
    } finally {
      table.close();
    }
  }

  @Test
  public void testBatchWithRowMutation() throws Exception {
    LOG.info("Starting testBatchWithRowMutation");
    final TableName TABLENAME = TableName.valueOf("testBatchWithRowMutation");
    try (Table t = TEST_UTIL.createTable(TABLENAME, FAMILY)) {
      byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
      };

      RowMutations arm = RowMutations.of(Collections.singletonList(
        new Put(ROW).addColumn(FAMILY, QUALIFIERS[0], VALUE)));
      Object[] batchResult = new Object[1];
      t.batch(Arrays.asList(arm), batchResult);

      Get g = new Get(ROW);
      Result r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      arm = RowMutations.of(Arrays.asList(
        new Put(ROW).addColumn(FAMILY, QUALIFIERS[1], VALUE),
        new Delete(ROW).addColumns(FAMILY, QUALIFIERS[0])));
      t.batch(Arrays.asList(arm), batchResult);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test that we get the correct remote exception for RowMutations from batch()
      try {
        arm = RowMutations.of(Collections.singletonList(
          new Put(ROW).addColumn(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE)));
        t.batch(Arrays.asList(arm), batchResult);
        fail("Expected RetriesExhaustedWithDetailsException with NoSuchColumnFamilyException");
      } catch(RetriesExhaustedException e) {
        String msg = e.getMessage();
        assertTrue(msg.contains("NoSuchColumnFamilyException"));
      }
    }
  }

  @Test
  public void testHTableExistsMethodSingleRegionSingleGet() throws Exception {
      // Test with a single region table.
      Table table = TEST_UTIL.createTable(
          TableName.valueOf(name.getMethodName()),
          new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertFalse(exist);

    table.put(put);

    exist = table.exists(get);
    assertTrue(exist);
  }

  @Test
  public void testHTableExistsMethodSingleRegionMultipleGets() throws Exception {
    Table table = TEST_UTIL.createTable(TableName.valueOf(
        name.getMethodName()), new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    List<Get> gets = new ArrayList<>();
    gets.add(new Get(ROW));
    gets.add(new Get(ANOTHERROW));

    boolean[] results = table.exists(gets);
    assertTrue(results[0]);
    assertFalse(results[1]);
  }

  @Test
  public void testHTableExistsBeforeGet() throws Exception {
    Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
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
        TableName.valueOf(name.getMethodName()), new byte[][] { FAMILY });
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

      boolean[] exists = table.exists(getList);
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
      TableName.valueOf(name.getMethodName()), new byte[][] { FAMILY },
      1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertFalse(exist);

    table.put(put);

    exist = table.exists(get);
    assertTrue(exist);
  }

  @Test
  public void testHTableExistsMethodMultipleRegionsMultipleGets() throws Exception {
    Table table = TEST_UTIL.createTable(
      TableName.valueOf(name.getMethodName()),
      new byte[][] { FAMILY }, 1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put (put);

    List<Get> gets = new ArrayList<>();
    gets.add(new Get(ANOTHERROW));
    gets.add(new Get(Bytes.add(ROW, new byte[] { 0x00 })));
    gets.add(new Get(ROW));
    gets.add(new Get(Bytes.add(ANOTHERROW, new byte[] { 0x00 })));

    LOG.info("Calling exists");
    boolean[] results = table.exists(gets);
    assertFalse(results[0]);
    assertFalse(results[1]);
    assertTrue(results[2]);
    assertFalse(results[3]);

    // Test with the first region.
    put = new Put(new byte[] { 0x00 });
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<>();
    gets.add(new Get(new byte[] { 0x00 }));
    gets.add(new Get(new byte[] { 0x00, 0x00 }));
    results = table.exists(gets);
    assertTrue(results[0]);
    assertFalse(results[1]);

    // Test with the last region
    put = new Put(new byte[] { (byte) 0xff, (byte) 0xff });
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<>();
    gets.add(new Get(new byte[] { (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }));
    results = table.exists(gets);
    assertFalse(results[0]);
    assertTrue(results[1]);
    assertFalse(results[2]);
  }

  @Test
  public void testGetEmptyRow() throws Exception {
    //Create a table and put in 1 row
    Admin admin = TEST_UTIL.getAdmin();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(name.getMethodName())));
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
    try (
      RpcClient client = RpcClientFactory.createClient(TEST_UTIL.getConfiguration(), "cluster")) {
      assertTrue(client.hasCellBlockSupport());
    }
  }

  @Test
  public void testPutWithPreBatchMutate() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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

  @Test
  public void testRowMutationsWithPreBatchMutate() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
    desc.addCoprocessor(WaitingForScanObserver.class.getName());
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

  @Test
  public void testLockLeakWithDelta() throws Exception, Throwable {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(WaitingForMultiMutationsObserver.class.getName());
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
          // the put will be blocked by WaitingForMultiMutationsObserver.
          table.put(put);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService appendService = Executors.newSingleThreadExecutor();
      appendService.execute(() -> {
        Append append = new Append(ROW);
        append.addColumn(FAMILY, QUALIFIER, VALUE);
        try (Table table = con.getTable(tableName)) {
          table.append(append);
          fail("The APPEND should fail because the target lock is blocked by previous put");
        } catch (Exception ex) {
        }
      });
      appendService.shutdown();
      appendService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WaitingForMultiMutationsObserver observer = find(tableName, WaitingForMultiMutationsObserver.class);
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

  @Test
  public void testMultiRowMutations() throws Exception, Throwable {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(MultiRowMutationEndpoint.class.getName());
    desc.addCoprocessor(WaitingForMultiMutationsObserver.class.getName());
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
          // the put will be blocked by WaitingForMultiMutationsObserver.
          table.put(put0);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService cpService = Executors.newSingleThreadExecutor();
      AtomicBoolean exceptionDuringMutateRows = new AtomicBoolean();
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
              if (controller.failedOnException() && !(controller.getFailedOn() instanceof UnknownProtocolException)) {
                exceptionDuringMutateRows.set(true);
              }
              return rpcCallback.get();
            });
        } catch (Throwable ex) {
          LOG.error("encountered " + ex);
        }
      });
      cpService.shutdown();
      cpService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WaitingForMultiMutationsObserver observer = find(tableName,
          WaitingForMultiMutationsObserver.class);
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
      if (!exceptionDuringMutateRows.get()) {
        fail("This cp should fail because the target lock is blocked by previous put");
      }
    }
  }

  /**
   * A test case for issue HBASE-17482
   * After combile seqid with mvcc readpoint, seqid/mvcc is acquired and stamped
   * onto cells in the append thread, a countdown latch is used to ensure that happened
   * before cells can be put into memstore. But the MVCCPreAssign patch(HBASE-16698)
   * make the seqid/mvcc acquirement in handler thread and stamping in append thread
   * No countdown latch to assure cells in memstore are stamped with seqid/mvcc.
   * If cells without mvcc(A.K.A mvcc=0) are put into memstore, then a scanner
   * with a smaller readpoint can see these data, which disobey the multi version
   * concurrency control rules.
   * This test case is to reproduce this scenario.
   * @throws IOException
   */
  @Test
  public void testMVCCUsingMVCCPreAssign() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    htd.addFamily(fam);
    Admin admin = TEST_UTIL.getAdmin();
    admin.createTable(htd);
    Table table = admin.getConnection().getTable(TableName.valueOf(name.getMethodName()));
    //put two row first to init the scanner
    Put put = new Put(Bytes.toBytes("0"));
    put.addColumn(FAMILY, Bytes.toBytes( ""), Bytes.toBytes("0"));
    table.put(put);
    put = new Put(Bytes.toBytes("00"));
    put.addColumn(FAMILY, Bytes.toBytes( ""), Bytes.toBytes("0"));
    table.put(put);
    Scan scan = new Scan();
    scan.setTimeRange(0, Long.MAX_VALUE);
    scan.setCaching(1);
    ResultScanner scanner = table.getScanner(scan);
    int rowNum = scanner.next() != null ? 1 : 0;
    //the started scanner shouldn't see the rows put below
    for(int i = 1; i < 1000; i++) {
      put = new Put(Bytes.toBytes(String.valueOf(i)));
      put.setDurability(Durability.ASYNC_WAL);
      put.addColumn(FAMILY, Bytes.toBytes( ""), Bytes.toBytes(i));
      table.put(put);
    }
    for(Result result : scanner) {
      rowNum++;
    }
    //scanner should only see two rows
    assertEquals(2, rowNum);
    scanner = table.getScanner(scan);
    rowNum = 0;
    for(Result result : scanner) {
      rowNum++;
    }
    // the new scanner should see all rows
    assertEquals(1001, rowNum);


  }

  @Test
  public void testPutThenGetWithMultipleThreads() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final int THREAD_NUM = 20;
    final int ROUND_NUM = 10;
    for (int round = 0; round < ROUND_NUM; round++) {
      ArrayList<Thread> threads = new ArrayList<>(THREAD_NUM);
      final AtomicInteger successCnt = new AtomicInteger(0);
      Table ht = TEST_UTIL.createTable(tableName, FAMILY);
      for (int i = 0; i < THREAD_NUM; i++) {
        final int index = i;
        Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            final byte[] row = Bytes.toBytes("row-" + index);
            final byte[] value = Bytes.toBytes("v" + index);
            try {
              Put put = new Put(row);
              put.addColumn(FAMILY, QUALIFIER, value);
              ht.put(put);
              Get get = new Get(row);
              Result result = ht.get(get);
              byte[] returnedValue = result.getValue(FAMILY, QUALIFIER);
              if (Bytes.equals(value, returnedValue)) {
                successCnt.getAndIncrement();
              } else {
                LOG.error("Should be equal but not, original value: " + Bytes.toString(value)
                    + ", returned value: "
                    + (returnedValue == null ? "null" : Bytes.toString(returnedValue)));
              }
            } catch (Throwable e) {
              // do nothing
            }
          }
        });
        threads.add(t);
      }
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join();
      }
      assertEquals("Not equal in round " + round, THREAD_NUM, successCnt.get());
      ht.close();
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private static void assertNoLocks(final TableName tableName) throws IOException, InterruptedException {
    HRegion region = (HRegion) find(tableName);
    assertEquals(0, region.getLockedRows().size());
  }
  private static HRegion find(final TableName tableName)
      throws IOException, InterruptedException {
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    List<HRegion> regions = rs.getRegions(tableName);
    assertEquals(1, regions.size());
    return regions.get(0);
  }

  private static <T extends RegionObserver> T find(final TableName tableName,
          Class<T> clz) throws IOException, InterruptedException {
    HRegion region = find(tableName);
    Coprocessor cp = region.getCoprocessorHost().findCoprocessor(clz.getName());
    assertTrue("The cp instance should be " + clz.getName()
            + ", current instance is " + cp.getClass().getName(), clz.isInstance(cp));
    return clz.cast(cp);
  }

  public static class WaitingForMultiMutationsObserver
      implements RegionCoprocessor, RegionObserver {
    final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

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

  public static class WaitingForScanObserver implements RegionCoprocessor, RegionObserver {
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

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

  static byte[] generateHugeValue(int size) {
    Random rand = ThreadLocalRandom.current();
    byte[] value = new byte[size];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) rand.nextInt(256);
    }
    return value;
  }

  @Test
  public void testScanWithBatchSizeReturnIncompleteCells() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor hd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(3).build())
        .build();

    Table table = TEST_UTIL.createTable(hd, null);

    Put put = new Put(ROW);
    put.addColumn(FAMILY, Bytes.toBytes(0), generateHugeValue(3 * 1024 * 1024));
    table.put(put);

    put = new Put(ROW);
    put.addColumn(FAMILY, Bytes.toBytes(1), generateHugeValue(4 * 1024 * 1024));
    table.put(put);

    for (int i = 2; i < 5; i++) {
      for (int version = 0; version < 2; version++) {
        put = new Put(ROW);
        put.addColumn(FAMILY, Bytes.toBytes(i), generateHugeValue(1024));
        table.put(put);
      }
    }

    Scan scan = new Scan();
    scan.withStartRow(ROW).withStopRow(ROW, true).addFamily(FAMILY).setBatch(3)
        .setMaxResultSize(4 * 1024 * 1024);
    Result result;
    try (ResultScanner scanner = table.getScanner(scan)) {
      List<Result> list = new ArrayList<>();
      /*
       * The first scan rpc should return a result with 2 cells, because 3MB + 4MB > 4MB; The second
       * scan rpc should return a result with 3 cells, because reach the batch limit = 3; The
       * mayHaveMoreCellsInRow in last result should be false in the scan rpc. BTW, the
       * moreResultsInRegion also would be false. Finally, the client should collect all the cells
       * into two result: 2+3 -> 3+2;
       */
      while ((result = scanner.next()) != null) {
        list.add(result);
      }

      Assert.assertEquals(5, list.stream().mapToInt(Result::size).sum());
      Assert.assertEquals(2, list.size());
      Assert.assertEquals(3, list.get(0).size());
      Assert.assertEquals(2, list.get(1).size());
    }

    scan = new Scan();
    scan.withStartRow(ROW).withStopRow(ROW, true).addFamily(FAMILY).setBatch(2)
        .setMaxResultSize(4 * 1024 * 1024);
    try (ResultScanner scanner = table.getScanner(scan)) {
      List<Result> list = new ArrayList<>();
      while ((result = scanner.next()) != null) {
        list.add(result);
      }
      Assert.assertEquals(5, list.stream().mapToInt(Result::size).sum());
      Assert.assertEquals(3, list.size());
      Assert.assertEquals(2, list.get(0).size());
      Assert.assertEquals(2, list.get(1).size());
      Assert.assertEquals(1, list.get(2).size());
    }
  }
}
