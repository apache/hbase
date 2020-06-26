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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

@Category(LargeTests.class)
public class TestFromClientSide3 {
  private static final Log LOG = LogFactory.getLog(TestFromClientSide3.class);
  private final static HBaseTestingUtility TEST_UTIL
    = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static Random random = new Random();
  private static int SLAVES = 3;
  private static final byte [] ROW = Bytes.toBytes("testRow");
  private static final byte[] ANOTHERROW = Bytes.toBytes("anotherrow");
  private static final byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte [] VALUE = Bytes.toBytes("testValue");
  private static final byte[] COL_QUAL = Bytes.toBytes("f1");
  private static final byte[] VAL_BYTES = Bytes.toBytes("v1");
  private static final byte[] ROW_BYTES = Bytes.toBytes("r1");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", true);
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
      put.add(family, qualifier, value);
    }
    table.put(put);
  }

  private void performMultiplePutAndFlush(HBaseAdmin admin, HTable table,
      byte[] row, byte[] family, int nFlushes, int nPuts)
  throws Exception {

    // connection needed for poll-wait
    HRegionLocation loc = table.getRegionLocation(row, true);
    AdminProtos.AdminService.BlockingInterface server =
      admin.getConnection().getAdmin(loc.getServerName());
    byte[] regName = loc.getRegionInfo().getRegionName();

    for (int i = 0; i < nFlushes; i++) {
      randomCFPuts(table, row, family, nPuts);
      List<String> sf = ProtobufUtil.getStoreFiles(server, regName, FAMILY);
      int sfCount = sf.size();

      // TODO: replace this api with a synchronous flush after HBASE-2949
      admin.flush(table.getTableName());

      // synchronously poll wait for a new storefile to appear (flush happened)
      while (ProtobufUtil.getStoreFiles(
          server, regName, FAMILY).size() == sfCount) {
        Thread.sleep(40);
      }
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
    TableName tableName = TableName.valueOf("testScanAfterDeletingSpecifiedRow");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getHBaseAdmin().createTable(desc);
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

      TEST_UTIL.getHBaseAdmin().flush(tableName);
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
    TableName tableName = TableName.valueOf("testScanAfterDeletingSpecifiedRowV2");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getHBaseAdmin().createTable(desc);
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

      TEST_UTIL.getHBaseAdmin().flush(tableName);
      r = t.get(new Get(row));
      assertEquals(3, r.size());
      assertEquals("testValue", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      assertEquals("qual0", Bytes.toString(CellUtil.cloneValue(r.rawCells()[1])));
      assertEquals("qual1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[2])));
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

    String tableName = "testAdvancedConfigOverride";
    TableName TABLE = TableName.valueOf(tableName);
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    ClusterConnection connection = (ClusterConnection)TEST_UTIL.getConnection();

    // Create 3 store files.
    byte[] row = Bytes.toBytes(random.nextInt());
    performMultiplePutAndFlush(admin, hTable, row, FAMILY, 3, 100);

    // Verify we have multiple store files.
    HRegionLocation loc = hTable.getRegionLocation(row, true);
    byte[] regionName = loc.getRegionInfo().getRegionName();
    AdminProtos.AdminService.BlockingInterface server =
      connection.getAdmin(loc.getServerName());
    assertTrue(ProtobufUtil.getStoreFiles(
      server, regionName, FAMILY).size() > 1);

    // Issue a compaction request
    admin.compact(TABLE.getName());

    // poll wait for the compactions to happen
    for (int i = 0; i < 10 * 1000 / 40; ++i) {
      // The number of store files after compaction should be lesser.
      loc = hTable.getRegionLocation(row, true);
      if (!loc.getRegionInfo().isOffline()) {
        regionName = loc.getRegionInfo().getRegionName();
        server = connection.getAdmin(loc.getServerName());
        if (ProtobufUtil.getStoreFiles(
            server, regionName, FAMILY).size() <= 1) {
          break;
        }
      }
      Thread.sleep(40);
    }
    // verify the compactions took place and that we didn't just time out
    assertTrue(ProtobufUtil.getStoreFiles(
      server, regionName, FAMILY).size() <= 1);

    // change the compaction.min config option for this table to 5
    LOG.info("hbase.hstore.compaction.min should now be 5");
    HTableDescriptor htd = new HTableDescriptor(hTable.getTableDescriptor());
    htd.setValue("hbase.hstore.compaction.min", String.valueOf(5));
    admin.modifyTable(TABLE, htd);
    Pair<Integer, Integer> st;
    while (null != (st = admin.getAlterStatus(TABLE)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }
    LOG.info("alter status finished");

    // Create 3 more store files.
    performMultiplePutAndFlush(admin, hTable, row, FAMILY, 3, 10);

    // Issue a compaction request
    admin.compact(TABLE.getName());

    // This time, the compaction request should not happen
    Thread.sleep(10 * 1000);
    loc = hTable.getRegionLocation(row, true);
    regionName = loc.getRegionInfo().getRegionName();
    server = connection.getAdmin(loc.getServerName());
    int sfCount = ProtobufUtil.getStoreFiles(
      server, regionName, FAMILY).size();
    assertTrue(sfCount > 1);

    // change an individual CF's config option to 2 & online schema update
    LOG.info("hbase.hstore.compaction.min should now be 2");
    HColumnDescriptor hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
    hcd.setValue("hbase.hstore.compaction.min", String.valueOf(2));
    htd.modifyFamily(hcd);
    admin.modifyTable(TABLE, htd);
    while (null != (st = admin.getAlterStatus(TABLE)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }
    LOG.info("alter status finished");

    // Issue a compaction request
    admin.compact(TABLE.getName());

    // poll wait for the compactions to happen
    for (int i = 0; i < 10 * 1000 / 40; ++i) {
      loc = hTable.getRegionLocation(row, true);
      regionName = loc.getRegionInfo().getRegionName();
      try {
        server = connection.getAdmin(loc.getServerName());
        if (ProtobufUtil.getStoreFiles(
            server, regionName, FAMILY).size() < sfCount) {
          break;
        }
      } catch (Exception e) {
        LOG.debug("Waiting for region to come online: " + Bytes.toString(regionName));
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
    admin.modifyTable(TABLE, htd);
    while (null != (st = admin.getAlterStatus(TABLE)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }
    LOG.info("alter status finished");
    assertNull(hTable.getTableDescriptor().getFamily(FAMILY).getValue(
        "hbase.hstore.compaction.min"));
  }

  @Test
  public void testHTableBatchWithEmptyPut() throws Exception {
    Table table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableBatchWithEmptyPut"), new byte[][] { FAMILY });
    try {
      List actions = (List) new ArrayList();
      Object[] results = new Object[2];
      // create an empty Put
      Put put1 = new Put(ROW);
      actions.add(put1);

      Put put2 = new Put(ANOTHERROW);
      put2.add(FAMILY, QUALIFIER, VALUE);
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
    Table table = TEST_UTIL.createTable(TableName.valueOf("testHTableWithLargeBatch"),
            new byte[][] { FAMILY });
    /*private static final Configuration conf = new Configuration();
    ClusterConnection conn = new MyConnectionImpl(conf);
    HTable ht = new HTable(conn, new BufferedMutatorParams(DUMMY_TABLE));
    ht.multiAp = new MyAsyncProcess(conn, conf, false);*/
    int sixtyFourK = 0;
    List actions = new ArrayList();
    Put put1,put2;
    try {

      Object[] results = new Object[(sixtyFourK + 1) * 2];

      for (int i = 0; i < sixtyFourK + 1; i ++) {
        put1 = new Put(ROW);
        put1.addColumn(FAMILY, QUALIFIER, VALUE);
        Random random= new Random();

        put1.setId(Integer.toString(random.nextInt()));
        //System.out.println("in test "+put1.getId());

        actions.add(put1);

        put2 = new Put(ANOTHERROW);
        put2.addColumn(FAMILY, QUALIFIER, VALUE);
        put2.setId(Integer.toString(random.nextInt()));
        //System.out.println("in test "+put2.getId());
        actions.add(put2);

      }

      table.batch(actions, results);
    } finally {
      table.close();
    }

    //System.out.println("end");

  }

  @Test
  public void testBatchWithRowMutation() throws Exception {
    LOG.info("Starting testBatchWithRowMutation");
    final TableName TABLENAME = TableName.valueOf("testBatchWithRowMutation");
    try (Table t = TEST_UTIL.createTable(TABLENAME, FAMILY)) {
      byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
      };
      RowMutations arm = new RowMutations(ROW);
      Put p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[0], VALUE);
      arm.add(p);
      Object[] batchResult = new Object[1];
      t.batch(Arrays.asList(arm), batchResult);

      Get g = new Get(ROW);
      Result r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[1], VALUE);
      arm.add(p);
      Delete d = new Delete(ROW);
      d.addColumns(FAMILY, QUALIFIERS[0]);
      arm.add(d);
      t.batch(Arrays.asList(arm), batchResult);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test that we get the correct remote exception for RowMutations from batch()
      try {
        arm = new RowMutations(ROW);
        p = new Put(ROW);
        p.addColumn(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE);
        arm.add(p);
        t.batch(Arrays.asList(arm), batchResult);
        fail("Expected RetriesExhaustedWithDetailsException with NoSuchColumnFamilyException");
      } catch (RetriesExhaustedWithDetailsException e) {
        String msg = e.getMessage();
        assertTrue(msg.contains("NoSuchColumnFamilyException"));
      }
    }
  }

  @Test
  public void testHTableExistsMethodSingleRegionSingleGet() throws Exception {

    // Test with a single region table.

    Table table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableExistsMethodSingleRegionSingleGet"), new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertEquals(exist, false);

    table.put(put);

    exist = table.exists(get);
    assertEquals(exist, true);
  }

  @Test
  public void testHTableExistsMethodSingleRegionMultipleGets() throws Exception {

    HTable table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableExistsMethodSingleRegionMultipleGets"), new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    List<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ROW));
    gets.add(new Get(ANOTHERROW));

    Boolean[] results = table.exists(gets);
    assertTrue(results[0]);
    assertFalse(results[1]);
  }

  @Test
  public void testHTableExistsBeforeGet() throws Exception {
    Table table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableExistsBeforeGet"), new byte[][] { FAMILY });
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
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
      Bytes.toBytes("testHTableExistsAllBeforeGet"), new byte[][] { FAMILY });
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      put = new Put(ROW2);
      put.add(FAMILY, QUALIFIER, VALUE);
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
    put.add(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);

    boolean exist = table.exists(get);
    assertEquals(exist, false);

    table.put(put);

    exist = table.exists(get);
    assertEquals(exist, true);
  }

  @Test
  public void testHTableExistsMethodMultipleRegionsMultipleGets() throws Exception {
    HTable table = TEST_UTIL.createTable(
      TableName.valueOf("testHTableExistsMethodMultipleRegionsMultipleGets"),
      new byte[][] { FAMILY }, 1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put (put);

    List<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ANOTHERROW));
    gets.add(new Get(Bytes.add(ROW, new byte[] { 0x00 })));
    gets.add(new Get(ROW));
    gets.add(new Get(Bytes.add(ANOTHERROW, new byte[] { 0x00 })));

    LOG.info("Calling exists");
    Boolean[] results = table.exists(gets);
    assertEquals(results[0], false);
    assertEquals(results[1], false);
    assertEquals(results[2], true);
    assertEquals(results[3], false);

    // Test with the first region.
    put = new Put(new byte[] { 0x00 });
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<Get>();
    gets.add(new Get(new byte[] { 0x00 }));
    gets.add(new Get(new byte[] { 0x00, 0x00 }));
    results = table.exists(gets);
    assertEquals(results[0], true);
    assertEquals(results[1], false);

    // Test with the last region
    put = new Put(new byte[] { (byte) 0xff, (byte) 0xff });
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    gets = new ArrayList<Get>();
    gets.add(new Get(new byte[] { (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff }));
    gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }));
    results = table.exists(gets);
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
    Table table = new HTable(TEST_UTIL.getConfiguration(), desc.getTableName());

    Put put = new Put(ROW_BYTES);
    put.add(FAMILY, COL_QUAL, VAL_BYTES);
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

  @Test(timeout = 30000)
  public void testMultiRowMutations() throws Exception, Throwable {
    final TableName tableName = TableName.valueOf("testMultiRowMutations");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addCoprocessor(MultiRowMutationEndpoint.class.getName());
    desc.addCoprocessor(WatiingForMultiMutationsObserver.class.getName());
    desc.setConfiguration("hbase.rowlock.wait.duration", String.valueOf(5000));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    // new a connection for lower retry number.
    Configuration copy = new Configuration(TEST_UTIL.getConfiguration());
    copy.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection con = ConnectionFactory.createConnection(copy)) {
      final byte[] row = Bytes.toBytes("ROW-0");
      final byte[] rowLocked= Bytes.toBytes("ROW-1");
      final byte[] value0 = Bytes.toBytes("VALUE-0");
      final byte[] value1 = Bytes.toBytes("VALUE-1");
      final byte[] value2 = Bytes.toBytes("VALUE-2");
      assertNoLocks(tableName);
      ExecutorService putService = Executors.newSingleThreadExecutor();
      putService.execute(new Runnable() {
        @Override
        public void run() {
          try (Table table = con.getTable(tableName)) {
            Put put0 = new Put(rowLocked);
            put0.addColumn(FAMILY, QUALIFIER, value0);
            // the put will be blocked by WatiingForMultiMutationsObserver.
            table.put(put0);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      });
      ExecutorService cpService = Executors.newSingleThreadExecutor();
      cpService.execute(new Runnable() {
        @Override
        public void run() {
          boolean threw;
          Put put1 = new Put(row);
          Put put2 = new Put(rowLocked);
          put1.addColumn(FAMILY, QUALIFIER, value1);
          put2.addColumn(FAMILY, QUALIFIER, value2);
          try (Table table = con.getTable(tableName)) {
            final MultiRowMutationProtos.MutateRowsRequest request
              = MultiRowMutationProtos.MutateRowsRequest.newBuilder()
                .addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
                        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT, put1))
                .addMutationRequest(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(
                        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType.PUT, put2))
                .build();
            table.coprocessorService(MultiRowMutationProtos.MultiRowMutationService.class, ROW, ROW,
              new Batch.Call<MultiRowMutationProtos.MultiRowMutationService, MultiRowMutationProtos.MutateRowsResponse>() {
                public MultiRowMutationProtos.MutateRowsResponse call(MultiRowMutationProtos.MultiRowMutationService instance) throws IOException {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<MultiRowMutationProtos.MutateRowsResponse> rpcCallback = new BlockingRpcCallback<>();
                  instance.mutateRows(controller, request, rpcCallback);
                  return rpcCallback.get();
                }
              });
            threw = false;
          } catch (Throwable ex) {
            threw = true;
          }
          if (!threw) {
            // Can't call fail() earlier because the catch would eat it.
            fail("This cp should fail because the target lock is blocked by previous put");
          }
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
    public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
            final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      try {
        latch.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }
  }

  private static byte[] generateHugeValue(int size) {
    Random rand = ThreadLocalRandom.current();
    byte[] value = new byte[size];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) rand.nextInt(256);
    }
    return value;
  }

  @Test
  public void testScanWithBatchSizeReturnIncompleteCells() throws IOException {
    TableName tableName = TableName.valueOf("testScanWithBatchSizeReturnIncompleteCells");
    HTableDescriptor hd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY).setMaxVersions(3);
    hd.addFamily(hcd);

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
      Assert.assertEquals(3, list.size());
      Assert.assertEquals(2, list.get(0).size());
      Assert.assertEquals(2, list.get(1).size());
      Assert.assertEquals(1, list.get(2).size());
    }
  }
}
