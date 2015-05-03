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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestFromClientSide3 {
  final Log LOG = LogFactory.getLog(getClass());
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
    // Nothing to do.
  }

  private void randomCFPuts(HTable table, byte[] row, byte[] family, int nPuts)
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
      byte[] row, byte[] family, int nFlushes, int nPuts) throws Exception {

    // connection needed for poll-wait
    HConnection conn = HConnectionManager.getConnection(TEST_UTIL
        .getConfiguration());
    HRegionLocation loc = table.getRegionLocation(row, true);
    AdminProtos.AdminService.BlockingInterface server = conn.getAdmin(loc.getServerName());
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
    TableName TABLE =
        TableName.valueOf(tableName);
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HConnection connection = HConnectionManager.getConnection(TEST_UTIL
        .getConfiguration());

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
    htd.addFamily(hcd);
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
    htd.addFamily(hcd);
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
    HTable table = TEST_UTIL.createTable(
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
  
  @Test
  public void testHTableExistsMethodSingleRegionSingleGet() throws Exception {

    // Test with a single region table.

    HTable table = TEST_UTIL.createTable(
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

  public void testHTableExistsMethodSingleRegionMultipleGets() throws Exception {

    HTable table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableExistsMethodSingleRegionMultipleGets"), new byte[][] { FAMILY });

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    List<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ROW));
    gets.add(null);
    gets.add(new Get(ANOTHERROW));

    Boolean[] results = table.exists(gets);
    assertEquals(results[0], true);
    assertEquals(results[1], false);
    assertEquals(results[2], false);
  }

  @Test
  public void testHTableExistsMethodMultipleRegionsSingleGet() throws Exception {

    HTable table = TEST_UTIL.createTable(
      Bytes.toBytes("testHTableExistsMethodMultipleRegionsSingleGet"), new byte[][] { FAMILY }, 1,
      new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
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
      Bytes.toBytes("testHTableExistsMethodMultipleRegionsMultipleGets"), new byte[][] { FAMILY },
      1, new byte[] { 0x00 }, new byte[] { (byte) 0xff }, 255);
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
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("test")));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), "test");

    Put put = new Put(ROW_BYTES);
    put.add(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(put);
    table.flushCommits();

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
  public void testLeaseRenewal() throws Exception {
    HTable table = TEST_UTIL.createTable(
      Bytes.toBytes("testLeaseRenewal"), FAMILY);
    Put p = new Put(ROW_BYTES);
    p.add(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(p);
    p = new Put(ANOTHERROW);
    p.add(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(p);
    Scan s = new Scan();
    s.setCaching(1);
    ResultScanner rs = table.getScanner(s);
    // make sure that calling renewLease does not impact the scan results
    assertTrue(((AbstractClientScanner)rs).renewLease());
    assertTrue(Arrays.equals(rs.next().getRow(), ANOTHERROW));
    assertTrue(((AbstractClientScanner)rs).renewLease());
    assertTrue(Arrays.equals(rs.next().getRow(), ROW_BYTES));
    assertTrue(((AbstractClientScanner)rs).renewLease());
    assertNull(rs.next());
    assertFalse(((AbstractClientScanner)rs).renewLease());
    rs.close();
    table.close();
  }
}
