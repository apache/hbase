/**
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
@Category({LargeTests.class, ClientTests.class})
public class TestAdmin1 {
  private static final Log LOG = LogFactory.getLog(TestAdmin1.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();
  }

  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd : this.admin.listTables()) {
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test (timeout=300000)
  public void testSplitFlushCompactUnknownTable() throws InterruptedException {
    final TableName unknowntable = TableName.valueOf("fubar");
    Exception exception = null;
    try {
      this.admin.compact(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.flush(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.split(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);
  }

  @Test (timeout=300000)
  public void testDeleteEditUnknownColumnFamilyAndOrTable() throws IOException {
    // Test we get exception if we try to
    final TableName nonexistentTable = TableName.valueOf("nonexistent");
    final byte[] nonexistentColumn = Bytes.toBytes("nonexistent");
    HColumnDescriptor nonexistentHcd = new HColumnDescriptor(nonexistentColumn);
    Exception exception = null;
    try {
      this.admin.addColumnFamily(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.deleteTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.deleteColumnFamily(nonexistentTable, nonexistentColumn);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.disableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.enableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.modifyColumnFamily(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      HTableDescriptor htd = new HTableDescriptor(nonexistentTable);
      htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
      this.admin.modifyTable(htd.getTableName(), htd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    // Now make it so at least the table exists and then do tests against a
    // nonexistent column family -- see if we get right exceptions.
    final TableName tableName =
      TableName.valueOf("testDeleteEditUnknownColumnFamilyAndOrTable" + System.currentTimeMillis());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("cf"));
    this.admin.createTable(htd);
    try {
      exception = null;
      try {
        this.admin.deleteColumnFamily(htd.getTableName(), nonexistentHcd.getName());
      } catch (IOException e) {
        exception = e;
      }
      assertTrue("found=" + exception.getClass().getName(),
          exception instanceof InvalidFamilyOperationException);

      exception = null;
      try {
        this.admin.modifyColumnFamily(htd.getTableName(), nonexistentHcd);
      } catch (IOException e) {
        exception = e;
      }
      assertTrue("found=" + exception.getClass().getName(),
          exception instanceof InvalidFamilyOperationException);
    } finally {
      this.admin.disableTable(tableName);
      this.admin.deleteTable(tableName);
    }
  }

  @Test (timeout=300000)
  public void testDisableAndEnableTable() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final TableName table = TableName.valueOf("testDisableAndEnableTable");
    Table ht = TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.addColumn(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    this.admin.disableTable(ht.getName());
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getTableStateManager().isTableState(
            ht.getName(), TableState.State.DISABLED));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(table));

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    ok = false;
    // verify that scan encounters correct exception
    Scan scan = new Scan();
    try {
      ResultScanner scanner = ht.getScanner(scan);
      Result res = null;
      do {
        res = scanner.next();
      } while (res != null);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    assertTrue(ok);
    this.admin.enableTable(table);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getTableStateManager().isTableState(
            ht.getName(), TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(table));

    // Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
    ht.close();
  }

  private TableState.State getStateFromMeta(TableName table) throws IOException {
    TableState state =
        MetaTableAccessor.getTableState(TEST_UTIL.getConnection(), table);
    assertNotNull(state);
    return state.getState();
  }

  @Test (timeout=300000)
  public void testDisableAndEnableTables() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final TableName table1 = TableName.valueOf("testDisableAndEnableTable1");
    final TableName table2 = TableName.valueOf("testDisableAndEnableTable2");
    Table ht1 = TEST_UTIL.createTable(table1, HConstants.CATALOG_FAMILY);
    Table ht2 = TEST_UTIL.createTable(table2, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.addColumn(HConstants.CATALOG_FAMILY, qualifier, value);
    ht1.put(put);
    ht2.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht1.get(get);
    ht2.get(get);

    this.admin.disableTables("testDisableAndEnableTable.*");

    // Test that tables are disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht1.get(get);
      ht2.get(get);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      ok = true;
    }

    assertEquals(TableState.State.DISABLED, getStateFromMeta(table1));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(table2));


    assertTrue(ok);
    this.admin.enableTables("testDisableAndEnableTable.*");

    // Test that tables are enabled
    try {
      ht1.get(get);
    } catch (IOException e) {
      ok = false;
    }
    try {
      ht2.get(get);
    } catch (IOException e) {
      ok = false;
    }
    assertTrue(ok);

    ht1.close();
    ht2.close();

    assertEquals(TableState.State.ENABLED, getStateFromMeta(table1));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(table2));
  }

  @Test (timeout=300000)
  public void testCreateTable() throws IOException {
    HTableDescriptor [] tables = admin.listTables();
    int numTables = tables.length;
    TableName tableName = TableName.valueOf("testCreateTable");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
    assertTrue("Table must be enabled.",
        TEST_UTIL.getHBaseCluster().getMaster().getTableStateManager()
            .isTableState(tableName, TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName));
  }

  @Test (timeout=300000)
  public void testTruncateTable() throws IOException {
    testTruncateTable(TableName.valueOf("testTruncateTable"), false);
  }

  @Test (timeout=300000)
  public void testTruncateTablePreservingSplits() throws IOException {
    testTruncateTable(TableName.valueOf("testTruncateTablePreservingSplits"), true);
  }

  private void testTruncateTable(final TableName tableName, boolean preserveSplits)
      throws IOException {
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);

    // Create & Fill the table
    Table table = TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY, splitKeys);
    try {
      TEST_UTIL.loadNumericRows(table, HConstants.CATALOG_FAMILY, 0, 10);
      assertEquals(10, TEST_UTIL.countRows(table));
    } finally {
      table.close();
    }
    assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());

    // Truncate & Verify
    this.admin.disableTable(tableName);
    this.admin.truncateTable(tableName, preserveSplits);
    table = TEST_UTIL.getConnection().getTable(tableName);
    try {
      assertEquals(0, TEST_UTIL.countRows(table));
    } finally {
      table.close();
    }
    if (preserveSplits) {
      assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    }
  }

  @Test (timeout=300000)
  public void testGetTableDescriptor() throws IOException {
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    HColumnDescriptor fam2 = new HColumnDescriptor("fam2");
    HColumnDescriptor fam3 = new HColumnDescriptor("fam3");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("myTestTable"));
    htd.addFamily(fam1);
    htd.addFamily(fam2);
    htd.addFamily(fam3);
    this.admin.createTable(htd);
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    HTableDescriptor confirmedHtd = table.getTableDescriptor();
    assertEquals(htd.compareTo(confirmedHtd), 0);
    MetaTableAccessor.fullScanMetaAndPrint(TEST_UTIL.getConnection());
    table.close();
  }

  @Test (timeout=300000)
  public void testCompactionTimestamps() throws Exception {
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    TableName tableName = TableName.valueOf("testCompactionTimestampsTable");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(fam1);
    this.admin.createTable(htd);
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    long ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    assertEquals(0, ts);
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    // no files written -> no data
    assertEquals(0, ts);

    this.admin.flush(tableName);
    ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    // still 0, we flushed a file, but no major compaction happened
    assertEquals(0, ts);

    byte[] regionName;
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      regionName = l.getAllRegionLocations().get(0).getRegionInfo().getRegionName();
    }
    long ts1 = this.admin.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    this.admin.flush(tableName);
    ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    // make sure the region API returns the same value, as the old file is still around
    assertEquals(ts1, ts);

    TEST_UTIL.compact(tableName, true);
    table.put(p);
    // forces a wait for the compaction
    this.admin.flush(tableName);
    ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    // after a compaction our earliest timestamp will have progressed forward
    assertTrue(ts > ts1);

    // region api still the same
    ts1 = this.admin.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    table.put(p);
    this.admin.flush(tableName);
    ts = this.admin.getLastMajorCompactionTimestamp(tableName);
    assertEquals(ts, ts1);
    table.close();
  }

  @Test (timeout=300000)
  public void testHColumnValidName() {
       boolean exceptionThrown;
       try {
         new HColumnDescriptor("\\test\\abc");
       } catch(IllegalArgumentException iae) {
           exceptionThrown = true;
           assertTrue(exceptionThrown);
       }
   }

  /**
   * Verify schema modification takes.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testOnlineChangeTableSchema() throws IOException, InterruptedException {
    final TableName tableName =
        TableName.valueOf("changeTableSchemaOnline");
    HTableDescriptor [] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);

    // FIRST, do htabledescriptor changes.
    HTableDescriptor htd = this.admin.getTableDescriptor(tableName);
    // Make a copy and assert copy is good.
    HTableDescriptor copy = new HTableDescriptor(htd);
    assertTrue(htd.equals(copy));
    // Now amend the copy. Introduce differences.
    long newFlushSize = htd.getMemStoreFlushSize() / 2;
    if (newFlushSize <=0) {
      newFlushSize = HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE / 2;
    }
    copy.setMemStoreFlushSize(newFlushSize);
    final String key = "anyoldkey";
    assertTrue(htd.getValue(key) == null);
    copy.setValue(key, key);
    boolean expectedException = false;
    try {
      admin.modifyTable(tableName, copy);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertFalse(expectedException);
    HTableDescriptor modifiedHtd = this.admin.getTableDescriptor(tableName);
    assertFalse(htd.equals(modifiedHtd));
    assertTrue(copy.equals(modifiedHtd));
    assertEquals(newFlushSize, modifiedHtd.getMemStoreFlushSize());
    assertEquals(key, modifiedHtd.getValue(key));

    // Now work on column family changes.
    int countOfFamilies = modifiedHtd.getFamilies().size();
    assertTrue(countOfFamilies > 0);
    HColumnDescriptor hcd = modifiedHtd.getFamilies().iterator().next();
    int maxversions = hcd.getMaxVersions();
    final int newMaxVersions = maxversions + 1;
    hcd.setMaxVersions(newMaxVersions);
    final byte [] hcdName = hcd.getName();
    expectedException = false;
    try {
      this.admin.modifyColumnFamily(tableName, hcd);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertFalse(expectedException);
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    HColumnDescriptor modifiedHcd = modifiedHtd.getFamily(hcdName);
    assertEquals(newMaxVersions, modifiedHcd.getMaxVersions());

    // Try adding a column
    assertFalse(this.admin.isTableDisabled(tableName));
    final String xtracolName = "xtracol";
    HColumnDescriptor xtracol = new HColumnDescriptor(xtracolName);
    xtracol.setValue(xtracolName, xtracolName);
    expectedException = false;
    try {
      this.admin.addColumnFamily(tableName, xtracol);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    // Add column should work even if the table is enabled
    assertFalse(expectedException);
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    hcd = modifiedHtd.getFamily(xtracol.getName());
    assertTrue(hcd != null);
    assertTrue(hcd.getValue(xtracolName).equals(xtracolName));

    // Delete the just-added column.
    this.admin.deleteColumnFamily(tableName, xtracol.getName());
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    hcd = modifiedHtd.getFamily(xtracol.getName());
    assertTrue(hcd == null);

    // Delete the table
    this.admin.disableTable(tableName);
    this.admin.deleteTable(tableName);
    this.admin.listTables();
    assertFalse(this.admin.tableExists(tableName));
  }

  protected void verifyRoundRobinDistribution(ClusterConnection c, RegionLocator regionLocator, int
      expectedRegions) throws IOException {
    int numRS = c.getCurrentNrHRS();
    List<HRegionLocation> regions = regionLocator.getAllRegionLocations();
    Map<ServerName, List<HRegionInfo>> server2Regions = new HashMap<>();
    for (HRegionLocation loc : regions) {
      ServerName server = loc.getServerName();
      List<HRegionInfo> regs = server2Regions.get(server);
      if (regs == null) {
        regs = new ArrayList<>();
        server2Regions.put(server, regs);
      }
      regs.add(loc.getRegionInfo());
    }
    if (numRS >= 2) {
      // Ignore the master region server,
      // which contains less regions by intention.
      numRS--;
    }
    float average = (float) expectedRegions/numRS;
    int min = (int)Math.floor(average);
    int max = (int)Math.ceil(average);
    for (List<HRegionInfo> regionList : server2Regions.values()) {
      assertTrue(regionList.size() == min || regionList.size() == max);
    }
  }

  @Test (timeout=300000)
  public void testCreateTableNumberOfRegions() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("testCreateTableNumberOfRegions");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    List<HRegionLocation> regions;
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 1 region", 1, regions.size());
    }

    TableName TABLE_2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    desc = new HTableDescriptor(TABLE_2);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, new byte[][]{new byte[]{42}});
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(TABLE_2)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 2 region", 2, regions.size());
    }

    TableName TABLE_3 = TableName.valueOf(tableName.getNameAsString() + "_3");
    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, "a".getBytes(), "z".getBytes(), 3);
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(TABLE_3)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 3 region", 3, regions.size());
    }

    TableName TABLE_4 = TableName.valueOf(tableName.getNameAsString() + "_4");
    desc = new HTableDescriptor(TABLE_4);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    try {
      admin.createTable(desc, "a".getBytes(), "z".getBytes(), 2);
      fail("Should not be able to create a table with only 2 regions using this API.");
    } catch (IllegalArgumentException eae) {
    // Expected
    }

    TableName TABLE_5 = TableName.valueOf(tableName.getNameAsString() + "_5");
    desc = new HTableDescriptor(TABLE_5);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, new byte[] { 1 }, new byte[] { 127 }, 16);
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(TABLE_5)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have 16 region", 16, regions.size());
    }
  }

  @Test (timeout=300000)
  public void testCreateTableWithRegions() throws IOException, InterruptedException {

    TableName tableName = TableName.valueOf("testCreateTableWithRegions");

    byte [][] splitKeys = {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 4, 4, 4 },
        new byte [] { 5, 5, 5 },
        new byte [] { 6, 6, 6 },
        new byte [] { 7, 7, 7 },
        new byte [] { 8, 8, 8 },
        new byte [] { 9, 9, 9 },
    };
    int expectedRegions = splitKeys.length + 1;

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    boolean tableAvailable = admin.isTableAvailable(tableName, splitKeys);
    assertTrue("Table should be created with splitKyes + 1 rows in META", tableAvailable);

    List<HRegionLocation> regions;
    Iterator<HRegionLocation> hris;
    HRegionInfo hri;
    ClusterConnection conn = (ClusterConnection) TEST_UTIL.getConnection();
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      regions = l.getAllRegionLocations();

      assertEquals("Tried to create " + expectedRegions + " regions " +
              "but only found " + regions.size(), expectedRegions, regions.size());
      System.err.println("Found " + regions.size() + " regions");

      hris = regions.iterator();
      hri = hris.next().getRegionInfo();
      assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[0]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[0]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[1]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[1]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[2]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[2]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[3]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[3]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[4]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[4]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[5]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[5]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[6]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[6]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[7]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[7]));
      assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[8]));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[8]));
      assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

      verifyRoundRobinDistribution(conn, l, expectedRegions);
    }



    // Now test using start/end with a number of regions

    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };

    // Splitting into 10 regions, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle

    expectedRegions = 10;

    TableName TABLE_2 = TableName.valueOf(tableName.getNameAsString() + "_2");

    desc = new HTableDescriptor(TABLE_2);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc, startKey, endKey, expectedRegions);

    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(TABLE_2)) {
      regions = l.getAllRegionLocations();
      assertEquals("Tried to create " + expectedRegions + " regions " +
          "but only found " + regions.size(), expectedRegions, regions.size());
      System.err.println("Found " + regions.size() + " regions");

      hris = regions.iterator();
      hri = hris.next().getRegionInfo();
      assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }));
      assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 }));
      hri = hris.next().getRegionInfo();
      assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 }));
      assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

      verifyRoundRobinDistribution(conn, l, expectedRegions);
    }

    // Try once more with something that divides into something infinite

    startKey = new byte [] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte [] { 1, 0, 0, 0, 0, 0 };

    expectedRegions = 5;

    TableName TABLE_3 = TableName.valueOf(tableName.getNameAsString() + "_3");

    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc, startKey, endKey, expectedRegions);


    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(TABLE_3)) {
      regions = l.getAllRegionLocations();
      assertEquals("Tried to create " + expectedRegions + " regions " +
          "but only found " + regions.size(), expectedRegions, regions.size());
      System.err.println("Found " + regions.size() + " regions");

      verifyRoundRobinDistribution(conn, l, expectedRegions);
    }


    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte [][] {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 2, 2, 2 }
    };

    TableName TABLE_4 = TableName.valueOf(tableName.getNameAsString() + "_4");
    desc = new HTableDescriptor(TABLE_4);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    try {
      admin.createTable(desc, splitKeys);
      assertTrue("Should not be able to create this table because of " +
          "duplicate split keys", false);
    } catch(IllegalArgumentException iae) {
      // Expected
    }
  }

  @Test (timeout=300000)
  public void testTableAvailableWithRandomSplitKeys() throws Exception {
    TableName tableName = TableName.valueOf("testTableAvailableWithRandomSplitKeys");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col"));
    byte[][] splitKeys = new byte[1][];
    splitKeys = new byte [][] {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 }
    };
    admin.createTable(desc);
    boolean tableAvailable = admin.isTableAvailable(tableName, splitKeys);
    assertFalse("Table should be created with 1 row in META", tableAvailable);
  }

  @Test (timeout=300000)
  public void testCreateTableWithOnlyEmptyStartRow() throws IOException {
    byte[] tableName = Bytes.toBytes("testCreateTableWithOnlyEmptyStartRow");
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor("col"));
    try {
      admin.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test (timeout=300000)
  public void testCreateTableWithEmptyRowInTheSplitKeys() throws IOException{
    byte[] tableName = Bytes.toBytes("testCreateTableWithEmptyRowInTheSplitKeys");
    byte[][] splitKeys = new byte[3][];
    splitKeys[0] = "region1".getBytes();
    splitKeys[1] = HConstants.EMPTY_BYTE_ARRAY;
    splitKeys[2] = "region2".getBytes();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor("col"));
    try {
      admin.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
      LOG.info("Expected ", e);
    }
  }

  @Test (timeout=120000)
  public void testTableExist() throws IOException {
    final TableName table = TableName.valueOf("testTableExist");
    boolean exist;
    exist = this.admin.tableExists(table);
    assertEquals(false, exist);
    TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    exist = this.admin.tableExists(table);
    assertEquals(true, exist);
  }

  /**
   * Tests forcing split from client and having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test (timeout=400000)
  public void testForceSplit() throws Exception {
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf") };
    int[] rowCounts = new int[] { 6000 };
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;
    int blockSize = 256;
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    byte[] splitKey = Bytes.toBytes(3500);
    splitTest(splitKey, familyNames, rowCounts, numVersions, blockSize);
  }

  /**
   * Test retain assignment on enableTable.
   *
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testEnableTableRetainAssignment() throws IOException {
    final TableName tableName = TableName.valueOf("testEnableTableAssignment");
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
        new byte[] { 3, 3, 3 }, new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 },
        new byte[] { 6, 6, 6 }, new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 },
        new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      List<HRegionLocation> regions = l.getAllRegionLocations();

      assertEquals(
          "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
          expectedRegions, regions.size());
      // Disable table.
      admin.disableTable(tableName);
      // Enable table, use retain assignment to assign regions.
      admin.enableTable(tableName);
      List<HRegionLocation> regions2 = l.getAllRegionLocations();

      // Check the assignment.
      assertEquals(regions.size(), regions2.size());
      assertTrue(regions2.containsAll(regions));
    }
  }

  /**
   * Multi-family scenario. Tests forcing split from client and
   * having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test (timeout=800000)
  public void testForceSplitMultiFamily() throws Exception {
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;

    // use small HFile block size so that we can have lots of blocks in HFile
    // Otherwise, if there is only one block,
    // HFileBlockIndex.midKey()'s value == startKey
    int blockSize = 256;
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf1"),
      Bytes.toBytes("cf2") };

    // one of the column families isn't splittable
    int[] rowCounts = new int[] { 6000, 1 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 1, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    // one column family has much smaller data than the other
    // the split key should be based on the largest column family
    rowCounts = new int[] { 6000, 300 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 300, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

  }

  void splitTest(byte[] splitPoint, byte[][] familyNames, int[] rowCounts,
    int numVersions, int blockSize) throws Exception {
    TableName tableName = TableName.valueOf("testForceSplit");
    StringBuilder sb = new StringBuilder();
    // Add tail to String so can see better in logs where a test is running.
    for (int i = 0; i < rowCounts.length; i++) {
      sb.append("_").append(Integer.toString(rowCounts[i]));
    }
    assertFalse(admin.tableExists(tableName));
    try(final Table table = TEST_UTIL.createTable(tableName, familyNames,
      numVersions, blockSize);
      final RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {

      int rowCount = 0;
      byte[] q = new byte[0];

      // insert rows into column families. The number of rows that have values
      // in a specific column family is decided by rowCounts[familyIndex]
      for (int index = 0; index < familyNames.length; index++) {
        ArrayList<Put> puts = new ArrayList<Put>(rowCounts[index]);
        for (int i = 0; i < rowCounts[index]; i++) {
          byte[] k = Bytes.toBytes(i);
          Put put = new Put(k);
          put.addColumn(familyNames[index], q, k);
          puts.add(put);
        }
        table.put(puts);

        if (rowCount < rowCounts[index]) {
          rowCount = rowCounts[index];
        }
      }

      // get the initial layout (should just be one region)
      List<HRegionLocation> m = locator.getAllRegionLocations();
      LOG.info("Initial regions (" + m.size() + "): " + m);
      assertTrue(m.size() == 1);

      // Verify row count
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      int rows = 0;
      for (@SuppressWarnings("unused") Result result : scanner) {
        rows++;
      }
      scanner.close();
      assertEquals(rowCount, rows);

      // Have an outstanding scan going on to make sure we can scan over splits.
      scan = new Scan();
      scanner = table.getScanner(scan);
      // Scan first row so we are into first region before split happens.
      scanner.next();

      // Split the table
      this.admin.split(tableName, splitPoint);

      final AtomicInteger count = new AtomicInteger(0);
      Thread t = new Thread("CheckForSplit") {
        @Override
        public void run() {
          for (int i = 0; i < 45; i++) {
            try {
              sleep(1000);
            } catch (InterruptedException e) {
              continue;
            }
            // check again
            List<HRegionLocation> regions = null;
            try {
              regions = locator.getAllRegionLocations();
            } catch (IOException e) {
              e.printStackTrace();
            }
            if (regions == null)
              continue;
            count.set(regions.size());
            if (count.get() >= 2) {
              LOG.info("Found: " + regions);
              break;
            }
            LOG.debug("Cycle waiting on split");
          }
          LOG.debug("CheckForSplit thread exited, current region count: " + count.get());
        }
      };
      t.setPriority(Thread.NORM_PRIORITY - 2);
      t.start();
      t.join();

      // Verify row count
      rows = 1; // We counted one row above.
      for (@SuppressWarnings("unused") Result result : scanner) {
        rows++;
        if (rows > rowCount) {
          scanner.close();
          assertTrue("Scanned more than expected (" + rowCount + ")", false);
        }
      }
      scanner.close();
      assertEquals(rowCount, rows);

      List<HRegionLocation> regions = null;
      try {
        regions = locator.getAllRegionLocations();
      } catch (IOException e) {
        e.printStackTrace();
      }
      assertEquals(2, regions.size());
      if (splitPoint != null) {
        // make sure the split point matches our explicit configuration
        assertEquals(Bytes.toString(splitPoint),
            Bytes.toString(regions.get(0).getRegionInfo().getEndKey()));
        assertEquals(Bytes.toString(splitPoint),
            Bytes.toString(regions.get(1).getRegionInfo().getStartKey()));
        LOG.debug("Properly split on " + Bytes.toString(splitPoint));
      } else {
        if (familyNames.length > 1) {
          int splitKey = Bytes.toInt(regions.get(0).getRegionInfo().getEndKey());
          // check if splitKey is based on the largest column family
          // in terms of it store size
          int deltaForLargestFamily = Math.abs(rowCount / 2 - splitKey);
          LOG.debug("SplitKey=" + splitKey + "&deltaForLargestFamily=" + deltaForLargestFamily +
              ", r=" + regions.get(0).getRegionInfo());
          for (int index = 0; index < familyNames.length; index++) {
            int delta = Math.abs(rowCounts[index] / 2 - splitKey);
            if (delta < deltaForLargestFamily) {
              assertTrue("Delta " + delta + " for family " + index + " should be at least "
                      + "deltaForLargestFamily " + deltaForLargestFamily, false);
            }
          }
        }
      }
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitAndMergeWithReplicaTable() throws Exception {
    // The test tries to directly split replica regions and directly merge replica regions. These
    // are not allowed. The test validates that. Then the test does a valid split/merge of allowed
    // regions.
    // Set up a table with 3 regions and replication set to 3
    TableName tableName = TableName.valueOf("testSplitAndMergeWithReplicaTable");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setRegionReplication(3);
    byte[] cf = "f".getBytes();
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    desc.addFamily(hcd);
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[]{(byte)'4'};
    splitRows[1] = new byte[]{(byte)'7'};
    TEST_UTIL.getAdmin().createTable(desc, splitRows);
    List<HRegion> oldRegions;
    do {
      oldRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
      Thread.sleep(10);
    } while (oldRegions.size() != 9); //3 regions * 3 replicas
    // write some data to the table
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<Put>();
    byte[] qualifier = "c".getBytes();
    Put put = new Put(new byte[]{(byte)'1'});
    put.addColumn(cf, qualifier, "100".getBytes());
    puts.add(put);
    put = new Put(new byte[]{(byte)'6'});
    put.addColumn(cf, qualifier, "100".getBytes());
    puts.add(put);
    put = new Put(new byte[]{(byte)'8'});
    put.addColumn(cf, qualifier, "100".getBytes());
    puts.add(put);
    ht.put(puts);
    ht.close();
    List<Pair<HRegionInfo, ServerName>> regions =
        MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getConnection(), tableName);
    boolean gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via the split API . Should fail
    try {
      TEST_UTIL.getAdmin().splitRegion(regions.get(1).getFirst().getRegionName());
    } catch (IllegalArgumentException ex) {
      gotException = true;
    }
    assertTrue(gotException);
    gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via a different split API (the difference is
    // this API goes direct to the regionserver skipping any checks in the admin). Should fail
    try {
      TEST_UTIL.getHBaseAdmin().split(regions.get(1).getSecond(), regions.get(1).getFirst(),
          new byte[]{(byte)'1'});
    } catch (IOException ex) {
      gotException = true;
    }
    assertTrue(gotException);
    gotException = false;
    // Try merging a replica with another. Should fail.
    try {
      TEST_UTIL.getAdmin().mergeRegionsAsync(regions.get(1).getFirst().getEncodedNameAsBytes(),
          regions.get(2).getFirst().getEncodedNameAsBytes(), true);
    } catch (IllegalArgumentException m) {
      gotException = true;
    }
    assertTrue(gotException);
    // Try going to the master directly (that will skip the check in admin)
    try {
      DispatchMergingRegionsRequest request = RequestConverter
          .buildDispatchMergingRegionsRequest(
            regions.get(1).getFirst().getEncodedNameAsBytes(),
            regions.get(2).getFirst().getEncodedNameAsBytes(),
            true,
            HConstants.NO_NONCE,
            HConstants.NO_NONCE);
      ((ClusterConnection) TEST_UTIL.getAdmin().getConnection()).getMaster()
        .dispatchMergingRegions(null, request);
    } catch (ServiceException m) {
      Throwable t = m.getCause();
      do {
        if (t instanceof MergeRegionException) {
          gotException = true;
          break;
        }
        t = t.getCause();
      } while (t != null);
    }
    assertTrue(gotException);
    gotException = false;
    // Try going to the regionservers directly
    // first move the region to the same regionserver
    if (!regions.get(2).getSecond().equals(regions.get(1).getSecond())) {
      moveRegionAndWait(regions.get(2).getFirst(), regions.get(1).getSecond());
    }
    try {
      AdminService.BlockingInterface admin = ((ClusterConnection) TEST_UTIL.getAdmin()
        .getConnection()).getAdmin(regions.get(1).getSecond());
      ProtobufUtil.mergeRegions(null, admin, regions.get(1).getFirst(), regions.get(2).getFirst(),
        true, null);
    } catch (MergeRegionException mm) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  private void moveRegionAndWait(HRegionInfo destRegion, ServerName destServer)
      throws InterruptedException, MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getAdmin().move(
        destRegion.getEncodedNameAsBytes(),
        Bytes.toBytes(destServer.getServerName()));
    while (true) {
      ServerName serverName = master.getAssignmentManager()
          .getRegionStates().getRegionServerOfRegion(destRegion);
      if (serverName != null && serverName.equals(destServer)) {
        TEST_UTIL.assertRegionOnServer(
            destRegion, serverName, 200);
        break;
      }
      Thread.sleep(10);
    }
  }

  @Test (expected=IllegalArgumentException.class, timeout=300000)
  public void testInvalidHColumnDescriptor() throws IOException {
     new HColumnDescriptor("/cfamily/name");
  }

  @Test (timeout=300000)
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    TableName tableName = TableName.valueOf("testEnableDisableAddColumnDeleteColumn");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!this.admin.isTableEnabled(TableName.valueOf("testEnableDisableAddColumnDeleteColumn"))) {
      Thread.sleep(10);
    }
    this.admin.disableTable(tableName);
    try {
      TEST_UTIL.getConnection().getTable(tableName);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      //expected
    }

    this.admin.addColumnFamily(tableName, new HColumnDescriptor("col2"));
    this.admin.enableTable(tableName);
    try {
      this.admin.deleteColumnFamily(tableName, Bytes.toBytes("col2"));
    } catch (TableNotDisabledException e) {
      LOG.info(e);
    }
    this.admin.disableTable(tableName);
    this.admin.deleteTable(tableName);
  }

  @Test (timeout=300000)
  public void testDeleteLastColumnFamily() throws Exception {
    TableName tableName = TableName.valueOf("testDeleteLastColumnFamily");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!this.admin.isTableEnabled(TableName.valueOf("testDeleteLastColumnFamily"))) {
      Thread.sleep(10);
    }

    // test for enabled table
    try {
      this.admin.deleteColumnFamily(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    // test for disabled table
    this.admin.disableTable(tableName);

    try {
      this.admin.deleteColumnFamily(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    this.admin.deleteTable(tableName);
  }

  /*
   * Test DFS replication for column families, where one CF has default replication(3) and the other
   * is set to 1.
   */
  @Test(timeout = 300000)
  public void testHFileReplication() throws Exception {
    TableName name = TableName.valueOf("testHFileReplication");
    String fn1 = "rep1";
    HColumnDescriptor hcd1 = new HColumnDescriptor(fn1);
    hcd1.setDFSReplication((short) 1);
    String fn = "defaultRep";
    HColumnDescriptor hcd = new HColumnDescriptor(fn);
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(hcd);
    htd.addFamily(hcd1);
    Table table = TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(name);
    Put p = new Put(Bytes.toBytes("defaultRep_rk"));
    byte[] q1 = Bytes.toBytes("q1");
    byte[] v1 = Bytes.toBytes("v1");
    p.addColumn(Bytes.toBytes(fn), q1, v1);
    List<Put> puts = new ArrayList<Put>(2);
    puts.add(p);
    p = new Put(Bytes.toBytes("rep1_rk"));
    p.addColumn(Bytes.toBytes(fn1), q1, v1);
    puts.add(p);
    try {
      table.put(puts);
      admin.flush(name);

      List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(name);
      for (HRegion r : regions) {
        Store store = r.getStore(Bytes.toBytes(fn));
        for (StoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn));
          assertTrue("Column family " + fn + " should have 3 copies",
            FSUtils.getDefaultReplication(TEST_UTIL.getTestFileSystem(), sf.getPath()) == (sf
                .getFileInfo().getFileStatus().getReplication()));
        }

        store = r.getStore(Bytes.toBytes(fn1));
        for (StoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn1));
          assertTrue("Column family " + fn1 + " should have only 1 copy", 1 == sf.getFileInfo()
              .getFileStatus().getReplication());
        }
      }
    } finally {
      if (admin.isTableEnabled(name)) {
        this.admin.disableTable(name);
        this.admin.deleteTable(name);
      }
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    TableName tableName = TableName.valueOf("testMergeWithFullRegionName");
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[]{(byte)'3'};
    splitRows[1] = new byte[]{(byte)'6'};
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<HRegionInfo> tableRegions;
      HRegionInfo regionA;
      HRegionInfo regionB;

      // merge with full name
      tableRegions = admin.getTableRegions(tableName);
      assertEquals(3, admin.getTableRegions(tableName).size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegionsAsync(regionA.getRegionName(), regionB.getRegionName(), false);
      Thread.sleep(1000);
      assertEquals(2, admin.getTableRegions(tableName).size());

      // merge with encoded name
      tableRegions = admin.getTableRegions(tableName);
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegionsAsync(
        regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
      Thread.sleep(1000);
      assertEquals(1, admin.getTableRegions(tableName).size());
    } finally {
      this.admin.disableTable(tableName);
      this.admin.deleteTable(tableName);
    }
  }
}
