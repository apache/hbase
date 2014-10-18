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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtilsForTests;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
public class TestAdmin {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);
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
      TEST_UTIL.deleteTable(htd.getName());
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
      this.admin.addColumn(nonexistentTable, nonexistentHcd);
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
      this.admin.deleteColumn(nonexistentTable, nonexistentColumn);
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
      this.admin.modifyColumn(nonexistentTable, nonexistentHcd);
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
        this.admin.deleteColumn(htd.getTableName(), nonexistentHcd.getName());
      } catch (IOException e) {
        exception = e;
      }
      assertTrue("found=" + exception.getClass().getName(),
          exception instanceof InvalidFamilyOperationException);

      exception = null;
      try {
        this.admin.modifyColumn(htd.getTableName(), nonexistentHcd);
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
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    this.admin.disableTable(ht.getName());
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getTableStateManager().isTableState(
        ht.getName(), TableState.State.DISABLED));

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
        .getMaster().getAssignmentManager().getTableStateManager().isTableState(
        ht.getName(), TableState.State.ENABLED));

    // Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
    ht.close();
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
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
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
  }

  @Test (timeout=300000)
  public void testCreateTable() throws IOException {
    HTableDescriptor [] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(TableName.valueOf("testCreateTable"), HConstants.CATALOG_FAMILY).close();
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getTableStateManager().isTableState(
        TableName.valueOf("testCreateTable"), TableState.State.ENABLED));
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
    HTable table = TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY, splitKeys);
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
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
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
    Table table = new HTable(TEST_UTIL.getConfiguration(), htd.getTableName());
    HTableDescriptor confirmedHtd = table.getTableDescriptor();
    assertEquals(htd.compareTo(confirmedHtd), 0);
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
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", true);
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
      this.admin.modifyColumn(tableName, hcd);
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
      this.admin.addColumn(tableName, xtracol);
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
    this.admin.deleteColumn(tableName, xtracol.getName());
    modifiedHtd = this.admin.getTableDescriptor(tableName);
    hcd = modifiedHtd.getFamily(xtracol.getName());
    assertTrue(hcd == null);

    // Delete the table
    this.admin.disableTable(tableName);
    this.admin.deleteTable(tableName);
    this.admin.listTables();
    assertFalse(this.admin.tableExists(tableName));
  }

  @Test (timeout=300000)
  public void testShouldFailOnlineSchemaUpdateIfOnlineSchemaIsNotEnabled()
      throws Exception {
    final TableName tableName = TableName.valueOf("changeTableSchemaOnlineFailure");
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", false);
    HTableDescriptor[] tables = admin.listTables();
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
    assertTrue("Online schema update should not happen.", expectedException);

    // Reset the value for the other tests
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", true);
  }

  @SuppressWarnings("deprecation")
  protected void verifyRoundRobinDistribution(HTable ht, int expectedRegions) throws IOException {
    int numRS = ht.getConnection().getCurrentNrHRS();
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    Map<ServerName, List<HRegionInfo>> server2Regions = new HashMap<ServerName, List<HRegionInfo>>();
    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      ServerName server = entry.getValue();
      List<HRegionInfo> regs = server2Regions.get(server);
      if (regs == null) {
        regs = new ArrayList<HRegionInfo>();
        server2Regions.put(server, regs);
      }
      regs.add(entry.getKey());
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
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    assertEquals("Table should have only 1 region", 1, regions.size());
    ht.close();

    TableName TABLE_2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    desc = new HTableDescriptor(TABLE_2);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, new byte[][]{new byte[]{42}});
    HTable ht2 = new HTable(TEST_UTIL.getConfiguration(), TABLE_2);
    regions = ht2.getRegionLocations();
    assertEquals("Table should have only 2 region", 2, regions.size());
    ht2.close();

    TableName TABLE_3 = TableName.valueOf(tableName.getNameAsString() + "_3");
    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, "a".getBytes(), "z".getBytes(), 3);
    HTable ht3 = new HTable(TEST_UTIL.getConfiguration(), TABLE_3);
    regions = ht3.getRegionLocations();
    assertEquals("Table should have only 3 region", 3, regions.size());
    ht3.close();

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
    admin.createTable(desc, new byte[] {1}, new byte[] {127}, 16);
    HTable ht5 = new HTable(TEST_UTIL.getConfiguration(), TABLE_5);
    regions = ht5.getRegionLocations();
    assertEquals("Table should have 16 region", 16, regions.size());
    ht5.close();
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

    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    Iterator<HRegionInfo> hris = regions.keySet().iterator();
    HRegionInfo hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[0]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[0]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[1]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[1]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[2]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[2]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[3]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[3]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[4]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[4]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[5]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[5]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[6]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[6]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[7]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[7]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[8]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[8]));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

    verifyRoundRobinDistribution(ht, expectedRegions);
    ht.close();

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
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedRegions);

    HTable ht2 = new HTable(TEST_UTIL.getConfiguration(), TABLE_2);
    regions = ht2.getRegionLocations();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    hris = regions.keySet().iterator();
    hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);

    verifyRoundRobinDistribution(ht2, expectedRegions);
    ht2.close();

    // Try once more with something that divides into something infinite

    startKey = new byte [] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte [] { 1, 0, 0, 0, 0, 0 };

    expectedRegions = 5;

    TableName TABLE_3 = TableName.valueOf(tableName.getNameAsString() + "_3");

    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedRegions);


    HTable ht3 = new HTable(TEST_UTIL.getConfiguration(), TABLE_3);
    regions = ht3.getRegionLocations();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    verifyRoundRobinDistribution(ht3, expectedRegions);
    ht3.close();


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
    Admin ladmin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    try {
      ladmin.createTable(desc, splitKeys);
      assertTrue("Should not be able to create this table because of " +
          "duplicate split keys", false);
    } catch(IllegalArgumentException iae) {
      // Expected
    }
    ladmin.close();
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
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    ht.close();
    assertEquals("Tried to create " + expectedRegions + " regions "
        + "but only found " + regions.size(), expectedRegions, regions.size());
    // Disable table.
    admin.disableTable(tableName);
    // Enable table, use retain assignment to assign regions.
    admin.enableTable(tableName);
    Map<HRegionInfo, ServerName> regions2 = ht.getRegionLocations();

    // Check the assignment.
    assertEquals(regions.size(), regions2.size());
    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      assertEquals(regions2.get(entry.getKey()), entry.getValue());
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
    final HTable table = TEST_UTIL.createTable(tableName, familyNames,
      numVersions, blockSize);

    int rowCount = 0;
    byte[] q = new byte[0];

    // insert rows into column families. The number of rows that have values
    // in a specific column family is decided by rowCounts[familyIndex]
    for (int index = 0; index < familyNames.length; index++) {
      ArrayList<Put> puts = new ArrayList<Put>(rowCounts[index]);
      for (int i = 0; i < rowCounts[index]; i++) {
        byte[] k = Bytes.toBytes(i);
        Put put = new Put(k);
        put.add(familyNames[index], q, k);
        puts.add(put);
      }
      table.put(puts);

      if ( rowCount < rowCounts[index] ) {
        rowCount = rowCounts[index];
      }
    }

    // get the initial layout (should just be one region)
    Map<HRegionInfo, ServerName> m = table.getRegionLocations();
    LOG.info("Initial regions (" + m.size() + "): " + m);
    assertTrue(m.size() == 1);

    // Verify row count
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    int rows = 0;
    for(@SuppressWarnings("unused") Result result : scanner) {
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
          // check again    table = new HTable(conf, tableName);
          Map<HRegionInfo, ServerName> regions = null;
          try {
            regions = table.getRegionLocations();
          } catch (IOException e) {
            e.printStackTrace();
          }
          if (regions == null) continue;
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

    Map<HRegionInfo, ServerName> regions = null;
    try {
      regions = table.getRegionLocations();
    } catch (IOException e) {
      e.printStackTrace();
    }
    assertEquals(2, regions.size());
    Set<HRegionInfo> hRegionInfos = regions.keySet();
    HRegionInfo[] r = hRegionInfos.toArray(new HRegionInfo[hRegionInfos.size()]);
    if (splitPoint != null) {
      // make sure the split point matches our explicit configuration
      assertEquals(Bytes.toString(splitPoint),
          Bytes.toString(r[0].getEndKey()));
      assertEquals(Bytes.toString(splitPoint),
          Bytes.toString(r[1].getStartKey()));
      LOG.debug("Properly split on " + Bytes.toString(splitPoint));
    } else {
      if (familyNames.length > 1) {
        int splitKey = Bytes.toInt(r[0].getEndKey());
        // check if splitKey is based on the largest column family
        // in terms of it store size
        int deltaForLargestFamily = Math.abs(rowCount/2 - splitKey);
        LOG.debug("SplitKey=" + splitKey + "&deltaForLargestFamily=" + deltaForLargestFamily +
          ", r=" + r[0]);
        for (int index = 0; index < familyNames.length; index++) {
          int delta = Math.abs(rowCounts[index]/2 - splitKey);
          if (delta < deltaForLargestFamily) {
            assertTrue("Delta " + delta + " for family " + index
              + " should be at least deltaForLargestFamily " + deltaForLargestFamily,
              false);
          }
        }
      }
    }
    TEST_UTIL.deleteTable(tableName);
    table.close();
  }

  /**
   * HADOOP-2156
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @Test (expected=IllegalArgumentException.class, timeout=300000)
  public void testEmptyHTableDescriptor() throws IOException {
    this.admin.createTable(new HTableDescriptor());
  }

  @Test (expected=IllegalArgumentException.class, timeout=300000)
  public void testInvalidHColumnDescriptor() throws IOException {
     new HColumnDescriptor("/cfamily/name");
  }

  @Test (timeout=300000)
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    TableName tableName = TableName.valueOf("testMasterAdmin");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!this.admin.isTableEnabled(TableName.valueOf("testMasterAdmin"))) {
      Thread.sleep(10);
    }
    this.admin.disableTable(tableName);
    try {
      new HTable(TEST_UTIL.getConfiguration(), tableName);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      //expected
    }

    this.admin.addColumn(tableName, new HColumnDescriptor("col2"));
    this.admin.enableTable(tableName);
    try {
      this.admin.deleteColumn(tableName, Bytes.toBytes("col2"));
    } catch (TableNotDisabledException e) {
      LOG.info(e);
    }
    this.admin.disableTable(tableName);
    this.admin.deleteTable(tableName);
  }

  @Test (timeout=300000)
  public void testCreateBadTables() throws IOException {
    String msg = null;
    try {
      this.admin.createTable(new HTableDescriptor(TableName.META_TABLE_NAME));
    } catch(TableExistsException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(TableExistsException.class.getName()) &&
      msg.contains(TableName.META_TABLE_NAME.getNameAsString()));

    // Now try and do concurrent creation with a bunch of threads.
    final HTableDescriptor threadDesc =
      new HTableDescriptor(TableName.valueOf("threaded_testCreateBadTables"));
    threadDesc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    int count = 10;
    Thread [] threads = new Thread [count];
    final AtomicInteger successes = new AtomicInteger(0);
    final AtomicInteger failures = new AtomicInteger(0);
    final Admin localAdmin = this.admin;
    for (int i = 0; i < count; i++) {
      threads[i] = new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          try {
            localAdmin.createTable(threadDesc);
            successes.incrementAndGet();
          } catch (TableExistsException e) {
            failures.incrementAndGet();
          } catch (IOException e) {
            throw new RuntimeException("Failed threaded create" + getName(), e);
          }
        }
      };
    }
    for (int i = 0; i < count; i++) {
      threads[i].start();
    }
    for (int i = 0; i < count; i++) {
      while(threads[i].isAlive()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // All threads are now dead.  Count up how many tables were created and
    // how many failed w/ appropriate exception.
    assertEquals(1, successes.get());
    assertEquals(count - 1, failures.get());
  }

  /**
   * Test for hadoop-1581 'HBASE: Unopenable tablename bug'.
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testTableNameClash() throws Exception {
    String name = "testTableNameClash";
    HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf(name + "SOMEUPPERCASE"));
    HTableDescriptor htd2 = new HTableDescriptor(TableName.valueOf(name));
    htd1.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    htd2.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(htd1);
    admin.createTable(htd2);
    // Before fix, below would fail throwing a NoServerForRegionException.
    new HTable(TEST_UTIL.getConfiguration(), htd2.getTableName()).close();
  }

  /***
   * HMaster.createTable used to be kind of synchronous call
   * Thus creating of table with lots of regions can cause RPC timeout
   * After the fix to make createTable truly async, RPC timeout shouldn't be an
   * issue anymore
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testCreateTableRPCTimeOut() throws Exception {
    String name = "testCreateTableRPCTimeOut";
    int oldTimeout = TEST_UTIL.getConfiguration().
      getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);
    try {
      int expectedRegions = 100;
      // Use 80 bit numbers to make sure we aren't limited
      byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
      byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
      Admin hbaseadmin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
      htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
      hbaseadmin.createTable(htd, startKey, endKey, expectedRegions);
      hbaseadmin.close();
    } finally {
      TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, oldTimeout);
    }
  }

  /**
   * Test read only tables
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testReadOnlyTable() throws Exception {
    TableName name = TableName.valueOf("testReadOnlyTable");
    Table table = TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    byte[] value = Bytes.toBytes("somedata");
    // This used to use an empty row... That must have been a bug
    Put put = new Put(value);
    put.add(HConstants.CATALOG_FAMILY, HConstants.CATALOG_FAMILY, value);
    table.put(put);
    table.close();
  }

  /**
   * Test that user table names can contain '-' and '.' so long as they do not
   * start with same. HBASE-771
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testTableNames() throws IOException {
    byte[][] illegalNames = new byte[][] {
        Bytes.toBytes("-bad"),
        Bytes.toBytes(".bad")
    };
    for (byte[] illegalName : illegalNames) {
      try {
        new HTableDescriptor(TableName.valueOf(illegalName));
        throw new IOException("Did not detect '" +
            Bytes.toString(illegalName) + "' as an illegal user table name");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
    byte[] legalName = Bytes.toBytes("g-oo.d");
    try {
      new HTableDescriptor(TableName.valueOf(legalName));
    } catch (IllegalArgumentException e) {
      throw new IOException("Legal user table name: '" +
        Bytes.toString(legalName) + "' caused IllegalArgumentException: " +
        e.getMessage());
    }
  }

  /**
   * For HADOOP-2579
   * @throws IOException
   */
  @Test (expected=TableExistsException.class, timeout=300000)
  public void testTableExistsExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf("testTableExistsExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY).close();
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
  }

  /**
   * Can't disable a table if the table isn't in enabled state
   * @throws IOException
   */
  @Test (expected=TableNotEnabledException.class, timeout=300000)
  public void testTableNotEnabledExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf("testTableNotEnabledExceptionWithATable");
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY).close();
    this.admin.disableTable(name);
    this.admin.disableTable(name);
  }

  /**
   * Can't enable a table if the table isn't in disabled state
   * @throws IOException
   */
  @Test (expected=TableNotDisabledException.class, timeout=300000)
  public void testTableNotDisabledExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf("testTableNotDisabledExceptionWithATable");
    Table t = TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    try {
    this.admin.enableTable(name);
    }finally {
       t.close();
    }
  }

  /**
   * For HADOOP-2579
   * @throws IOException
   */
  @Test (expected=TableNotFoundException.class, timeout=300000)
  public void testTableNotFoundExceptionWithoutAnyTables() throws IOException {
    TableName tableName = TableName
        .valueOf("testTableNotFoundExceptionWithoutAnyTables");
    Table ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    ht.get(new Get("e".getBytes()));
  }


  @Test (timeout=300000)
  public void testShouldCloseTheRegionBasedOnTheEncodedRegionName()
      throws Exception {
    TableName TABLENAME =
        TableName.valueOf("TestHBACloseRegion");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.getTable().isSystemTable()) {
        info = regionInfo;
        admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), rs
            .getServerName().getServerName());
      }
    }
    boolean isInList = ProtobufUtil.getOnlineRegions(
      rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(
        rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.",
      isInList);
  }

  @Test (timeout=300000)
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion1");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion1")) {
          info = regionInfo;
          try {
            admin.closeRegionWithEncodedRegionName("sample", rs.getServerName()
              .getServerName());
          } catch (NotServingRegionException nsre) {
            // expected, ignore it
          }
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
        onlineRegions.contains(info));
  }

  @Test (timeout=300000)
  public void testCloseRegionThatFetchesTheHRIFromMeta() throws Exception {
    TableName TABLENAME =
        TableName.valueOf("TestHBACloseRegion2");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {

        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion2")) {
          info = regionInfo;
          admin.closeRegion(regionInfo.getRegionNameAsString(), rs
              .getServerName().getServerName());
        }
      }
    }

    boolean isInList = ProtobufUtil.getOnlineRegions(
      rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(
        rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.",
      isInList);
  }

  @Test (timeout=300000)
  public void testCloseRegionWhenServerNameIsNull() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion3");
    createTableWithDefaultConf(TABLENAME);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegion3")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
                null);
          }
        }
      }
      fail("The test should throw exception if the servername passed is null.");
    } catch (IllegalArgumentException e) {
    }
  }


  @Test (timeout=300000)
  public void testCloseRegionWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegionWhenServerNameIsEmpty");
    createTableWithDefaultConf(TABLENAME);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegionWhenServerNameIsEmpty")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
                " ");
          }
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test (timeout=300000)
  public void testCloseRegionWhenEncodedRegionNameIsNotGiven() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion4");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion4")) {
          info = regionInfo;
          try {
            admin.closeRegionWithEncodedRegionName(regionInfo
              .getRegionNameAsString(), rs.getServerName().getServerName());
          } catch (NotServingRegionException nsre) {
            // expected, ignore it.
          }
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
        onlineRegions.contains(info));
  }

  private HBaseAdmin createTable(byte[] TABLENAME) throws IOException {

    Configuration config = TEST_UTIL.getConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLENAME));
    HColumnDescriptor hcd = new HColumnDescriptor("value");

    htd.addFamily(hcd);
    admin.createTable(htd, null);
    return admin;
  }

  private void createTableWithDefaultConf(byte[] TABLENAME) throws IOException {
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));
  }

  private void createTableWithDefaultConf(TableName TABLENAME) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null);
  }

  /**
   * For HBASE-2556
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testGetTableRegions() throws IOException {

    final TableName tableName = TableName.valueOf("testGetTableRegions");

    int expectedRegions = 10;

    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };


    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, startKey, endKey, expectedRegions);

    List<HRegionInfo> RegionInfos = admin.getTableRegions(tableName);

    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + RegionInfos.size(),
        expectedRegions, RegionInfos.size());

 }

  @Test (timeout=300000)
  public void testHLogRollWriting() throws Exception {
    setUpforLogRolling();
    String className = this.getClass().getName();
    StringBuilder v = new StringBuilder(className);
    while (v.length() < 1000) {
      v.append(className);
    }
    byte[] value = Bytes.toBytes(v.toString());
    HRegionServer regionServer = startAndWriteData(TableName.valueOf("TestLogRolling"), value);
    LOG.info("after writing there are "
        + HLogUtilsForTests.getNumRolledLogFiles(regionServer.getWAL()) + " log files");

    // flush all regions

    List<HRegion> regions = new ArrayList<HRegion>(regionServer
        .getOnlineRegionsLocalContext());
    for (HRegion r : regions) {
      r.flushcache();
    }
    admin.rollHLogWriter(regionServer.getServerName().getServerName());
    int count = HLogUtilsForTests.getNumRolledLogFiles(regionServer.getWAL());
    LOG.info("after flushing all regions and rolling logs there are " +
        count + " log files");
    assertTrue(("actual count: " + count), count <= 2);
  }

  @Test (timeout=300000)
  public void testMoveToPreviouslyAssignedRS() throws IOException, InterruptedException {
    byte[] tableName = Bytes.toBytes("testMoveToPreviouslyAssignedRS");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    HBaseAdmin localAdmin = createTable(tableName);
    List<HRegionInfo> tableRegions = localAdmin.getTableRegions(tableName);
    HRegionInfo hri = tableRegions.get(0);
    AssignmentManager am = master.getAssignmentManager();
    assertTrue("Region " + hri.getRegionNameAsString()
      + " should be assigned properly", am.waitForAssignment(hri));
    ServerName server = am.getRegionStates().getRegionServerOfRegion(hri);
    localAdmin.move(hri.getEncodedNameAsBytes(), Bytes.toBytes(server.getServerName()));
    assertEquals("Current region server and region server before move should be same.", server,
      am.getRegionStates().getRegionServerOfRegion(hri));
  }


  private void setUpforLogRolling() {
    // Force a region split after every 768KB
    TEST_UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE,
        768L * 1024L);

    // We roll the log after every 32 writes
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maxlogentries", 32);

    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // For less frequently updated regions flush after every 2 flushes
    TEST_UTIL.getConfiguration().setInt(
        "hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    TEST_UTIL.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        8192);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 10 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    TEST_UTIL.getConfiguration().setInt(HConstants.THREAD_WAKE_FREQUENCY,
        2 * 1000);

    /**** configuration for testLogRollOnDatanodeDeath ****/
    // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.tolerable.lowreplication", 2);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 3);
  }

  private HRegionServer startAndWriteData(TableName tableName, byte[] value)
  throws IOException, InterruptedException {
    // When the hbase:meta table can be opened, the region servers are running
    new HTable(
      TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME).close();

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    for (int i = 1; i <= 256; i++) { // 256 writes should cause 8 log rolls
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }

    table.close();
    return regionServer;
  }

  /**
   * HBASE-4417 checkHBaseAvailable() doesn't close zk connections
   */
  @Test (timeout=300000)
  public void testCheckHBaseAvailableClosesConnection() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    int initialCount = HConnectionTestingUtility.getConnectionCount();
    HBaseAdmin.checkHBaseAvailable(conf);
    int finalCount = HConnectionTestingUtility.getConnectionCount();

    Assert.assertEquals(initialCount, finalCount) ;
  }

  /**
   * Check that we have an exception if the cluster is not there.
   */
  @Test (timeout=300000)
  public void testCheckHBaseAvailableWithoutCluster() {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());

    // Change the ZK address to go to something not used.
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
      conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 9999)+10);

    int initialCount = HConnectionTestingUtility.getConnectionCount();

    long start = System.currentTimeMillis();
    try {
      HBaseAdmin.checkHBaseAvailable(conf);
      assertTrue(false);
    } catch (MasterNotRunningException ignored) {
    } catch (ZooKeeperConnectionException ignored) {
    } catch (ServiceException ignored) {
    } catch (IOException ignored) {
    }
    long end = System.currentTimeMillis();

    int finalCount = HConnectionTestingUtility.getConnectionCount();

    Assert.assertEquals(initialCount, finalCount) ;

    LOG.info("It took "+(end-start)+" ms to find out that" +
      " HBase was not available");
  }

  @Test (timeout=300000)
  public void testDisableCatalogTable() throws Exception {
    try {
      this.admin.disableTable(TableName.META_TABLE_NAME);
      fail("Expected to throw ConstraintException");
    } catch (ConstraintException e) {
    }
    // Before the fix for HBASE-6146, the below table creation was failing as the hbase:meta table
    // actually getting disabled by the disableTable() call.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testDisableCatalogTable".getBytes()));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1".getBytes());
    htd.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(htd);
  }

  @Test (timeout=300000)
  public void testIsEnabledOrDisabledOnUnknownTable() throws Exception {
    try {
      admin.isTableEnabled(TableName.valueOf("unkownTable"));
      fail("Test should fail if isTableEnabled called on unknown table.");
    } catch (IOException e) {
    }

    try {
      admin.isTableDisabled(TableName.valueOf("unkownTable"));
      fail("Test should fail if isTableDisabled called on unknown table.");
    } catch (IOException e) {
    }
  }

  @Test (timeout=300000)
  public void testGetRegion() throws Exception {
    // We use actual HBaseAdmin instance instead of going via Admin interface in
    // here because makes use of an internal HBA method (TODO: Fix.).
    HBaseAdmin rawAdmin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    final TableName tableName = TableName.valueOf("testGetRegion");
    LOG.info("Started " + tableName);
    HTable t = TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    TEST_UTIL.createMultiRegions(t, HConstants.CATALOG_FAMILY);

    HRegionLocation regionLocation = t.getRegionLocation("mmm");
    HRegionInfo region = regionLocation.getRegionInfo();
    byte[] regionName = region.getRegionName();
    Pair<HRegionInfo, ServerName> pair = rawAdmin.getRegion(regionName);
    assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
    pair = rawAdmin.getRegion(region.getEncodedNameAsBytes());
    assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
  }
}
