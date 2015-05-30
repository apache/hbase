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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
@Category(LargeTests.class)
public class TestAdmin1 {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;

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
  public void testFailedCatalogTrackerGetCleansUpProperly()
  throws ZooKeeperConnectionException, IOException {
    // An HBaseAdmin that we can make fail when it goes to get catalogtracker.
    final AtomicBoolean fail = new AtomicBoolean(false);
    final AtomicReference<CatalogTracker> internalCt = new AtomicReference<CatalogTracker>();
    HBaseAdmin doctoredAdmin = new HBaseAdmin(this.admin.getConfiguration()) {
      @Override
      protected CatalogTracker startCatalogTracker(CatalogTracker ct)
      throws IOException, InterruptedException {
        internalCt.set(ct);
        super.startCatalogTracker(ct);
        if (fail.get()) {
          throw new IOException("Intentional test fail",
            new KeeperException.ConnectionLossException());
        }
        return ct;
      }
    };
    try {
      CatalogTracker ct = doctoredAdmin.getCatalogTracker();
      assertFalse(ct.isStopped());
      doctoredAdmin.cleanupCatalogTracker(ct);
      assertTrue(ct.isStopped());
      // Now have mess with our doctored admin and make the start of catalog tracker 'fail'.
      fail.set(true);
      boolean expectedException = false;
      try {
        doctoredAdmin.getCatalogTracker();
      } catch (IOException ioe) {
        assertTrue(ioe.getCause() instanceof KeeperException.ConnectionLossException);
        expectedException = true;
      }
      if (!expectedException) fail("Didn't get expected exception!");
      // Assert that the internally made ct was properly shutdown.
      assertTrue("Internal CatalogTracker not closed down", internalCt.get().isStopped());
    } finally {
      doctoredAdmin.close();
    }
  }

  @Test (timeout=300000)
  public void testSplitFlushCompactUnknownTable() throws InterruptedException {
    final String unknowntable = "fubar";
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
    final String nonexistent = "nonexistent";
    HColumnDescriptor nonexistentHcd = new HColumnDescriptor(nonexistent);
    Exception exception = null;
    try {
      this.admin.addColumn(nonexistent, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.deleteTable(nonexistent);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.deleteColumn(nonexistent, nonexistent);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.disableTable(nonexistent);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.enableTable(nonexistent);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      this.admin.modifyColumn(nonexistent, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(nonexistent));
      this.admin.modifyTable(htd.getTableName(), htd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    // Now make it so at least the table exists and then do tests against a
    // nonexistent column family -- see if we get right exceptions.
    final String tableName =
        "testDeleteEditUnknownColumnFamilyAndOrTable" + System.currentTimeMillis();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
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
    final byte [] table = Bytes.toBytes("testDisableAndEnableTable");
    HTable ht = TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    this.admin.disableTable(ht.getName());
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getZKTable().isDisabledTable(
            ht.getName()));

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      ok = true;
    }
    assertTrue(ok);
    this.admin.enableTable(table);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getZKTable().isEnabledTable(
            ht.getName()));

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
    final byte [] table1 = Bytes.toBytes("testDisableAndEnableTable1");
    final byte [] table2 = Bytes.toBytes("testDisableAndEnableTable2");
    HTable ht1 = TEST_UTIL.createTable(table1, HConstants.CATALOG_FAMILY);
    HTable ht2 = TEST_UTIL.createTable(table2, HConstants.CATALOG_FAMILY);
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
    TEST_UTIL.createTable(Bytes.toBytes("testCreateTable"),
      HConstants.CATALOG_FAMILY).close();
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getZKTable().isEnabledTable(
            TableName.valueOf("testCreateTable")));
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
    HTable table = new HTable(TEST_UTIL.getConfiguration(), "myTestTable");
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
    final byte[] tableName = Bytes.toBytes("changeTableSchemaOnlineFailure");
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

  /**
   * Listens for when an event is done in Master.
   */
  static class DoneListener implements EventHandler.EventHandlerListener {
    private final AtomicBoolean done;

    DoneListener(final AtomicBoolean done) {
      super();
      this.done = done;
    }

    @Override
    public void afterProcess(EventHandler event) {
      this.done.set(true);
      synchronized (this.done) {
        // Wake anyone waiting on this value to change.
        this.done.notifyAll();
      }
    }

    @Override
    public void beforeProcess(EventHandler event) {
      // continue
    }
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
    float average = (float) expectedRegions/numRS;
    int min = (int)Math.floor(average);
    int max = (int)Math.ceil(average);
    for (List<HRegionInfo> regionList : server2Regions.values()) {
      assertTrue(regionList.size() == min || regionList.size() == max);
    }
  }

  @Test (timeout=300000)
  public void testCreateTableNumberOfRegions() throws IOException, InterruptedException {
    byte[] tableName = Bytes.toBytes("testCreateTableNumberOfRegions");
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    assertEquals("Table should have only 1 region", 1, regions.size());
    ht.close();

    byte [] TABLE_2 = Bytes.add(tableName, Bytes.toBytes("_2"));
    desc = new HTableDescriptor(TableName.valueOf(TABLE_2));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, new byte[][]{new byte[]{42}});
    HTable ht2 = new HTable(TEST_UTIL.getConfiguration(), TABLE_2);
    regions = ht2.getRegionLocations();
    assertEquals("Table should have only 2 region", 2, regions.size());
    ht2.close();

    byte [] TABLE_3 = Bytes.add(tableName, Bytes.toBytes("_3"));
    desc = new HTableDescriptor(TableName.valueOf(TABLE_3));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, "a".getBytes(), "z".getBytes(), 3);
    HTable ht3 = new HTable(TEST_UTIL.getConfiguration(), TABLE_3);
    regions = ht3.getRegionLocations();
    assertEquals("Table should have only 3 region", 3, regions.size());
    ht3.close();

    byte [] TABLE_4 = Bytes.add(tableName, Bytes.toBytes("_4"));
    desc = new HTableDescriptor(TableName.valueOf(TABLE_4));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    try {
      admin.createTable(desc, "a".getBytes(), "z".getBytes(), 2);
      fail("Should not be able to create a table with only 2 regions using this API.");
    } catch (IllegalArgumentException eae) {
    // Expected
    }

    byte [] TABLE_5 = Bytes.add(tableName, Bytes.toBytes("_5"));
    desc = new HTableDescriptor(TableName.valueOf(TABLE_5));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, new byte[] {1}, new byte[] {127}, 16);
    HTable ht5 = new HTable(TEST_UTIL.getConfiguration(), TABLE_5);
    regions = ht5.getRegionLocations();
    assertEquals("Table should have 16 region", 16, regions.size());
    ht5.close();
  }

  @Test (timeout=300000)
  public void testCreateTableWithRegions() throws IOException, InterruptedException {

    byte[] tableName = Bytes.toBytes("testCreateTableWithRegions");

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

    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    boolean tableAvailable = admin.isTableAvailable(Bytes.toString(tableName), splitKeys);
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

    byte [] TABLE_2 = Bytes.add(tableName, Bytes.toBytes("_2"));

    desc = new HTableDescriptor(TableName.valueOf(TABLE_2));
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

    byte [] TABLE_3 = Bytes.add(tableName, Bytes.toBytes("_3"));

    desc = new HTableDescriptor(TableName.valueOf(TABLE_3));
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

    byte [] TABLE_4 = Bytes.add(tableName, Bytes.toBytes("_4"));
    desc = new HTableDescriptor(TableName.valueOf(TABLE_4));
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    HBaseAdmin ladmin = new HBaseAdmin(TEST_UTIL.getConfiguration());
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
    byte[] tableName = Bytes.toBytes("testTableAvailableWithRandomSplitKeys");
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor("col"));
    byte[][] splitKeys = new byte[1][];
    splitKeys = new byte [][] {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 }
    };
    admin.createTable(desc);
    boolean tableAvailable = admin.isTableAvailable(Bytes.toString(tableName), splitKeys);
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
    final byte [] table = Bytes.toBytes("testTableExist");
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
  @Test (timeout=300000)
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
    byte[] tableName = Bytes.toBytes("testEnableTableAssignment");
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
        new byte[] { 3, 3, 3 }, new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 },
        new byte[] { 6, 6, 6 }, new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 },
        new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
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
  @Test (timeout=300000)
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
    this.admin.split(tableName.getName(), splitPoint);

    final AtomicInteger count = new AtomicInteger(0);
    Thread t = new Thread("CheckForSplit") {
      public void run() {
        for (int i = 0; i < 20; i++) {
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
    TableName tableName = TableName.valueOf("testEnableDisableAddColumnDeleteColumn");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!ZKTableReadOnly.isEnabledTable(zkw,
        TableName.valueOf("testEnableDisableAddColumnDeleteColumn"))) {
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
  public void testDeleteLastColumnFamily() throws Exception {
    TableName tableName = TableName.valueOf("testDeleteLastColumnFamily");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!this.admin.isTableEnabled(TableName.valueOf("testDeleteLastColumnFamily"))) {
      Thread.sleep(10);
    }

    // test for enabled table
    try {
      this.admin.deleteColumn(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    // test for disabled table
    this.admin.disableTable(tableName);

    try {
      this.admin.deleteColumn(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    this.admin.deleteTable(tableName);
  }
}
