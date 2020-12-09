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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;

/**
 * Class to test HBaseAdmin. Spins up the minicluster once at test start and then takes it down
 * afterward. Add any testing of HBaseAdmin functionality here.
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestAdmin1 extends TestAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAdmin1.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAdmin1.class);

  @Test
  public void testCompactRegionWithTableName() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName).
        setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1")).build();
      ADMIN.createTable(htd);
      Region metaRegion = null;
      for (int i = 0; i < NB_SERVERS; i++) {
        HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i);
        List<HRegion> onlineRegions = rs.getRegions(TableName.META_TABLE_NAME);
        if (!onlineRegions.isEmpty()) {
          metaRegion = onlineRegions.get(0);
          break;
        }
      }

      long metaReadCountBeforeCompact = metaRegion.getReadRequestsCount();
      try {
        ADMIN.majorCompactRegion(tableName.getName());
      } catch (IllegalArgumentException iae) {
        LOG.info("This is expected");
      }
      assertEquals(metaReadCountBeforeCompact, metaRegion.getReadRequestsCount());
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitFlushCompactUnknownTable() throws InterruptedException {
    final TableName unknowntable = TableName.valueOf(name.getMethodName());
    Exception exception = null;
    try {
      ADMIN.compact(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.flush(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.split(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);
  }

  @Test
  public void testCompactionTimestamps() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1")).build();
    ADMIN.createTable(htd);
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    long ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    assertEquals(0, ts);
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    // no files written -> no data
    assertEquals(0, ts);

    ADMIN.flush(tableName);
    ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    // still 0, we flushed a file, but no major compaction happened
    assertEquals(0, ts);

    byte[] regionName;
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      regionName = l.getAllRegionLocations().get(0).getRegion().getRegionName();
    }
    long ts1 = ADMIN.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    ADMIN.flush(tableName);
    ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    // make sure the region API returns the same value, as the old file is still around
    assertEquals(ts1, ts);

    TEST_UTIL.compact(tableName, true);
    table.put(p);
    // forces a wait for the compaction
    ADMIN.flush(tableName);
    ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    // after a compaction our earliest timestamp will have progressed forward
    assertTrue(ts > ts1);

    // region api still the same
    ts1 = ADMIN.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    table.put(p);
    ADMIN.flush(tableName);
    ts = ADMIN.getLastMajorCompactionTimestamp(tableName);
    assertEquals(ts, ts1);
    table.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnValidName() {
    ColumnFamilyDescriptorBuilder.of("\\test\\abc");
  }

  @Test
  public void testTableExist() throws IOException {
    final TableName table = TableName.valueOf(name.getMethodName());
    boolean exist;
    exist = ADMIN.tableExists(table);
    assertEquals(false, exist);
    TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    exist = ADMIN.tableExists(table);
    assertEquals(true, exist);
  }

  /**
   * Tests forcing split from client and having scanners successfully ride over split.
   */
  @Test
  public void testForceSplit() throws Exception {
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf") };
    int[] rowCounts = new int[] { 6000 };
    int numVersions = ColumnFamilyDescriptorBuilder.DEFAULT_MAX_VERSIONS;
    int blockSize = 256;
    splitTest(null, familyNames, rowCounts, numVersions, blockSize, true);

    byte[] splitKey = Bytes.toBytes(3500);
    splitTest(splitKey, familyNames, rowCounts, numVersions, blockSize, true);
    // test regionSplitSync
    splitTest(splitKey, familyNames, rowCounts, numVersions, blockSize, false);
  }

  /**
   * Multi-family scenario. Tests forcing split from client and having scanners successfully ride
   * over split.
   */
  @Test
  public void testForceSplitMultiFamily() throws Exception {
    int numVersions = ColumnFamilyDescriptorBuilder.DEFAULT_MAX_VERSIONS;

    // use small HFile block size so that we can have lots of blocks in HFile
    // Otherwise, if there is only one block,
    // HFileBlockIndex.midKey()'s value == startKey
    int blockSize = 256;
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf1"), Bytes.toBytes("cf2") };

    // one of the column families isn't splittable
    int[] rowCounts = new int[] { 6000, 1 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize, true);

    rowCounts = new int[] { 1, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize, true);

    // one column family has much smaller data than the other
    // the split key should be based on the largest column family
    rowCounts = new int[] { 6000, 300 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize, true);

    rowCounts = new int[] { 300, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize, true);
  }

  private int count(ResultScanner scanner) throws IOException {
    int rows = 0;
    while (scanner.next() != null) {
      rows++;
    }
    return rows;
  }

  private void splitTest(byte[] splitPoint, byte[][] familyNames, int[] rowCounts, int numVersions,
      int blockSize, boolean async) throws Exception {
    TableName tableName = TableName.valueOf("testForceSplit");
    StringBuilder sb = new StringBuilder();
    // Add tail to String so can see better in logs where a test is running.
    for (int i = 0; i < rowCounts.length; i++) {
      sb.append("_").append(Integer.toString(rowCounts[i]));
    }
    assertFalse(ADMIN.tableExists(tableName));
    try (final Table table = TEST_UTIL.createTable(tableName, familyNames, numVersions, blockSize);
        final RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {

      int rowCount = 0;
      byte[] q = new byte[0];

      // insert rows into column families. The number of rows that have values
      // in a specific column family is decided by rowCounts[familyIndex]
      for (int index = 0; index < familyNames.length; index++) {
        ArrayList<Put> puts = new ArrayList<>(rowCounts[index]);
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
      int rows;
      try (ResultScanner scanner = table.getScanner(scan)) {
        rows = count(scanner);
      }
      assertEquals(rowCount, rows);

      // Have an outstanding scan going on to make sure we can scan over splits.
      scan = new Scan();
      try (ResultScanner scanner = table.getScanner(scan)) {
        // Scan first row so we are into first region before split happens.
        scanner.next();

        // Split the table
        if (async) {
          ADMIN.split(tableName, splitPoint);
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
                  LOG.warn("get location failed", e);
                }
                if (regions == null) {
                  continue;
                }
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
        } else {
          // Sync split region, no need to create a thread to check
          ADMIN.splitRegionAsync(m.get(0).getRegion().getRegionName(), splitPoint).get();
        }
        // Verify row count
        rows = 1 + count(scanner); // We counted one row above.
      }
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
          Bytes.toString(regions.get(0).getRegion().getEndKey()));
        assertEquals(Bytes.toString(splitPoint),
          Bytes.toString(regions.get(1).getRegion().getStartKey()));
        LOG.debug("Properly split on " + Bytes.toString(splitPoint));
      } else {
        if (familyNames.length > 1) {
          int splitKey = Bytes.toInt(regions.get(0).getRegion().getEndKey());
          // check if splitKey is based on the largest column family
          // in terms of it store size
          int deltaForLargestFamily = Math.abs(rowCount / 2 - splitKey);
          LOG.debug("SplitKey=" + splitKey + "&deltaForLargestFamily=" + deltaForLargestFamily +
            ", r=" + regions.get(0).getRegion());
          for (int index = 0; index < familyNames.length; index++) {
            int delta = Math.abs(rowCounts[index] / 2 - splitKey);
            if (delta < deltaForLargestFamily) {
              assertTrue("Delta " + delta + " for family " + index + " should be at least " +
                "deltaForLargestFamily " + deltaForLargestFamily, false);
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
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] cf = Bytes.toBytes("f");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(3)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf)).build();
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[] { (byte) '4' };
    splitRows[1] = new byte[] { (byte) '7' };
    TEST_UTIL.getAdmin().createTable(desc, splitRows);
    List<HRegion> oldRegions;
    do {
      oldRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
      Thread.sleep(10);
    } while (oldRegions.size() != 9); // 3 regions * 3 replicas
    // write some data to the table
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<>();
    byte[] qualifier = Bytes.toBytes("c");
    Put put = new Put(new byte[] { (byte) '1' });
    put.addColumn(cf, qualifier, Bytes.toBytes("100"));
    puts.add(put);
    put = new Put(new byte[] { (byte) '6' });
    put.addColumn(cf, qualifier, Bytes.toBytes("100"));
    puts.add(put);
    put = new Put(new byte[] { (byte) '8' });
    put.addColumn(cf, qualifier, Bytes.toBytes("100"));
    puts.add(put);
    ht.put(puts);
    ht.close();
    List<Pair<RegionInfo, ServerName>> regions =
      MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getConnection(), tableName);
    boolean gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via the split API . Should fail
    try {
      TEST_UTIL.getAdmin().splitRegionAsync(regions.get(1).getFirst().getRegionName()).get();
    } catch (IllegalArgumentException ex) {
      gotException = true;
    }
    assertTrue(gotException);
    gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via a different split API (the difference is
    // this API goes direct to the regionserver skipping any checks in the admin). Should fail
    try {
      TEST_UTIL.getHBaseAdmin().splitRegionAsync(regions.get(1).getFirst(),
        new byte[] { (byte) '1' });
    } catch (IOException ex) {
      gotException = true;
    }
    assertTrue(gotException);

    gotException = false;
    // testing Sync split operation
    try {
      TEST_UTIL.getAdmin()
        .splitRegionAsync(regions.get(1).getFirst().getRegionName(), new byte[] { (byte) '1' })
        .get();
    } catch (IllegalArgumentException ex) {
      gotException = true;
    }
    assertTrue(gotException);

    gotException = false;
    // Try merging a replica with another. Should fail.
    try {
      TEST_UTIL.getAdmin().mergeRegionsAsync(regions.get(1).getFirst().getEncodedNameAsBytes(),
        regions.get(2).getFirst().getEncodedNameAsBytes(), true).get();
    } catch (IllegalArgumentException m) {
      gotException = true;
    }
    assertTrue(gotException);
    // Try going to the master directly (that will skip the check in admin)
    try {
      byte[][] nameofRegionsToMerge = new byte[2][];
      nameofRegionsToMerge[0] = regions.get(1).getFirst().getEncodedNameAsBytes();
      nameofRegionsToMerge[1] = regions.get(2).getFirst().getEncodedNameAsBytes();
      MergeTableRegionsRequest request = RequestConverter.buildMergeTableRegionsRequest(
        nameofRegionsToMerge, true, HConstants.NO_NONCE, HConstants.NO_NONCE);
      ((ClusterConnection) TEST_UTIL.getAdmin().getConnection()).getMaster().mergeTableRegions(null,
        request);
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException m) {
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
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidColumnDescriptor() throws IOException {
    ColumnFamilyDescriptorBuilder.of("/cfamily/name");
  }

  /**
   * Test DFS replication for column families, where one CF has default replication(3) and the other
   * is set to 1.
   */
  @Test
  public void testHFileReplication() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());
    String fn1 = "rep1";
    String fn = "defaultRep";
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fn))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(fn1))
        .setDFSReplication((short) 1).build())
      .build();
    Table table = TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(tableName);
    Put p = new Put(Bytes.toBytes("defaultRep_rk"));
    byte[] q1 = Bytes.toBytes("q1");
    byte[] v1 = Bytes.toBytes("v1");
    p.addColumn(Bytes.toBytes(fn), q1, v1);
    List<Put> puts = new ArrayList<>(2);
    puts.add(p);
    p = new Put(Bytes.toBytes("rep1_rk"));
    p.addColumn(Bytes.toBytes(fn1), q1, v1);
    puts.add(p);
    try {
      table.put(puts);
      ADMIN.flush(tableName);

      List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(tableName);
      for (HRegion r : regions) {
        HStore store = r.getStore(Bytes.toBytes(fn));
        for (HStoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn));
          assertTrue("Column family " + fn + " should have 3 copies",
            CommonFSUtils.getDefaultReplication(TEST_UTIL.getTestFileSystem(),
              sf.getPath()) == (sf.getFileInfo().getFileStatus().getReplication()));
        }

        store = r.getStore(Bytes.toBytes(fn1));
        for (HStoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn1));
          assertTrue("Column family " + fn1 + " should have only 1 copy",
            1 == sf.getFileInfo().getFileStatus().getReplication());
        }
      }
    } finally {
      if (ADMIN.isTableEnabled(tableName)) {
        ADMIN.disableTable(tableName);
        ADMIN.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("d")).build();
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[] { (byte) '3' };
    splitRows[1] = new byte[] { (byte) '6' };
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<RegionInfo> tableRegions;
      RegionInfo regionA;
      RegionInfo regionB;
      RegionInfo regionC;
      RegionInfo mergedChildRegion = null;

      // merge with full name
      tableRegions = ADMIN.getRegions(tableName);
      assertEquals(3, tableRegions.size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      regionC = tableRegions.get(2);
      // TODO convert this to version that is synchronous (See HBASE-16668)
      ADMIN.mergeRegionsAsync(regionA.getRegionName(), regionB.getRegionName(),
        false).get(60, TimeUnit.SECONDS);

      tableRegions = ADMIN.getRegions(tableName);

      assertEquals(2, tableRegions.size());
      for (RegionInfo ri : tableRegions) {
        if (regionC.compareTo(ri) != 0) {
          mergedChildRegion = ri;
          break;
        }
      }

      assertNotNull(mergedChildRegion);
      // Need to wait GC for merged child region is done.
      HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
      CatalogJanitor cj = services.getCatalogJanitor();
      assertTrue(cj.scan() > 0);
      // Wait until all procedures settled down
      while (!services.getMasterProcedureExecutor().getActiveProcIds().isEmpty()) {
        Thread.sleep(200);
      }

      // TODO convert this to version that is synchronous (See HBASE-16668)
      ADMIN.mergeRegionsAsync(regionC.getEncodedNameAsBytes(),
        mergedChildRegion.getEncodedNameAsBytes(), false)
        .get(60, TimeUnit.SECONDS);

      assertEquals(1, ADMIN.getRegions(tableName).size());
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  @Test
  public void testMergeRegionsInvalidRegionCount()
      throws IOException, InterruptedException, ExecutionException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("d")).build();
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[] { (byte) '3' };
    splitRows[1] = new byte[] { (byte) '6' };
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<RegionInfo> tableRegions = ADMIN.getRegions(tableName);
      // 0
      try {
        ADMIN.mergeRegionsAsync(new byte[0][0], false).get();
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }
      // 1
      try {
        ADMIN.mergeRegionsAsync(new byte[][] { tableRegions.get(0).getEncodedNameAsBytes() }, false)
          .get();
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitShouldNotHappenIfSplitIsDisabledForTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    Table table = TEST_UTIL.createTable(htd, null);
    for (int i = 0; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      byte[] q1 = Bytes.toBytes("q1");
      byte[] v1 = Bytes.toBytes("v1");
      p.addColumn(Bytes.toBytes("f"), q1, v1);
      table.put(p);
    }
    ADMIN.flush(tableName);
    try {
      ADMIN.split(tableName, Bytes.toBytes("row5"));
      Threads.sleep(10000);
    } catch (Exception e) {
      // Nothing to do.
    }
    // Split should not happen.
    List<RegionInfo> allRegions =
      MetaTableAccessor.getTableRegions(ADMIN.getConnection(), tableName, true);
    assertEquals(1, allRegions.size());
  }
}
