/**
 * Copyright 2013 The Apache Software Foundation
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestClientLocalScanner {
  private final static Log LOG = LogFactory.getLog(TestClientLocalScanner.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] FAMILY2 = Bytes.toBytes("testFamily2");
  private static int SLAVES = 3;

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

  @Test(timeout=180000)
  public void testCompareLocalScanToRemoteScan() throws IOException {
    byte [] name = Bytes.toBytes("testCompareLocalScanToRemoteScan");
    HTable t = TEST_UTIL.createTable(name, new byte[][] {FAMILY, FAMILY2}, 3,
        Bytes.toBytes("aaa"), Bytes.toBytes("yyy"), 25);
    HTable tmpTable = new HTable(TEST_UTIL.getConfiguration(), name);

    int rowCount = TEST_UTIL.loadTable(t, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY2);
    t.flushCommits();
    TEST_UTIL.flush(name);
    HBaseTestingUtility.assertRowCount(t, rowCount);
    Scan scan = getScan(100, 100, true, FAMILY);
    assertTrue(compareScanners(tmpTable.getScanner(scan),
        t.getLocalScanner(scan)));
    FileSystem fs = FileSystem.get(t.getConfiguration());
    String hardLinkFolder = TEST_UTIL.getConfiguration().get(
        HConstants.READ_ONLY_HARDLINKS_FOLDER,
        HConstants.READ_ONLY_HARDLINKS_FOLDER_DEFAULT);
    Path folder = new Path(hardLinkFolder);
    FileStatus[] files = fs.listStatus(folder);
    assertTrue(files != null && files.length == 0);
  }

  class ReadOnlyStoreCompactionInjectionHandler extends InjectionHandler {
    final byte[] tableName;
    private boolean done;
    ReadOnlyStoreCompactionInjectionHandler(byte[] tableName) {
      this.tableName = tableName;
    }
    @Override
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.READONLYSTORE_COMPACTION_WHILE_SNAPSHOTTING && !done) {
        try {
          TEST_UTIL.getRSForFirstRegionInTable(tableName).compactSplitThread.
          requestCompaction(TEST_UTIL.getRSForFirstRegionInTable(tableName).
            getOnlineRegions().iterator().next(), "Testing");
          Thread.sleep(1000);
          done = true;
        } catch (IOException e) {
          LOG.error("Caught IOException " + e.getMessage());
        } catch (InterruptedException e) {
          LOG.error("Caught InterruptedException " + e.getMessage());
        }
      }
    }
  }

  /**
   * The compaction works as follows :
   *  1 files := List of files under a store path;
   *  2 while success :
   *  3   Loop over files:
   *  4     Create hardlink for file;
   *  5   if failed :
   *  6     success = false;
   *  7 success = true
   *
   * This test injects compaction in between the steps 1 and 2. Verified the
   * failure by observing the logs.
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testCompactionWhileSnapshotting() throws IOException {
    final byte [] name = Bytes.toBytes("testCompactionWhileSnapshotting");
    final HTable t = TEST_UTIL.createTable(name, FAMILY);
    final int numRows = 1000;
    final int storefiles = 10;
    int rowCnt = 0;
    for (int j=0; j<storefiles; j++) {
      for (int i=(j*numRows); i<((j+1)*numRows); i++) {
        byte[] row = Bytes.toBytes("row" + i);
        Put p = new Put(row);
        p.add(FAMILY, FAMILY, row);
        t.put(p);
      }
      t.flushCommits();
      TEST_UTIL.flush(name);
    }
    ReadOnlyStoreCompactionInjectionHandler ih =
        new ReadOnlyStoreCompactionInjectionHandler(name);
    InjectionHandler.set(ih);
    ResultScanner scanner = t.getLocalScanner(new Scan());
    for (@SuppressWarnings("unused") Result r : scanner) {
      rowCnt++;
    }
    InjectionHandler.clear();
    assertTrue(rowCnt == (storefiles * numRows));
  }

  /**
   * This is to test the local scanner working.
   * Using the same code as testAcrossMultipleRegions.
   * With a small difference that the data is flushed to disk
   * since the scan works only on the data present in files.
   */
  @Test(timeout=180000)
  public void testFilterAcrossMutlipleRegionsWithLocalScan() throws IOException {
    byte [] name = Bytes.toBytes("testFilterAcrossMutlipleRegionsWithLocalScan");
    HTable t = TEST_UTIL.createTable(name, FAMILY);
    int rowCount = TEST_UTIL.loadTable(t, FAMILY);
    t.flushCommits();
    TEST_UTIL.flush(name);

    HBaseTestingUtility.assertRowCount(t, rowCount);
    // Split the table.  Should split on a reasonable key; 'lqj'
    Map<HRegionInfo, HServerAddress> regions  = splitTable(t);
    HBaseTestingUtility.assertRowCount(t, rowCount);
    // Get end key of first region.
    byte [] endKey = regions.keySet().iterator().next().getEndKey();
    // Count rows with a filter that stops us before passed 'endKey'.
    // Should be count of rows in first region.
    int endKeyCount = countRows(t, t.getLocalScanner(createScanWithRowFilter(endKey)));
    assertTrue(endKeyCount < rowCount);

    // How do I know I did not got to second region?  Thats tough.  Can't really
    // do that in client-side region test.  I verified by tracing in debugger.
    // I changed the messages that come out when set to DEBUG so should see
    // when scanner is done. Says "Finished with scanning..." with region name.
    // Check that its finished in right region.

    // New test.  Make it so scan goes into next region by one and then two.
    // Make sure count comes out right.
    byte [] key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 1)};
    String defaultName = t.getConfiguration().get("fs.default.name");
    String rootdir = t.getConfiguration().get("hbase.rootdir");

    t.getConfiguration().set("fs.default.name", "");
    t.getConfiguration().set("hbase.rootdir", "");
    int plusOneCount = countRows(t, t.getLocalScanner(createScanWithRowFilter(key)));
    assertEquals(endKeyCount + 1, plusOneCount);
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 2)};
    int plusTwoCount = countRows(t, t.getLocalScanner(createScanWithRowFilter(key)));
    assertEquals(endKeyCount + 2, plusTwoCount);

    // New test.  Make it so I scan one less than endkey.
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] - 1)};
    int minusOneCount = countRows(t, t.getLocalScanner(createScanWithRowFilter(key)));
    assertEquals(endKeyCount - 1, minusOneCount);
    // For above test... study logs.  Make sure we do "Finished with scanning.."
    // in first region and that we do not fall into the next region.

    key = new byte [] {'a', 'a', 'a'};
    int countBBB = countRows(t,
      t.getLocalScanner(createScanWithRowFilter(key, null, CompareFilter.CompareOp.EQUAL)));
    assertEquals(1, countBBB);

    int countGreater = countRows(t,
      t.getLocalScanner(createScanWithRowFilter(endKey, null, CompareFilter.CompareOp.GREATER_OR_EQUAL)));
    // Because started at start of table.
    assertEquals(0, countGreater);
    countGreater = countRows(t, t.getLocalScanner(createScanWithRowFilter(endKey, endKey,
      CompareFilter.CompareOp.GREATER_OR_EQUAL)));
    assertEquals(rowCount - endKeyCount, countGreater);

    t.getConfiguration().set("fs.default.name", defaultName);
    t.getConfiguration().set("hbase.rootdir", rootdir);
    t.close();
  }

  @Test(timeout=180000)
  public void testInconsistentRegionDirectories() throws IOException {
    byte [] tableName = Bytes.toBytes("testInconsistentRegionDirectories");
    String rootDir = TEST_UTIL.getConfiguration().get("hbase.rootdir");
    String tmpPath = "/tmp/testInconsistentRegionDirectories/";
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path p = new Path(tmpPath);
    fs.mkdirs(p);
    assertTrue(fs.listStatus(p).length == 0);
    TEST_UTIL.getConfiguration().set("hbase.rootdir", tmpPath);
    HTable t = TEST_UTIL.createTable(tableName, FAMILY);
    t.getConfiguration().setBoolean(HConstants.USE_CONF_FROM_SERVER, false);
    TEST_UTIL.loadTable(t, FAMILY);
    try {
      t.getLocalScanner(new Scan());
    } catch (Exception e) {
      LOG.debug("Exception", e);
      assertTrue(fs.listStatus(p).length == 0);
      return;
    } finally {
      TEST_UTIL.getConfiguration().set("hbase.rootdir", rootDir);
      t.getConfiguration().setBoolean(HConstants.USE_CONF_FROM_SERVER, false);
      t.close();
    }
    assertTrue(false);
  }

  //Marked as unstable and recored in t3864238
  @Test(timeout=180000)
  @Category(org.apache.hadoop.hbase.UnstableTests.class)
  public void testLocalScannerWithoutHardlinks() throws IOException {
    byte [] tableName = Bytes.toBytes("testLocalScannerWithoutHardlinks");
    HTable t = TEST_UTIL.createTable(tableName, FAMILY);
    HTable tmpTable = new HTable(TEST_UTIL.getConfiguration(), tableName);
    int rowCount = TEST_UTIL.loadTable(t, FAMILY);
    t.flushCommits();
    TEST_UTIL.flush(tableName);
    HBaseTestingUtility.assertRowCount(t, rowCount);

    Scan scan = getScan(100, 100, true, FAMILY);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    String hardLinkFolder = TEST_UTIL.getConfiguration().get(
        HConstants.READ_ONLY_HARDLINKS_FOLDER,
        HConstants.READ_ONLY_HARDLINKS_FOLDER_DEFAULT);
    Path folder = new Path(hardLinkFolder);

    ResultScanner scanner = t.getLocalScanner(scan);
    assertTrue(fs.exists(folder));
    assertTrue(fs.listStatus(folder).length > 0);
    scanner.close();

    Path rootdir = new Path(TEST_UTIL.getConfiguration().get("hbase.rootdir"));
    HRegionInfo info = t.getRegionsInfo().keySet().iterator().next();
    HColumnDescriptor family = info.getTableDesc().getFamily(FAMILY);
    Path tableDir = HTableDescriptor.getTableDir(rootdir, info.getTableDesc().getName());
    Path homedir = Store.getStoreHomedir(tableDir, info.getEncodedName(), family.getName());

    assertTrue(fs.exists(homedir));
    int files = fs.listStatus(homedir).length;

    scanner = t.getLocalScanner(scan, false);
    assertTrue(compareScanners(tmpTable.getScanner(scan),
        scanner));
    assertTrue(fs.exists(folder));
    assertTrue(fs.listStatus(folder).length <= 0);
    scanner.close();

    assertTrue(fs.exists(homedir));
    assertTrue(fs.listStatus(homedir).length == files);
    t.close();
    fs.close();
  }

  public Scan getScan(int caching, int batching,
      boolean serverPrefetching, byte[] family) {
    Scan scan = new Scan();
    scan.addFamily(family);
    scan.setServerPrefetching(serverPrefetching);
    scan.setCaching(caching);
    scan.setBatch(batching);
    return scan;
  }

  /**
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private Map<HRegionInfo, HServerAddress> waitOnSplit(final HTable t)
  throws IOException {
    Map<HRegionInfo, HServerAddress> regions = t.getRegionsInfo();
    int originalCount = regions.size();
    for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 30); i++) {
      Thread.currentThread();
      try {
        Thread.sleep(TEST_UTIL.getConfiguration().getInt("hbase.server.thread.wakefrequency", 1000));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      regions = t.getRegionsInfo();
      if (regions.size() > originalCount) break;
    }
    return regions;
  }

  /**
   * Split table into multiple regions.
   * @param t Table to split.
   * @return Map of regions to servers.
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> splitTable(final HTable t)
  throws IOException {
    // Split this table in two.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.split(t.getTableName());
    Map<HRegionInfo, HServerAddress> regions = waitOnSplit(t);
    assertTrue(regions.size() > 1);
    return regions;
  }

  /**
   * Compares the given scanners, iterates over the Results that are populated
   * and compares and verifies that they are equal.
   * @param scanner1
   * @param scanner2
   * @return
   * @throws IOException
   */
  private boolean compareScanners(final ResultScanner scanner1,
    final ResultScanner scanner2) throws IOException {
    // Assert all rows in table.
    while (true) {
      Result r1 = scanner1.next();
      Result r2 = scanner2.next();
      if (r1 == null || r2 == null) {
        if (r1 == null && r2 == null) {
          return true;
        }
        return false;
      }
      List<KeyValue> l1 = r1.list();
      List<KeyValue> l2 = r2.list();
      if (l1.size() != l2.size()) {
        return false;
      }
      int sz = l1.size();
      for (int i=0; i<sz; i++) {
        KeyValue kv1 = l1.get(i);
        KeyValue kv2 = l2.get(i);
        if (KeyValue.COMPARATOR.compare(kv1, kv2) != 0) {
          return false;
        }
      }
    }
  }

  /**
   * @param t
   * @param scanner
   * @return Count of rows in table.
   * @throws IOException
   */
  private int countRows(final HTable t, final ResultScanner scanner)
  throws IOException {
    // Assert all rows in table.
    int count = 0;
    for (Result result: scanner) {
      count++;
      assertTrue(result.size() > 0);
    }
    return count;
  }

  /**
   * @param key
   * @return Scan with RowFilter that does LESS than passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key) {
    return createScanWithRowFilter(key, null, CompareFilter.CompareOp.LESS);
  }

  /**
   * @param key
   * @param op
   * @param startRow
   * @return Scan with RowFilter that does CompareOp op on passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key,
      final byte [] startRow, CompareFilter.CompareOp op) {
    // Make sure key is of some substance... non-null and > than first key.
    assertTrue(key != null && key.length > 0 &&
      Bytes.BYTES_COMPARATOR.compare(key, new byte [] {'a', 'a', 'a'}) >= 0);
    LOG.info("Key=" + Bytes.toString(key));
    Scan s = startRow == null? new Scan(): new Scan(startRow);
    Filter f = new RowFilter(op, new BinaryComparator(key));
    f = new WhileMatchFilter(f);
    s.setFilter(f);
    return s;
  }
}
