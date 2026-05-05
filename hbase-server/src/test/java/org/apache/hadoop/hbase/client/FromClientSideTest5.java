/*
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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run tests that use the HBase clients; {@link Table}. Sets up the HBase mini cluster once at start
 * and runs through all client tests. Each creates a table named for the method and does its stuff
 * against that. Parameterized to run with different registry implementations.
 */
public class FromClientSideTest5 extends FromClientSideTestBase {

  protected FromClientSideTest5(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FromClientSideTest5.class);

  @TestTemplate
  public void testGetClosestRowBefore() throws IOException, InterruptedException {

    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    try (
      Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1, 1024);
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {

      // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
      // in Store.rowAtOrBeforeFromStoreFile
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      Put put1 = new Put(firstRow);
      Put put2 = new Put(secondRow);
      Put put3 = new Put(thirdRow);
      Put put4 = new Put(forthRow);
      byte[] one = new byte[] { 1 };
      byte[] two = new byte[] { 2 };
      byte[] three = new byte[] { 3 };
      byte[] four = new byte[] { 4 };

      put1.addColumn(HConstants.CATALOG_FAMILY, null, one);
      put2.addColumn(HConstants.CATALOG_FAMILY, null, two);
      put3.addColumn(HConstants.CATALOG_FAMILY, null, three);
      put4.addColumn(HConstants.CATALOG_FAMILY, null, four);
      table.put(put1);
      table.put(put2);
      table.put(put3);
      table.put(put4);
      region.flush(true);

      Result result;

      // Test before first that null is returned
      result = getReverseScanResult(table, beforeFirstRow);
      assertNull(result);

      // Test at first that first is returned
      result = getReverseScanResult(table, firstRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test in between first and second that first is returned
      result = getReverseScanResult(table, beforeSecondRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test at second make sure second is returned
      result = getReverseScanResult(table, secondRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test in second and third, make sure second is returned
      result = getReverseScanResult(table, beforeThirdRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test at third make sure third is returned
      result = getReverseScanResult(table, thirdRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test in third and forth, make sure third is returned
      result = getReverseScanResult(table, beforeForthRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test at forth make sure forth is returned
      result = getReverseScanResult(table, forthRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

      // Test after forth make sure forth is returned
      result = getReverseScanResult(table, Bytes.add(forthRow, one));
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));
    }
  }

  private Result getReverseScanResult(Table table, byte[] row) throws IOException {
    Scan scan = new Scan().withStartRow(row);
    scan.setReadType(ReadType.PREAD);
    scan.setReversed(true);
    scan.setCaching(1);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner scanner = table.getScanner(scan)) {
      return scanner.next();
    }
  }

  @TestTemplate
  public void testRowMutations() throws Exception {
    LOG.info("Starting testRowMutations");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"),
        Bytes.toBytes("c"), Bytes.toBytes("d") };

      // Test for Put operations
      RowMutations arm = new RowMutations(ROW);
      Put p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[0], VALUE);
      arm.add(p);
      Result r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertTrue(r.isEmpty());

      Get g = new Get(ROW);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      // Test for Put and Delete operations
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[1], VALUE);
      arm.add(p);
      Delete d = new Delete(ROW);
      d.addColumns(FAMILY, QUALIFIERS[0]);
      arm.add(d);
      // TODO: Trying mutateRow again. The batch was failing with a one try only.
      r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertTrue(r.isEmpty());

      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test for Increment and Append operations
      arm = new RowMutations(ROW);
      arm.add(Arrays.asList(new Put(ROW).addColumn(FAMILY, QUALIFIERS[0], VALUE),
        new Delete(ROW).addColumns(FAMILY, QUALIFIERS[1]),
        new Increment(ROW).addColumn(FAMILY, QUALIFIERS[2], 5L),
        new Append(ROW).addColumn(FAMILY, QUALIFIERS[3], Bytes.toBytes("abc"))));
      r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertEquals(5L, Bytes.toLong(r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals("abc", Bytes.toString(r.getValue(FAMILY, QUALIFIERS[3])));

      g = new Get(ROW);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[1]));
      assertEquals(5L, Bytes.toLong(r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals("abc", Bytes.toString(r.getValue(FAMILY, QUALIFIERS[3])));

      // Test that we get a region level exception
      RowMutations nceRm = new RowMutations(ROW);
      p = new Put(ROW);
      p.addColumn(new byte[] { 'b', 'o', 'g', 'u', 's' }, QUALIFIERS[0], VALUE);
      nceRm.add(p);
      Exception e = assertThrows(Exception.class, () -> t.mutateRow(nceRm),
        "Expected NoSuchColumnFamilyException");
      if (!(e instanceof NoSuchColumnFamilyException)) {
        assertThat(e, instanceOf(RetriesExhaustedWithDetailsException.class));
        List<Throwable> causes = ((RetriesExhaustedWithDetailsException) e).getCauses();
        assertThat(causes, hasItem(instanceOf(NoSuchColumnFamilyException.class)));
      }
    }
  }

  @TestTemplate
  public void testBatchAppendWithReturnResultFalse() throws Exception {
    LOG.info("Starting testBatchAppendWithReturnResultFalse");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Append append1 = new Append(Bytes.toBytes("row1"));
      append1.setReturnResults(false);
      append1.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value1"));
      Append append2 = new Append(Bytes.toBytes("row1"));
      append2.setReturnResults(false);
      append2.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value2"));
      List<Append> appends = new ArrayList<>();
      appends.add(append1);
      appends.add(append2);
      Object[] results = new Object[2];
      table.batch(appends, results);
      assertEquals(2, results.length);
      for (Object r : results) {
        Result result = (Result) r;
        assertTrue(result.isEmpty());
      }
    }
  }

  @TestTemplate
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      byte[] v1 = Bytes.toBytes("42");
      byte[] v2 = Bytes.toBytes("23");
      byte[][] QUALIFIERS =
        new byte[][] { Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c") };
      Append a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v1);
      a.addColumn(FAMILY, QUALIFIERS[1], v2);
      a.setReturnResults(false);
      assertEmptyResult(t.append(a));

      a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v2);
      a.addColumn(FAMILY, QUALIFIERS[1], v1);
      a.addColumn(FAMILY, QUALIFIERS[2], v2);
      Result r = t.append(a);
      assertEquals(0, Bytes.compareTo(Bytes.add(v1, v2), r.getValue(FAMILY, QUALIFIERS[0])));
      assertEquals(0, Bytes.compareTo(Bytes.add(v2, v1), r.getValue(FAMILY, QUALIFIERS[1])));
      // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
      assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals(r.getColumnLatestCell(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatestCell(FAMILY, QUALIFIERS[2]).getTimestamp());
    }
  }

  private List<Result> doAppend(final boolean walUsed) throws IOException {
    LOG.info("Starting testAppend, walUsed is " + walUsed);
    TableName tableName = TableName.valueOf(
      this.tableName.getNameAsString() + (walUsed ? "_testAppendWithWAL" : "testAppendWithoutWAL"));
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      final byte[] row1 = Bytes.toBytes("c");
      final byte[] row2 = Bytes.toBytes("b");
      final byte[] row3 = Bytes.toBytes("a");
      final byte[] qual = Bytes.toBytes("qual");
      Put put_0 = new Put(row2);
      put_0.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Put put_1 = new Put(row3);
      put_1.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Append append_0 = new Append(row1);
      append_0.addColumn(FAMILY, qual, Bytes.toBytes("i"));
      Append append_1 = new Append(row1);
      append_1.addColumn(FAMILY, qual, Bytes.toBytes("k"));
      Append append_2 = new Append(row1);
      append_2.addColumn(FAMILY, qual, Bytes.toBytes("e"));
      if (!walUsed) {
        append_2.setDurability(Durability.SKIP_WAL);
      }
      Append append_3 = new Append(row1);
      append_3.addColumn(FAMILY, qual, Bytes.toBytes("a"));
      Scan s = new Scan();
      s.setCaching(1);
      t.append(append_0);
      t.put(put_0);
      t.put(put_1);
      List<Result> results = new LinkedList<>();
      try (ResultScanner scanner = t.getScanner(s)) {
        // get one row(should be row3) from the scanner to make sure that we have send a request to
        // region server, which means we have already set the read point, so later we should not see
        // the new appended values.
        Result r = scanner.next();
        assertNotNull(r);
        results.add(r);
        t.append(append_1);
        t.append(append_2);
        t.append(append_3);
        for (;;) {
          r = scanner.next();
          if (r == null) {
            break;
          }
          results.add(r);
        }
      }
      return results;
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @TestTemplate
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i < resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j < resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertArrayEquals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneQualifier(cellWithWal),
          CellUtil.cloneQualifier(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal));
      }
    }
  }

  @TestTemplate
  public void testClientPoolRoundRobin() throws IOException {
    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, Integer.MAX_VALUE);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final long ts = EnvironmentEdgeManager.currentTime();
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(QUALIFIER);

        assertEquals(versions, navigableMap.size(), "The number of versions of '"
          + Bytes.toString(FAMILY) + ":" + Bytes.toString(QUALIFIER) + " did not match");
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue(Bytes.equals(VALUE, entry.getValue()),
            "The value at time " + entry.getKey() + " did not match what was put");
        }
      }
    }
  }

  @Disabled("Flakey: HBASE-8989")
  @TestTemplate
  public void testClientPoolThreadLocal() throws IOException {
    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 3);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final long ts = EnvironmentEdgeManager.currentTime();
      final Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(QUALIFIER);

        assertEquals(versions, navigableMap.size(), "The number of versions of '"
          + Bytes.toString(FAMILY) + ":" + Bytes.toString(QUALIFIER) + " did not match");
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue(Bytes.equals(VALUE, entry.getValue()),
            "The value at time " + entry.getKey() + " did not match what was put");
        }
      }

      final Object waitLock = new Object();
      ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
      final AtomicReference<AssertionError> error = new AtomicReference<>(null);
      for (int versions = numVersions; versions < numVersions * 2; versions++) {
        final int versionsCopy = versions;
        executorService.submit((Callable<Void>) () -> {
          try {
            Put put = new Put(ROW);
            put.addColumn(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(QUALIFIER);

            assertEquals(versionsCopy, navigableMap.size(),
              "The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy);
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue(Bytes.equals(VALUE, entry.getValue()),
                "The value at time " + entry.getKey() + " did not match what was put");
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception ignored) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e.toString(), e);
          }

          return null;
        });
      }
      synchronized (waitLock) {
        waitLock.notifyAll();
      }
      executorService.shutdownNow();
      assertNull(error.get());
    }
  }

  /**
   * Test ScanMetrics
   */
  @TestTemplate
  @SuppressWarnings({ "unused", "checkstyle:EmptyBlock" })
  public void testScanMetrics() throws Exception {
    // Set up test table:
    // Create table:
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      int numOfRegions;
      try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        numOfRegions = r.getStartKeys().length;
      }
      // Create 3 rows in the table, with rowkeys starting with "zzz*" so that
      // scan are forced to hit all the regions.
      Put put1 = new Put(Bytes.toBytes("zzz1"));
      put1.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put2 = new Put(Bytes.toBytes("zzz2"));
      put2.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put3 = new Put(Bytes.toBytes("zzz3"));
      put3.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(Arrays.asList(put1, put2, put3));

      Scan scan1 = new Scan();
      int numRecords = 0;
      try (ResultScanner scanner = ht.getScanner(scan1)) {
        for (Result result : scanner) {
          numRecords++;
        }

        LOG.info("test data has {} records.", numRecords);

        // by default, scan metrics collection is turned off
        assertNull(scanner.getScanMetrics());
      }

      // turn on scan metrics
      Scan scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(numRecords + 1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        for (Result result : scanner.next(numRecords - 1)) {
        }
        assertNotNull(scanner.getScanMetrics());
      }

      // set caching to 1, because metrics are collected in each roundtrip only
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        // per HBASE-5717, this should still collect even if you don't run all the way to
        // the end of the scanner. So this is asking for 2 of the 3 rows we inserted.
        for (Result result : scanner.next(numRecords - 1)) {
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals(numOfRegions, scanMetrics.countOfRegions.get(),
          "Did not access all the regions in the table");
      }

      // check byte counters
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals(numBytes, scanMetrics.countOfBytesInResults.get(),
          "Did not count the result bytes");
      }

      // check byte counters on a small scan
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      scan2.setReadType(ReadType.PREAD);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals(numBytes, scanMetrics.countOfBytesInResults.get(),
          "Did not count the result bytes");
      }

      // now, test that the metrics are still collected even if you don't call close, but do
      // run past the end of all the records
      /**
       * There seems to be a timing issue here. Comment out for now. Fix when time. Scan
       * scanWithoutClose = new Scan(); scanWithoutClose.setCaching(1);
       * scanWithoutClose.setScanMetricsEnabled(true); ResultScanner scannerWithoutClose =
       * ht.getScanner(scanWithoutClose); for (Result result : scannerWithoutClose.next(numRecords +
       * 1)) { } ScanMetrics scanMetricsWithoutClose = getScanMetrics(scanWithoutClose);
       * assertEquals("Did not access all the regions in the table", numOfRegions,
       * scanMetricsWithoutClose.countOfRegions.get());
       */

      // finally,
      // test that the metrics are collected correctly if you both run past all the records,
      // AND close the scanner
      Scan scanWithClose = new Scan();
      // make sure we can set caching up to the number of a scanned values
      scanWithClose.setCaching(numRecords);
      scanWithClose.setScanMetricsEnabled(true);
      try (ResultScanner scannerWithClose = ht.getScanner(scanWithClose)) {
        for (Result result : scannerWithClose.next(numRecords + 1)) {
        }
        scannerWithClose.close();
        ScanMetrics scanMetricsWithClose = scannerWithClose.getScanMetrics();
        assertEquals(numOfRegions, scanMetricsWithClose.countOfRegions.get(),
          "Did not access all the regions in the table");
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * Tests that cache on write works all the way up from the client-side. Performs inserts, flushes,
   * and compactions, verifying changes in the block cache along the way.
   */
  @TestTemplate
  public void testCacheOnWriteEvictOnClose() throws Exception {
    byte[] data = Bytes.toBytes("data");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName);
      RegionLocator locator = conn.getRegionLocator(tableName)) {
      // get the block cache and region
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();

      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

      // establish baseline stats
      long startBlockCount = cache.getBlockCount();
      long startBlockHits = cache.getStats().getHitCount();
      long startBlockMiss = cache.getStats().getMissCount();

      // wait till baseline is stable, (minimal 500 ms)
      for (int i = 0; i < 5; i++) {
        Thread.sleep(100);
        if (
          startBlockCount != cache.getBlockCount()
            || startBlockHits != cache.getStats().getHitCount()
            || startBlockMiss != cache.getStats().getMissCount()
        ) {
          startBlockCount = cache.getBlockCount();
          startBlockHits = cache.getStats().getHitCount();
          startBlockMiss = cache.getStats().getMissCount();
          i = -1;
        }
      }

      // insert data
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));

      // data was in memstore so don't expect any changes
      assertEquals(startBlockCount, cache.getBlockCount());
      assertEquals(startBlockHits, cache.getStats().getHitCount());
      assertEquals(startBlockMiss, cache.getStats().getMissCount());

      // flush the data
      LOG.debug("Flushing cache");
      region.flush(true);

      // expect two more blocks in cache - DATA and ROOT_INDEX
      // , no change in hits/misses
      long expectedBlockCount = startBlockCount + 2;
      long expectedBlockHits = startBlockHits;
      long expectedBlockMiss = startBlockMiss;
      assertEquals(expectedBlockCount, cache.getBlockCount());
      assertEquals(expectedBlockHits, cache.getStats().getHitCount());
      assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
      // read the data and expect same blocks, one new hit, no misses
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
      assertEquals(expectedBlockCount, cache.getBlockCount());
      assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
      assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
      // insert a second column, read the row, no new blocks, one new hit
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      byte[] data2 = Bytes.add(data, data);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      Result r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      assertEquals(expectedBlockCount, cache.getBlockCount());
      assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
      assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
      // flush, one new block
      LOG.info("Flushing cache");
      region.flush(true);

      // + 1 for Index Block, +1 for data block
      expectedBlockCount += 2;
      assertEquals(expectedBlockCount, cache.getBlockCount());
      assertEquals(expectedBlockHits, cache.getStats().getHitCount());
      assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
      // compact, net minus two blocks, two hits, no misses
      LOG.info("Compacting");
      assertEquals(2, store.getStorefilesCount());
      region.compact(true);
      store.closeAndArchiveCompactedFiles();
      waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
      assertEquals(1, store.getStorefilesCount());
      // evicted two data blocks and two index blocks and compaction does not cache new blocks
      expectedBlockCount = startBlockCount;
      assertEquals(expectedBlockCount, cache.getBlockCount());
      expectedBlockHits += 2;
      assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
      assertEquals(expectedBlockHits, cache.getStats().getHitCount());
      // read the row, this should be a cache miss because we don't cache data
      // blocks on compaction
      r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      expectedBlockCount += 1; // cached one data block
      assertEquals(expectedBlockCount, cache.getBlockCount());
      assertEquals(expectedBlockHits, cache.getStats().getHitCount());
      assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
    }
  }

  private void waitForStoreFileCount(HStore store, int count, int timeout)
    throws InterruptedException {
    await().atMost(Duration.ofMillis(timeout))
      .untilAsserted(() -> assertEquals(count, store.getStorefilesCount()));
  }

  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  @TestTemplate
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    byte[] family1 = Bytes.toBytes("f1");
    byte[] family2 = Bytes.toBytes("f2");
    TEST_UTIL.createTable(tableName, new byte[][] { family1, family2 }, 10);
    try (Connection conn = getConnection(); Table ignored = conn.getTable(tableName);
      Admin admin = conn.getAdmin();
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      List<HRegionLocation> allRegionLocations = locator.getAllRegionLocations();
      assertEquals(1, allRegionLocations.size());
      RegionInfo regionInfo = allRegionLocations.get(0).getRegion();
      ServerName addrBefore = allRegionLocations.get(0).getServerName();
      // Verify region location before move.
      HRegionLocation addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(), true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      // Make sure more than one server.
      if (TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() <= 1) {
        TEST_UTIL.getMiniHBaseCluster().startRegionServer();
        Waiter.waitFor(TEST_UTIL.getConfiguration(), 30000, new Waiter.Predicate<Exception>() {
          @Override
          public boolean evaluate() throws Exception {
            return TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() > 1;
          }
        });
      }

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i
          < TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size(); i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(), addr);
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }

  /**
   * Tests getRegionsInRange by creating some regions over which a range of keys spans; then
   * changing the key range.
   */
  @TestTemplate
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte[] startKey = Bytes.toBytes("ddc");
    byte[] endKey = Bytes.toBytes("mmm");
    TEST_UTIL.createMultiRegionTable(tableName, new byte[][] { FAMILY }, 10);

    int numOfRegions;
    try (Connection conn = getConnection(); RegionLocator r = conn.getRegionLocator(tableName)) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = getRegionsInRange(tableName, HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = getRegionsInRange(tableName, startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList =
      getRegionsInRange(tableName, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = Bytes.toBytes("ccc");
    endKey = Bytes.toBytes("ccc");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(1, regionsList.size());
  }

  private List<HRegionLocation> getRegionsInRange(TableName tableName, byte[] startKey,
    byte[] endKey) throws IOException {
    List<HRegionLocation> regionsInRange = new ArrayList<>();
    byte[] currentKey = startKey;
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    try (Connection conn = getConnection(); RegionLocator r = conn.getRegionLocator(tableName)) {
      do {
        HRegionLocation regionLocation = r.getRegionLocation(currentKey);
        regionsInRange.add(regionLocation);
        currentKey = regionLocation.getRegion().getEndKey();
      } while (
        !Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
          && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0)
      );
      return regionsInRange;
    }
  }

  @TestTemplate
  public void testJira6912() throws Exception {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 10);
    try (Connection conn = getConnection(); Table foo = conn.getTable(tableName)) {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i != 100; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILY, FAMILY, Bytes.toBytes(i));
        puts.add(put);
      }
      foo.put(puts);
      // If i comment this out it works
      TEST_UTIL.flush();

      Scan scan = new Scan();
      scan.withStartRow(Bytes.toBytes(1));
      scan.withStopRow(Bytes.toBytes(3));
      scan.addColumn(FAMILY, FAMILY);
      scan.setFilter(
        new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

      try (ResultScanner scanner = foo.getScanner(scan)) {
        Result[] bar = scanner.next(100);
        assertEquals(1, bar.length);
      }
    }
  }

  @TestTemplate
  public void testScanNullQualifier() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);
      LOG.info("Row put");

      Scan scan = new Scan();
      scan.addColumn(FAMILY, null);

      ResultScanner scanner = table.getScanner(scan);
      Result[] bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(1, bar[0].size());

      scan = new Scan();
      scan.addFamily(FAMILY);

      scanner = table.getScanner(scan);
      bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(2, bar[0].size());
    }
  }

  @TestTemplate
  public void testRawScanRespectsVersions() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      byte[] row = Bytes.toBytes("row");

      // put the same row 4 times, with different values
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 10, VALUE);
      table.put(p);
      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 11, ArrayUtils.add(VALUE, (byte) 2));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 12, ArrayUtils.add(VALUE, (byte) 3));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 13, ArrayUtils.add(VALUE, (byte) 4));
      table.put(p);

      int versions = 4;
      Scan s = new Scan().withStartRow(row);
      // get all the possible versions
      s.readAllVersions();
      s.setRaw(true);

      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals(versions, r.listCells().size(),
            "Found an unexpected number of results for the row!");
          count++;
        }
        assertEquals(1, count,
          "Found more than a single row when raw scanning the table with a single row!");
      }

      // then if we decrease the number of versions, but keep the scan raw, we should see exactly
      // that number of versions
      versions = 2;
      s.readVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals(versions, r.listCells().size(),
            "Found an unexpected number of results for the row!");
          count++;
        }
        assertEquals(1, count,
          "Found more than a single row when raw scanning the table with a single row!");
      }

      // finally, if we turn off raw scanning, but max out the number of versions, we should go back
      // to seeing just three
      versions = 3;
      s.readVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals(versions, r.listCells().size(),
            "Found an unexpected number of results for the row!");
          count++;
        }
        assertEquals(1, count,
          "Found more than a single row when raw scanning the table with a single row!");
      }

    }
    TEST_UTIL.deleteTable(tableName);
  }

  @TestTemplate
  public void testEmptyFilterList() throws Exception {
    // Test Initialization.
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      // Insert one row each region
      Put put = new Put(Bytes.toBytes("row"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      List<Result> scanResults = new LinkedList<>();
      Scan scan = new Scan();
      scan.setFilter(new FilterList());
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          scanResults.add(r);
        }
      }
      assertEquals(1, scanResults.size());
      Get g = new Get(Bytes.toBytes("row"));
      g.setFilter(new FilterList());
      Result getResult = table.get(g);
      Result scanResult = scanResults.get(0);
      assertEquals(scanResult.rawCells().length, getResult.rawCells().length);
      for (int i = 0; i != scanResult.rawCells().length; ++i) {
        Cell scanCell = scanResult.rawCells()[i];
        Cell getCell = getResult.rawCells()[i];
        assertEquals(0, Bytes.compareTo(CellUtil.cloneRow(scanCell), CellUtil.cloneRow(getCell)));
        assertEquals(0,
          Bytes.compareTo(CellUtil.cloneFamily(scanCell), CellUtil.cloneFamily(getCell)));
        assertEquals(0,
          Bytes.compareTo(CellUtil.cloneQualifier(scanCell), CellUtil.cloneQualifier(getCell)));
        assertEquals(0,
          Bytes.compareTo(CellUtil.cloneValue(scanCell), CellUtil.cloneValue(getCell)));
      }
    }
  }

  @TestTemplate
  public void testSmallScan() throws Exception {
    // Test Initialization.
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      // Insert one row each region
      int insertNum = 10;
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // normal scan
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // small scan
      Scan scan = new Scan().withStartRow(HConstants.EMPTY_START_ROW)
        .withStopRow(HConstants.EMPTY_END_ROW, true);
      scan.setReadType(ReadType.PREAD);
      scan.setCaching(2);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  @TestTemplate
  public void testFilterAllRecords() throws IOException {
    Scan scan = new Scan();
    scan.setBatch(1);
    scan.setCaching(1);
    // Filter out any records
    scan.setFilter(new FilterList(new FirstKeyOnlyFilter(), new InclusiveStopFilter(new byte[0])));
    try (Connection conn = getConnection();
      Table table = conn.getTable(TableName.META_TABLE_NAME)) {
      try (ResultScanner s = table.getScanner(scan)) {
        assertNull(s.next());
      }
    }
  }

  @TestTemplate
  public void testCellSizeLimit() throws IOException {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setValue(HRegion.HBASE_MAX_CELL_SIZE_KEY, Integer.toString(10 * 1024))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    try (Connection conn = getConnection()) {
      try (Admin admin = conn.getAdmin()) {
        admin.createTable(tableDescriptor);
      }
      try (Table t = conn.getTable(tableName)) {
        // Will succeed
        t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(0L)));
        t.increment(new Increment(ROW).addColumn(FAMILY, QUALIFIER, 1L));

        // Will succeed
        t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[9 * 1024]));

        // Will fail
        assertThrows(IOException.class,
          () -> t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[10 * 1024])),
          "Oversize cell failed to trigger exception");
        assertThrows(IOException.class,
          () -> t.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, new byte[2 * 1024])),
          "Oversize cell failed to trigger exception");
      }
    }
  }

  @TestTemplate
  public void testCellSizeNoLimit() throws IOException {

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setValue(HRegion.HBASE_MAX_CELL_SIZE_KEY, Integer.toString(0))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    try (Connection conn = getConnection()) {
      try (Admin admin = conn.getAdmin()) {
        admin.createTable(tableDescriptor);
      }
      // Will succeed
      try (Table ht = conn.getTable(tableName)) {
        ht.put(new Put(ROW).addColumn(FAMILY, QUALIFIER,
          new byte[HRegion.DEFAULT_MAX_CELL_SIZE - 1024]));
        ht.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, new byte[1024 + 1]));
      }
    }
  }

  @TestTemplate
  public void testDeleteSpecifiedVersionOfSpecifiedColumn() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 5);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t < 4; t++) {
        put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
      }
      ht.put(put);

      Delete delete = new Delete(ROW);
      // Delete version 3000 of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER, ts[2]);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[0], ts[1], ts[3] },
        new byte[][] { VALUES[0], VALUES[1], VALUES[3] }, 0, 2);

      delete = new Delete(ROW);
      // Delete a version 5000 of column FAMILY:QUALIFIER which didn't exist
      delete.addColumn(FAMILY, QUALIFIER, ts[4]);
      ht.delete(delete);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[0], ts[1], ts[3] },
        new byte[][] { VALUES[0], VALUES[1], VALUES[3] }, 0, 2);
    }
  }

  @TestTemplate
  public void testDeleteLatestVersionOfSpecifiedColumn() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 5);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t < 4; t++) {
        put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
      }
      ht.put(put);

      Delete delete = new Delete(ROW);
      // Delete latest version of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version 1000,2000,3000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[0], ts[1], ts[2] },
        new byte[][] { VALUES[0], VALUES[1], VALUES[2] }, 0, 2);

      delete = new Delete(ROW);
      // Delete two latest version of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER);
      delete.addColumn(FAMILY, QUALIFIER);
      ht.delete(delete);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[0] },
        new byte[][] { VALUES[0] }, 0, 0);

      put = new Put(ROW);
      // Put a version 5000 of column FAMILY:QUALIFIER
      put.addColumn(FAMILY, QUALIFIER, ts[4], VALUES[4]);
      ht.put(put);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000,5000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[0], ts[4] },
        new byte[][] { VALUES[0], VALUES[4] }, 0, 1);
    }
  }

  /**
   * Test for HBASE-17125
   */
  @TestTemplate
  public void testReadWithFilter() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 3);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      byte[] VALUEA = Bytes.toBytes("value-a");
      byte[] VALUEB = Bytes.toBytes("value-b");
      long[] ts = { 1000, 2000, 3000, 4000 };

      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t <= 3; t++) {
        if (t <= 1) {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEA);
        } else {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEB);
        }
      }
      table.put(put);

      Scan scan = new Scan()
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(3);
      Result result = getSingleScanResult(table, scan);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);

      Get get = new Get(ROW)
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(3);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);

      // Test with max versions 1, it should still read ts[1]
      scan = new Scan()
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(1);
      result = getSingleScanResult(table, scan);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);

      // Test with max versions 1, it should still read ts[1]
      get = new Get(ROW)
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(1);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);

      // Test with max versions 5, it should still read ts[1]
      scan = new Scan()
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(5);
      result = getSingleScanResult(table, scan);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);

      // Test with max versions 5, it should still read ts[1]
      get = new Get(ROW)
        .setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value-a")))
        .readVersions(5);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { ts[1] }, new byte[][] { VALUEA },
        0, 0);
    }
  }

  @TestTemplate
  public void testCellUtilTypeMethods() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final byte[] row = Bytes.toBytes("p");
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(p);

      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());
        Cell c = cs.current();
        assertTrue(CellUtil.isPut(c));
        assertFalse(CellUtil.isDelete(c));
        assertFalse(cs.advance());
        assertNull(scanner.next());
      }

      Delete d = new Delete(row);
      d.addColumn(FAMILY, QUALIFIER);
      table.delete(d);

      Scan scan = new Scan();
      scan.setRaw(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());

        // First cell should be the delete (masking the Put)
        Cell c = cs.current();
        assertTrue(CellUtil.isDelete(c), "Cell should be a Delete: " + c);
        assertFalse(CellUtil.isPut(c), "Cell should not be a Put: " + c);

        // Second cell should be the original Put
        assertTrue(cs.advance());
        c = cs.current();
        assertFalse(CellUtil.isDelete(c), "Cell should not be a Delete: " + c);
        assertTrue(CellUtil.isPut(c), "Cell should be a Put: " + c);

        // No more cells in this row
        assertFalse(cs.advance());

        // No more results in this scan
        assertNull(scanner.next());
      }
    }
  }

  @TestTemplate
  public void testCreateTableWithZeroRegionReplicas() throws Exception {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
      .setRegionReplication(0).build();

    try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {
      assertThrows(DoNotRetryIOException.class, () -> admin.createTable(desc));
    }
  }

  @TestTemplate
  public void testModifyTableWithZeroRegionReplicas() throws Exception {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf"))).build();
    TableDescriptor newDesc =
      TableDescriptorBuilder.newBuilder(desc).setRegionReplication(0).build();
    try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {
      admin.createTable(desc);
      assertThrows(DoNotRetryIOException.class, () -> admin.modifyTable(newDesc));
    }
  }

  @TestTemplate
  public void testModifyTableWithMemstoreData() throws Exception {
    createTableAndValidateTableSchemaModification(tableName, true);
  }

  @TestTemplate
  public void testDeleteCFWithMemstoreData() throws Exception {
    createTableAndValidateTableSchemaModification(tableName, false);
  }

  /**
   * Create table and validate online schema modification
   * @param tableName   Table name
   * @param modifyTable Modify table if true otherwise delete column family
   * @throws IOException in case of failures
   */
  private void createTableAndValidateTableSchemaModification(TableName tableName,
    boolean modifyTable) throws Exception {
    try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {
      // Create table with two Cfs
      byte[] cf1 = Bytes.toBytes("cf1");
      byte[] cf2 = Bytes.toBytes("cf2");
      TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2)).build();
      admin.createTable(tableDesc);

      Table t = TEST_UTIL.getConnection().getTable(tableName);
      // Insert few records and flush the table
      t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val1")));
      t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));
      admin.flush(tableName);
      Path tableDir = CommonFSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), tableName);
      List<Path> regionDirs = FSUtils.getRegionDirs(TEST_UTIL.getTestFileSystem(), tableDir);
      assertEquals(1, regionDirs.size());
      List<Path> familyDirs =
        FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
      assertEquals(2, familyDirs.size());

      // Insert record but dont flush the table
      t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val2")));
      t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));

      if (modifyTable) {
        tableDesc = TableDescriptorBuilder.newBuilder(tableDesc).removeColumnFamily(cf2).build();
        admin.modifyTable(tableDesc);
      } else {
        admin.deleteColumnFamily(tableName, cf2);
      }
      // After table modification or delete family there should be only one CF in FS
      familyDirs = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
      assertEquals(1, familyDirs.size(), "CF dir count should be 1, but was " + familyDirs.size());
    }
  }
}
