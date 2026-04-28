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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Run tests that use the HBase clients; {@link Table}. Sets up the HBase mini cluster once at start
 * and runs through all client tests. Each creates a table named for the method and does its stuff
 * against that. Parameterized to run with different registry implementations.
 */
public class FromClientSideTest4 extends FromClientSideTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(FromClientSideTest4.class);

  protected FromClientSideTest4(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  /**
   * Test batch operations with combination of valid and invalid args
   */
  @TestTemplate
  public void testBatchOperationsWithErrors() throws Exception {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 10);
    try (Connection conn = getConnection(); Table foo = conn.getTable(tableName)) {
      int NUM_OPS = 100;

      // 1.1 Put with no column families (local validation, runtime exception)
      List<Put> puts = new ArrayList<>(NUM_OPS);
      for (int i = 0; i != NUM_OPS; i++) {
        Put put = new Put(Bytes.toBytes(i));
        puts.add(put);
      }
      assertThrows(IllegalArgumentException.class, () -> foo.put(puts));
      assertEquals(NUM_OPS, puts.size());

      // 1.2 Put with invalid column family
      puts.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY, Bytes.toBytes(i));
        puts.add(put);
      }

      RetriesExhaustedException ree =
        assertThrows(RetriesExhaustedException.class, () -> foo.put(puts));
      if (ree instanceof RetriesExhaustedWithDetailsException) {
        assertThat(((RetriesExhaustedWithDetailsException) ree).exceptions.get(0),
          instanceOf(NoSuchColumnFamilyException.class));
      } else {
        assertThat(ree.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // 2.1 Get non-existent rows
      List<Get> gets = new ArrayList<>(NUM_OPS);
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        gets.add(get);
      }
      Result[] getsResult = foo.get(gets);
      assertNotNull(getsResult);
      assertEquals(NUM_OPS, getsResult.length);
      for (int i = 0; i < NUM_OPS; i++) {
        Result getResult = getsResult[i];
        if (i % 2 == 0) {
          assertFalse(getResult.isEmpty());
        } else {
          assertTrue(getResult.isEmpty());
        }
      }

      // 2.2 Get with invalid column family
      gets.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        get.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY);
        gets.add(get);
      }
      ree = assertThrows(RetriesExhaustedException.class, () -> foo.get(gets));
      if (ree instanceof RetriesExhaustedWithDetailsException) {
        assertThat(((RetriesExhaustedWithDetailsException) ree).exceptions.get(0),
          instanceOf(NoSuchColumnFamilyException.class));
      } else {
        assertThat(ree.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // 3.1 Delete with invalid column family
      List<Delete> deletes = new ArrayList<>(NUM_OPS);
      for (int i = 0; i < NUM_OPS; i++) {
        Delete delete = new Delete(Bytes.toBytes(i));
        delete.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY);
        deletes.add(delete);
      }
      ree = assertThrows(RetriesExhaustedException.class, () -> foo.delete(deletes));
      if (ree instanceof RetriesExhaustedWithDetailsException) {
        assertThat(((RetriesExhaustedWithDetailsException) ree).exceptions.get(0),
          instanceOf(NoSuchColumnFamilyException.class));
      } else {
        assertThat(ree.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // all valid rows should have been deleted
      gets.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        gets.add(get);
      }
      getsResult = foo.get(gets);
      assertNotNull(getsResult);
      assertEquals(NUM_OPS, getsResult.length);
      for (Result getResult : getsResult) {
        assertTrue(getResult.isEmpty());
      }

      // 3.2 Delete non-existent rows
      deletes.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Delete delete = new Delete(Bytes.toBytes(i));
        deletes.add(delete);
      }
      foo.delete(deletes);
    }
  }

  //
  // JIRA Testers
  //

  /**
   * HBASE-867 If millions of columns in a column family, hbase scanner won't come up Test will
   * create numRows rows, each with numColsPerRow columns (1 version each), and attempt to scan them
   * all. To test at scale, up numColsPerRow to the millions (have not gotten that to work running
   * as junit though)
   */
  @TestTemplate
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte[][] ROWS = makeN(ROW, numRows);
    byte[][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert rows
      for (int i = 0; i < numRows; i++) {
        Put put = new Put(ROWS[i]);
        put.setDurability(Durability.SKIP_WAL);
        for (int j = 0; j < numColsPerRow; j++) {
          put.addColumn(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
        }
        assertEquals(put.size(), numColsPerRow, "Put expected to contain " + numColsPerRow
          + " columns but " + "only contains " + put.size());
        ht.put(put);
      }

      // Get a row
      Get get = new Get(ROWS[numRows - 1]);
      Result result = ht.get(get);
      assertNumKeys(result, numColsPerRow);
      Cell[] keys = result.rawCells();
      for (int i = 0; i < result.size(); i++) {
        assertKey(keys[i], ROWS[numRows - 1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }

      // Scan the rows
      Scan scan = new Scan();
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int rowCount = 0;
        while ((result = scanner.next()) != null) {
          assertNumKeys(result, numColsPerRow);
          Cell[] kvs = result.rawCells();
          for (int i = 0; i < numColsPerRow; i++) {
            assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
          }
          rowCount++;
        }
        assertEquals(rowCount, numRows,
          "Expected to scan " + numRows + " rows but actually scanned " + rowCount + " rows");
      }

      // flush and try again

      TEST_UTIL.flush();

      // Get a row
      get = new Get(ROWS[numRows - 1]);
      result = ht.get(get);
      assertNumKeys(result, numColsPerRow);
      keys = result.rawCells();
      for (int i = 0; i < result.size(); i++) {
        assertKey(keys[i], ROWS[numRows - 1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }

      // Scan the rows
      scan = new Scan();
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int rowCount = 0;
        while ((result = scanner.next()) != null) {
          assertNumKeys(result, numColsPerRow);
          Cell[] kvs = result.rawCells();
          for (int i = 0; i < numColsPerRow; i++) {
            assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
          }
          rowCount++;
        }
        assertEquals(rowCount, numRows,
          "Expected to scan " + numRows + " rows but actually scanned " + rowCount + " rows");
      }
    }
  }

  /**
   * HBASE-861 get with timestamp will return a value if there is a version with an earlier
   * timestamp
   */
  @TestTemplate
  public void testJiraTest861() throws Exception {
    byte[][] VALUES = makeNAscii(VALUE, 7);
    long[] STAMPS = makeStamps(7);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert three versions
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      ht.put(put);

      // Get the middle value
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

      // Try to get one version before (expect fail)
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

      // Try to get one version after (expect fail)
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

      // Try same from storefile
      TEST_UTIL.flush();
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

      // Insert two more versions surrounding others, into memstore
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
      ht.put(put);

      // Check we can get everything we should and can't get what we shouldn't
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

      // Try same from two storefiles
      TEST_UTIL.flush();
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    }
  }

  /**
   * HBASE-33 Add a HTable get/obtainScanner method that retrieves all versions of a particular
   * column and row between two timestamps
   */
  @TestTemplate
  public void testJiraTest33() throws Exception {
    byte[][] VALUES = makeNAscii(VALUE, 7);
    long[] STAMPS = makeStamps(7);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert lots versions
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      // Try same from storefile
      TEST_UTIL.flush();

      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);
    }
  }

  /**
   * HBASE-1014 commit(BatchUpdate) method should return timestamp
   */
  @TestTemplate
  public void testJiraTest1014() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      long manualStamp = 12345;

      // Insert lots versions
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, manualStamp, VALUE);
      ht.put(put);

      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp - 1);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp + 1);
    }
  }

  /**
   * HBASE-1182 Scan for columns > some timestamp
   */
  @TestTemplate
  public void testJiraTest1182() throws Exception {
    byte[][] VALUES = makeNAscii(VALUE, 7);
    long[] STAMPS = makeStamps(7);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert lots versions
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      // Try same from storefile
      TEST_UTIL.flush();

      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    }
  }

  /**
   * HBASE-52 Add a means of scanning over all versions
   */
  @TestTemplate
  public void testJiraTest52() throws Exception {
    byte[][] VALUES = makeNAscii(VALUE, 7);
    long[] STAMPS = makeStamps(7);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert lots versions
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      // Try same from storefile
      TEST_UTIL.flush();

      getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    }
  }

  @TestTemplate
  @SuppressWarnings("checkstyle:MethodLength")
  public void testDuplicateVersions() throws Exception {
    long[] STAMPS = makeStamps(20);
    byte[][] VALUES = makeNAscii(VALUE, 20);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Insert 4 versions of same column
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

      // Ensure maxVersions in query is respected
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(2);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      Scan scan = new Scan().withStartRow(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.readVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      // Flush and redo

      TEST_UTIL.flush();

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

      // Ensure maxVersions in query is respected
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(2);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      scan = new Scan().withStartRow(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.readVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      // Add some memstore and retest

      // Insert 4 more versions of same column and a dupe
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
      ht.put(put);

      // Ensure maxVersions in query is respected
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(7);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] },
        0, 6);

      scan = new Scan().withStartRow(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.readVersions(7);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] },
        0, 6);

      get = new Get(ROW);
      get.readVersions(7);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] },
        0, 6);

      scan = new Scan().withStartRow(ROW);
      scan.readVersions(7);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] },
        0, 6);

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

      // Ensure maxVersions of table is respected

      TEST_UTIL.flush();

      // Insert 4 more versions of same column and a dupe
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
      ht.put(put);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
          STAMPS[11], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
          VALUES[11], VALUES[13], VALUES[15] },
        0, 9);

      scan = new Scan().withStartRow(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
          STAMPS[11], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
          VALUES[11], VALUES[13], VALUES[15] },
        0, 9);

      // Delete a version in the memstore and a version in a storefile
      Delete delete = new Delete(ROW);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[11]);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[7]);
      ht.delete(delete);

      // Test that it's gone
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
          STAMPS[9], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8],
          VALUES[9], VALUES[13], VALUES[15] },
        0, 9);

      scan = new Scan().withStartRow(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
          STAMPS[9], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8],
          VALUES[9], VALUES[13], VALUES[15] },
        0, 9);
    }
  }

  @TestTemplate
  public void testUpdates() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table hTable = conn.getTable(tableName)) {
      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row1");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @TestTemplate
  public void testUpdatesWithMajorCompaction() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table hTable = conn.getTable(tableName);
      Admin admin = conn.getAdmin()) {
      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row2");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @TestTemplate
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    try (Connection conn = getConnection(); Table hTable = conn.getTable(tableName);
      Admin admin = conn.getAdmin()) {
      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row3");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);

      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @TestTemplate
  public void testGetEmptyTable() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Get get = new Get(ROW);
      get.addFamily(FAMILY);
      Result r = table.get(get);
      assertTrue(r.isEmpty());
    }
  }

  @TestTemplate
  public void testGetNullQualifier() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);
      LOG.info("Row put");

      Get get = new Get(ROW);
      get.addColumn(FAMILY, null);
      Result r = table.get(get);
      assertEquals(1, r.size());

      get = new Get(ROW);
      get.addFamily(FAMILY);
      r = table.get(get);
      assertEquals(2, r.size());
    }
  }

  @TestTemplate
  public void testGetNonExistentRow() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      LOG.info("Row put");

      Get get = new Get(ROW);
      get.addFamily(FAMILY);
      Result r = table.get(get);
      assertFalse(r.isEmpty());
      System.out.println("Row retrieved successfully");

      byte[] missingrow = Bytes.toBytes("missingrow");
      get = new Get(missingrow);
      get.addFamily(FAMILY);
      r = table.get(get);
      assertTrue(r.isEmpty());
      LOG.info("Row missing as it should be");
    }
  }

  @TestTemplate
  public void testPut() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    final byte[] value = Bytes.toBytes("abcd");
    TEST_UTIL.createTable(tableName, new byte[][] { CONTENTS_FAMILY, SMALL_FAMILY });
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(CONTENTS_FAMILY, null, value);
      table.put(put);

      put = new Put(row2);
      put.addColumn(CONTENTS_FAMILY, null, value);

      assertEquals(1, put.size());
      assertEquals(1, put.getFamilyCellMap().get(CONTENTS_FAMILY).size());

      // KeyValue v1 expectation. Cast for now until we go all Cell all the time. TODO
      KeyValue kv = (KeyValue) put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

      assertTrue(Bytes.equals(CellUtil.cloneFamily(kv), CONTENTS_FAMILY));
      // will it return null or an empty byte array?
      assertTrue(Bytes.equals(CellUtil.cloneQualifier(kv), new byte[0]));

      assertTrue(Bytes.equals(CellUtil.cloneValue(kv), value));

      table.put(put);

      Scan scan = new Scan();
      scan.addColumn(CONTENTS_FAMILY, null);
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          for (Cell key : r.rawCells()) {
            System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
          }
        }
      }
    }
  }

  @TestTemplate
  public void testPutNoCF() throws IOException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      assertThrows(NoSuchColumnFamilyException.class,
        () -> table.put(new Put(ROW).addColumn(BAD_FAM, QUALIFIER, VAL)),
        "Should throw NoSuchColumnFamilyException");
    }
  }

  @TestTemplate
  public void testRowsPut() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    TEST_UTIL.createTable(tableName, new byte[][] { CONTENTS_FAMILY, SMALL_FAMILY });
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      ArrayList<Put> rowsUpdate = new ArrayList<>();
      for (int i = 0; i < NB_BATCH_ROWS; i++) {
        byte[] row = Bytes.toBytes("row" + i);
        Put put = new Put(row);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(CONTENTS_FAMILY, null, value);
        rowsUpdate.add(put);
      }
      table.put(rowsUpdate);
      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int nbRows = Iterables.size(scanner);
        assertEquals(NB_BATCH_ROWS, nbRows);
      }
    }
  }

  @TestTemplate
  public void testRowsPutBufferedManyManyFlushes() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    TEST_UTIL.createTable(tableName, new byte[][] { CONTENTS_FAMILY, SMALL_FAMILY });
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      ArrayList<Put> rowsUpdate = new ArrayList<>();
      for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
        byte[] row = Bytes.toBytes("row" + i);
        Put put = new Put(row);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(CONTENTS_FAMILY, null, value);
        rowsUpdate.add(put);
      }
      table.put(rowsUpdate);

      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int nbRows = Iterables.size(scanner);
        assertEquals(NB_BATCH_ROWS * 10, nbRows);
      }
    }
  }

  /**
   * test for HBASE-737
   */
  @TestTemplate
  public void testHBase737() throws IOException {
    final byte[] FAM1 = Bytes.toBytes("fam1");
    final byte[] FAM2 = Bytes.toBytes("fam2");
    TEST_UTIL.createTable(tableName, new byte[][] { FAM1, FAM2 });
    // Open table
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      // Insert some values
      Put put = new Put(ROW);
      put.addColumn(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
      table.put(put);
      Threads.sleepWithoutInterrupt(1000);

      put = new Put(ROW);
      put.addColumn(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
      table.put(put);

      Threads.sleepWithoutInterrupt(1000);

      put = new Put(ROW);
      put.addColumn(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
      table.put(put);

      long[] times = new long[3];

      // First scan the memstore

      Scan scan = new Scan();
      scan.addFamily(FAM1);
      scan.addFamily(FAM2);
      try (ResultScanner s = table.getScanner(scan)) {
        int index = 0;
        Result r;
        while ((r = s.next()) != null) {
          for (Cell key : r.rawCells()) {
            times[index++] = key.getTimestamp();
          }
        }
      }
      for (int i = 0; i < times.length - 1; i++) {
        for (int j = i + 1; j < times.length; j++) {
          assertTrue(times[j] > times[i]);
        }
      }

      // Flush data to disk and try again
      TEST_UTIL.flush();

      // Reset times
      Arrays.fill(times, 0);

      Threads.sleepWithoutInterrupt(1000);

      scan = new Scan();
      scan.addFamily(FAM1);
      scan.addFamily(FAM2);
      try (ResultScanner s = table.getScanner(scan)) {
        int index = 0;
        Result r = null;
        while ((r = s.next()) != null) {
          for (Cell key : r.rawCells()) {
            times[index++] = key.getTimestamp();
          }
        }
        for (int i = 0; i < times.length - 1; i++) {
          for (int j = i + 1; j < times.length; j++) {
            assertTrue(times[j] > times[i]);
          }
        }
      }
    }
  }

  @TestTemplate
  public void testListTables() throws IOException {
    final String testTableName = tableName.toString();
    final TableName tableName1 = TableName.valueOf(testTableName + "1");
    final TableName tableName2 = TableName.valueOf(testTableName + "2");
    final TableName tableName3 = TableName.valueOf(testTableName + "3");
    TableName[] tables = new TableName[] { tableName1, tableName2, tableName3 };
    for (TableName table : tables) {
      TEST_UTIL.createTable(table, FAMILY);
    }
    try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {
      List<TableDescriptor> ts = admin.listTableDescriptors();
      HashSet<TableDescriptor> result = new HashSet<>(ts);
      int size = result.size();
      assertTrue(size >= tables.length);
      for (TableName table : tables) {
        boolean found = false;
        for (TableDescriptor t : ts) {
          if (t.getTableName().equals(table)) {
            found = true;
            break;
          }
        }
        assertTrue(found, "Not found: " + table);
      }
    }
  }

  @TestTemplate
  public void testMiscHTableStuff() throws IOException {
    final String testTableName = tableName.toString();
    final TableName tableAname = TableName.valueOf(testTableName + "A");
    final TableName tableBname = TableName.valueOf(testTableName + "B");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");
    TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    try (Connection conn = getConnection(); Table a = conn.getTable(tableAname);
      Table b = conn.getTable(tableBname)) {
      Put put = new Put(ROW);
      put.addColumn(HConstants.CATALOG_FAMILY, null, value);
      a.put(put);

      // open a new connection to A and a connection to b
      try (Connection c = getConnection(); Table newA = c.getTable(tableAname)) {
        // copy data from A to B
        Scan scan = new Scan();
        scan.addFamily(HConstants.CATALOG_FAMILY);
        try (ResultScanner s = newA.getScanner(scan)) {
          for (Result r : s) {
            put = new Put(r.getRow());
            put.setDurability(Durability.SKIP_WAL);
            for (Cell kv : r.rawCells()) {
              put.add(kv);
            }
            b.put(put);
          }
        }
      }

      // Opening a new connection to A will cause the tables to be reloaded
      try (Connection c = getConnection(); Table anotherA = c.getTable(tableAname)) {
        Get get = new Get(ROW);
        get.addFamily(HConstants.CATALOG_FAMILY);
        anotherA.get(get);
      }

      // We can still access A through newA because it has the table information
      // cached. And if it needs to recalibrate, that will cause the information
      // to be reloaded.

      // Test user metadata
      Admin admin = TEST_UTIL.getAdmin();
      // make a modifiable descriptor
      TableDescriptor desc = a.getDescriptor();
      // offline the table
      admin.disableTable(tableAname);
      // add a user attribute to HTD
      TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(desc).setValue(attrName, attrValue);
      // add a user attribute to HCD
      for (ColumnFamilyDescriptor c : desc.getColumnFamilies()) {
        builder.modifyColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(c).setValue(attrName, attrValue).build());
      }
      // update metadata for all regions of this table
      admin.modifyTable(builder.build());
      // enable the table
      admin.enableTable(tableAname);

      // Test that attribute changes were applied
      desc = a.getDescriptor();
      assertEquals(desc.getTableName(), tableAname, "wrong table descriptor returned");
      // check HTD attribute
      value = desc.getValue(attrName);
      assertNotNull(value, "missing HTD attribute value");
      assertFalse(Bytes.compareTo(value, attrValue) != 0, "HTD attribute value is incorrect");
      // check HCD attribute
      for (ColumnFamilyDescriptor c : desc.getColumnFamilies()) {
        value = c.getValue(attrName);
        assertNotNull(value, "missing HCD attribute value");
        assertFalse(Bytes.compareTo(value, attrValue) != 0, "HCD attribute value is incorrect");
      }
    }
  }
}
