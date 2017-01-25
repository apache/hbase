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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(SmallTests.class)
public class TestRollbackFromClient {
  @Rule
  public TestName name = new TestName();
  private final static HBaseTestingUtility TEST_UTIL
    = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final int SLAVES = 3;
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] QUALIFIER_V2 = Bytes.toBytes("testQualifierV2");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, FailedDefaultWALProvider.class.getName());
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAppendRollback() throws IOException {
    Updater updateForEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) {
        try {
          Append append = new Append(ROW);
          append.add(FAMILY, QUALIFIER, VALUE);
          append.add(FAMILY, QUALIFIER_V2, VALUE);
          FailedHLog.SHOULD_FAIL.set(true);
          table.append(append);
        } catch (IOException e) {
          // It should fail because the WAL fail also
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 0;
      }
    };
    testRollback(updateForEmptyTable, 1, null);
    testRollback(updateForEmptyTable, 2, null);

    final Append preAppend = new Append(ROW);
    preAppend.add(FAMILY, QUALIFIER, VALUE);
    Cell initCell = preAppend.getCellList(FAMILY).get(0);
    Updater updateForNonEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) throws IOException {
        table.append(preAppend);
        try {
          Append append = new Append(ROW);
          append.add(FAMILY, QUALIFIER, VALUE);
          append.add(FAMILY, QUALIFIER_V2, VALUE);
          FailedHLog.SHOULD_FAIL.set(true);
          table.append(append);
          Assert.fail("It should fail because the WAL sync is failed");
        } catch (IOException e) {
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 1;
      }
    };
    testRollback(updateForNonEmptyTable, 1, initCell);
    testRollback(updateForNonEmptyTable, 2, initCell);
  }

  @Test
  public void testIncrementRollback() throws IOException {
    Updater updateForEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) {
        try {
          Increment inc = new Increment(ROW);
          inc.addColumn(FAMILY, QUALIFIER, 1);
          inc.addColumn(FAMILY, QUALIFIER_V2, 2);
          FailedHLog.SHOULD_FAIL.set(true);
          table.increment(inc);
        } catch (IOException e) {
          // It should fail because the WAL fail also
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 0;
      }
    };
    testRollback(updateForEmptyTable, 1, null);
    testRollback(updateForEmptyTable, 2, null);

    final Increment preIncrement = new Increment(ROW);
    preIncrement.addColumn(FAMILY, QUALIFIER, 1);
    Cell initCell = preIncrement.getCellList(FAMILY).get(0);
    Updater updateForNonEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) throws IOException {
        table.increment(preIncrement);
        try {
          Increment inc = new Increment(ROW);
          inc.addColumn(FAMILY, QUALIFIER, 1);
          inc.addColumn(FAMILY, QUALIFIER_V2, 2);
          FailedHLog.SHOULD_FAIL.set(true);
          table.increment(inc);
          Assert.fail("It should fail because the WAL sync is failed");
        } catch (IOException e) {
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 1;
      }
    };
    testRollback(updateForNonEmptyTable, 1, initCell);
    testRollback(updateForNonEmptyTable, 2, initCell);
  }

  @Test
  public void testPutRollback() throws IOException {
    Updater updateForEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) {
        try {
          Put put = new Put(ROW);
          put.addColumn(FAMILY, QUALIFIER, VALUE);
          FailedHLog.SHOULD_FAIL.set(true);
          table.put(put);
          Assert.fail("It should fail because the WAL sync is failed");
        } catch (IOException e) {
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 0;
      }
    };
    testRollback(updateForEmptyTable, 1, null);
    testRollback(updateForEmptyTable, 2, null);

    final Put prePut = new Put(ROW);
    prePut.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("aaaaaaaaaaaaaaaaaaaaaa"));
    Cell preCell = prePut.getCellList(FAMILY).get(0);
    Updater updateForNonEmptyTable = new Updater() {
      @Override
      public int updateData(Table table, byte[] family) throws IOException {
        table.put(prePut);
        try {
          Put put = new Put(ROW);
          put.addColumn(FAMILY, QUALIFIER, VALUE);
          FailedHLog.SHOULD_FAIL.set(true);
          table.put(put);
          Assert.fail("It should fail because the WAL sync is failed");
        } catch (IOException e) {
        } finally {
          FailedHLog.SHOULD_FAIL.set(false);
        }
        return 1;
      }
    };
    testRollback(updateForNonEmptyTable, 1, preCell);
    testRollback(updateForNonEmptyTable, 2, preCell);
  }

  private void testRollback(Updater updater, int versions, Cell initCell) throws IOException {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor col = new HColumnDescriptor(FAMILY);
    col.setMaxVersions(versions);
    desc.addFamily(col);
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    int expected;
    List<Cell> cells;
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table table = conn.getTable(tableName)) {
      expected = updater.updateData(table, FAMILY);
      cells = getAllCells(table);
    }
    TEST_UTIL.getHBaseAdmin().disableTable(tableName);
    TEST_UTIL.getHBaseAdmin().deleteTable(tableName);
    assertEquals(expected, cells.size());
    if (initCell != null && cells.isEmpty()) {
      Cell cell = cells.get(0);
      assertTrue("row isn't matched", CellUtil.matchingRow(initCell, cell));
      assertTrue("column isn't matched", CellUtil.matchingColumn(initCell, cell));
      assertTrue("qualifier isn't matched", CellUtil.matchingQualifier(initCell, cell));
      assertTrue("value isn't matched", CellUtil.matchingValue(initCell, cell));
    }
  }

  interface Updater {
    int updateData(Table table, byte[] family) throws IOException;
  }

  private static List<Cell> getAllCells(Table table) throws IOException {
    List<Cell> cells = new ArrayList<>();
    try (ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result r : scanner) {
        cells.addAll(r.listCells());
      }
      return cells;
    }
  }

  public static class FailedDefaultWALProvider extends DefaultWALProvider {
    @Override
    public WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException {
      WAL wal = super.getWAL(identifier, namespace);
      return new FailedHLog(wal);
    }
  }

  public static class FailedHLog implements WAL {
    private static final AtomicBoolean SHOULD_FAIL = new AtomicBoolean(false);
    private final WAL delegation;
    FailedHLog(final WAL delegation) {
      this.delegation = delegation;
    }
    @Override
    public void registerWALActionsListener(WALActionsListener listener) {
      delegation.registerWALActionsListener(listener);
    }

    @Override
    public boolean unregisterWALActionsListener(WALActionsListener listener) {
      return delegation.unregisterWALActionsListener(listener);
    }

    @Override
    public byte[][] rollWriter() throws FailedLogCloseException, IOException {
      return delegation.rollWriter();
    }

    @Override
    public byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
      return delegation.rollWriter(force);
    }

    @Override
    public void shutdown() throws IOException {
      delegation.shutdown();
    }

    @Override
    public void close() throws IOException {
      delegation.close();
    }

    @Override
    public long append(HTableDescriptor htd, HRegionInfo info, WALKey key, WALEdit edits, boolean inMemstore) throws IOException {
      return delegation.append(htd, info, key, edits, inMemstore);
    }

    @Override
    public void sync() throws IOException {
      delegation.sync();
    }

    @Override
    public void sync(long txid) throws IOException {
      if (SHOULD_FAIL.get()) {
        throw new IOException("[TESTING] we need the failure!!!");
      }
      delegation.sync(txid);
    }

    @Override
    public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
      return delegation.startCacheFlush(encodedRegionName, families);
    }

    @Override
    public void completeCacheFlush(byte[] encodedRegionName) {
      delegation.completeCacheFlush(encodedRegionName);
    }

    @Override
    public void abortCacheFlush(byte[] encodedRegionName) {
      delegation.abortCacheFlush(encodedRegionName);
    }

    @Override
    public WALCoprocessorHost getCoprocessorHost() {
      return delegation.getCoprocessorHost();
    }

    @Override
    public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
      return delegation.getEarliestMemstoreSeqNum(encodedRegionName);
    }

    @Override
    public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
      return delegation.getEarliestMemstoreSeqNum(encodedRegionName, familyName);
    }

  }
}
