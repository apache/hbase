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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Tests class {@link org.apache.hadoop.hbase.client.HTableWrapper}
 * by invoking its methods and briefly asserting the result is reasonable.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestHTableWrapper {

  private static final HBaseTestingUtility util = new HBaseTestingUtility();

  private static final TableName TEST_TABLE = TableName.valueOf("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");
  private static final byte[] ROW_D = Bytes.toBytes("ddd");
  private static final byte[] ROW_E = Bytes.toBytes("eee");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);
  private static final byte[] bytes4 = Bytes.toBytes(4);
  private static final byte[] bytes5 = Bytes.toBytes(5);

  static class DummyRegionObserver extends BaseRegionObserver {
  }

  private HTableInterface hTableInterface;
  private Table table;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    table = util.createTable(TEST_TABLE, TEST_FAMILY);

    Put puta = new Put(ROW_A);
    puta.add(TEST_FAMILY, qualifierCol1, bytes1);
    table.put(puta);

    Put putb = new Put(ROW_B);
    putb.add(TEST_FAMILY, qualifierCol1, bytes2);
    table.put(putb);

    Put putc = new Put(ROW_C);
    putc.add(TEST_FAMILY, qualifierCol1, bytes3);
    table.put(putc);
  }

  @After
  public void after() throws Exception {
    try {
      if (table != null) {
        table.close();
      }
    } finally {
      util.deleteTable(TEST_TABLE);
    }
  }

  @Test
  public void testHTableInterfaceMethods() throws Exception {
    Configuration conf = util.getConfiguration();
    MasterCoprocessorHost cpHost = util.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    Class<?> implClazz = DummyRegionObserver.class;
    cpHost.load(implClazz, Coprocessor.PRIORITY_HIGHEST, conf);
    CoprocessorEnvironment env = cpHost.findCoprocessorEnvironment(implClazz.getName());
    assertEquals(Coprocessor.VERSION, env.getVersion());
    assertEquals(VersionInfo.getVersion(), env.getHBaseVersion());
    hTableInterface = env.getTable(TEST_TABLE);
    checkHTableInterfaceMethods();
    cpHost.shutdown(env);
  }

  private void checkHTableInterfaceMethods() throws Exception {
    checkConf();
    checkNameAndDescriptor();
    checkAutoFlush();
    checkBufferSize();
    checkExists();
    checkGetRowOrBefore();
    checkAppend();
    checkPutsAndDeletes();
    checkCheckAndPut();
    checkCheckAndDelete();
    checkIncrementColumnValue();
    checkIncrement();
    checkBatch();
    checkCoprocessorService();
    checkMutateRow();
    checkResultScanner();

    hTableInterface.flushCommits();
    hTableInterface.close();
  }

  private void checkConf() {
    Configuration confExpected = util.getConfiguration();
    Configuration confActual = hTableInterface.getConfiguration();
    assertTrue(confExpected == confActual);
  }

  private void checkNameAndDescriptor() throws IOException {
    assertEquals(TEST_TABLE, hTableInterface.getName());
    assertEquals(table.getTableDescriptor(), hTableInterface.getTableDescriptor());
  }

  private void checkAutoFlush() {
    boolean initialAutoFlush = hTableInterface.isAutoFlush();
    hTableInterface.setAutoFlushTo(false);
    assertFalse(hTableInterface.isAutoFlush());
    hTableInterface.setAutoFlush(true, true);
    assertTrue(hTableInterface.isAutoFlush());
    hTableInterface.setAutoFlushTo(initialAutoFlush);
  }

  private void checkBufferSize() throws IOException {
    long initialWriteBufferSize = hTableInterface.getWriteBufferSize();
    hTableInterface.setWriteBufferSize(12345L);
    assertEquals(12345L, hTableInterface.getWriteBufferSize());
    hTableInterface.setWriteBufferSize(initialWriteBufferSize);
  }

  private void checkExists() throws IOException {
    boolean ex = hTableInterface.exists(new Get(ROW_A).addColumn(TEST_FAMILY, qualifierCol1));
    assertTrue(ex);

    Boolean[] exArray = hTableInterface.exists(Arrays.asList(new Get[] {
        new Get(ROW_A).addColumn(TEST_FAMILY, qualifierCol1),
        new Get(ROW_B).addColumn(TEST_FAMILY, qualifierCol1),
        new Get(ROW_C).addColumn(TEST_FAMILY, qualifierCol1),
        new Get(Bytes.toBytes("does not exist")).addColumn(TEST_FAMILY, qualifierCol1), }));
    assertArrayEquals(new Boolean[] { Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE },
        exArray);
  }

  @SuppressWarnings("deprecation")
  private void checkGetRowOrBefore() throws IOException {
    Result rowOrBeforeResult = hTableInterface.getRowOrBefore(ROW_A, TEST_FAMILY);
    assertArrayEquals(ROW_A, rowOrBeforeResult.getRow());
  }

  private void checkAppend() throws IOException {
    final byte[] appendValue = Bytes.toBytes("append");
    Append append = new Append(qualifierCol1).add(TEST_FAMILY, qualifierCol1, appendValue);
    Result appendResult = hTableInterface.append(append);
    byte[] appendedRow = appendResult.getRow();
    checkRowValue(appendedRow, appendValue);
  }

  private void checkPutsAndDeletes() throws IOException {
    // put:
    Put putD = new Put(ROW_D).add(TEST_FAMILY, qualifierCol1, bytes2);
    hTableInterface.put(putD);
    checkRowValue(ROW_D, bytes2);

    // delete:
    Delete delete = new Delete(ROW_D);
    hTableInterface.delete(delete);
    checkRowValue(ROW_D, null);

    // multiple puts:
    Put[] puts = new Put[] { new Put(ROW_D).add(TEST_FAMILY, qualifierCol1, bytes2),
        new Put(ROW_E).add(TEST_FAMILY, qualifierCol1, bytes3) };
    hTableInterface.put(Arrays.asList(puts));
    checkRowsValues(new byte[][] { ROW_D, ROW_E }, new byte[][] { bytes2, bytes3 });

    // multiple deletes:
    Delete[] deletes = new Delete[] { new Delete(ROW_D), new Delete(ROW_E) };
    hTableInterface.delete(new ArrayList<Delete>(Arrays.asList(deletes)));
    checkRowsValues(new byte[][] { ROW_D, ROW_E }, new byte[][] { null, null });
  }

  private void checkCheckAndPut() throws IOException {
    Put putC = new Put(ROW_C).add(TEST_FAMILY, qualifierCol1, bytes5);
    assertFalse(hTableInterface.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, /* expect */bytes4,
      putC/* newValue */));
    assertTrue(hTableInterface.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, /* expect */bytes3,
      putC/* newValue */));
    checkRowValue(ROW_C, bytes5);
  }

  private void checkCheckAndDelete() throws IOException {
    Delete delete = new Delete(ROW_C);
    assertFalse(hTableInterface.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, bytes4, delete));
    assertTrue(hTableInterface.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, bytes5, delete));
    checkRowValue(ROW_C, null);
  }

  private void checkIncrementColumnValue() throws IOException {
    hTableInterface.put(new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, Bytes.toBytes(1L)));
    checkRowValue(ROW_A, Bytes.toBytes(1L));

    final long newVal = hTableInterface
        .incrementColumnValue(ROW_A, TEST_FAMILY, qualifierCol1, 10L);
    assertEquals(11L, newVal);
    checkRowValue(ROW_A, Bytes.toBytes(11L));

    final long newVal2 = hTableInterface.incrementColumnValue(ROW_A, TEST_FAMILY, qualifierCol1,
        -10L, Durability.SYNC_WAL);
    assertEquals(1L, newVal2);
    checkRowValue(ROW_A, Bytes.toBytes(1L));
  }

  private void checkIncrement() throws IOException {
    hTableInterface.increment(new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, -5L));
    checkRowValue(ROW_A, Bytes.toBytes(-4L));
  }

  private void checkBatch() throws IOException, InterruptedException {
    Object[] results1 = hTableInterface.batch(Arrays.asList(new Row[] {
        new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L),
        new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L) }));
    assertEquals(2, results1.length);
    for (Object r2 : results1) {
      assertTrue(r2 instanceof Result);
    }
    checkRowValue(ROW_A, Bytes.toBytes(0L));
    Object[] results2 = new Result[2];
    hTableInterface.batch(
        Arrays.asList(new Row[] { new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L),
            new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L) }), results2);
    for (Object r2 : results2) {
      assertTrue(r2 instanceof Result);
    }
    checkRowValue(ROW_A, Bytes.toBytes(4L));

    // with callbacks:
    final long[] updateCounter = new long[] { 0L };
    Object[] results3 = hTableInterface.batchCallback(
        Arrays.asList(new Row[] { new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L),
            new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L) }),
        new Batch.Callback<Result>() {
          @Override
          public void update(byte[] region, byte[] row, Result result) {
            updateCounter[0]++;
          }
        });
    assertEquals(2, updateCounter[0]);
    assertEquals(2, results3.length);
    for (Object r3 : results3) {
      assertTrue(r3 instanceof Result);
    }
    checkRowValue(ROW_A, Bytes.toBytes(8L));

    Object[] results4 = new Result[2];
    updateCounter[0] = 0L;
    hTableInterface.batchCallback(
        Arrays.asList(new Row[] { new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L),
            new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L) }), results4,
        new Batch.Callback<Result>() {
          @Override
          public void update(byte[] region, byte[] row, Result result) {
            updateCounter[0]++;
          }
        });
    assertEquals(2, updateCounter[0]);
    for (Object r2 : results4) {
      assertTrue(r2 instanceof Result);
    }
    checkRowValue(ROW_A, Bytes.toBytes(12L));
  }

  private void checkCoprocessorService() {
    CoprocessorRpcChannel crc = hTableInterface.coprocessorService(ROW_A);
    assertNotNull(crc);
  }

  private void checkMutateRow() throws IOException {
    Put put = new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, bytes1);
    RowMutations rowMutations = new RowMutations(ROW_A);
    rowMutations.add(put);
    hTableInterface.mutateRow(rowMutations);
    checkRowValue(ROW_A, bytes1);
  }

  private void checkResultScanner() throws IOException {
    ResultScanner resultScanner = hTableInterface.getScanner(TEST_FAMILY);
    Result[] results = resultScanner.next(10);
    assertEquals(3, results.length);

    resultScanner = hTableInterface.getScanner(TEST_FAMILY, qualifierCol1);
    results = resultScanner.next(10);
    assertEquals(3, results.length);

    resultScanner = hTableInterface.getScanner(new Scan(ROW_A, ROW_C));
    results = resultScanner.next(10);
    assertEquals(2, results.length);
  }

  private void checkRowValue(byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, qualifierCol1);
    Result result = hTableInterface.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, qualifierCol1);
    assertArrayEquals(expectedValue, actualValue);
  }

  private void checkRowsValues(byte[][] rows, byte[][] expectedValues) throws IOException {
    if (rows.length != expectedValues.length) {
      throw new IllegalArgumentException();
    }
    Get[] gets = new Get[rows.length];
    for (int i = 0; i < gets.length; i++) {
      gets[i] = new Get(rows[i]).addColumn(TEST_FAMILY, qualifierCol1);
    }
    Result[] results = hTableInterface.get(Arrays.asList(gets));
    for (int i = 0; i < expectedValues.length; i++) {
      byte[] actualValue = results[i].getValue(TEST_FAMILY, qualifierCol1);
      assertArrayEquals(expectedValues[i], actualValue);
    }
  }

}
