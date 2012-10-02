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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * Verifies ProcessRowEndpoint works.
 * The tested RowProcessor performs two scans and a read-modify-write.
 */
@Category(MediumTests.class)
public class TestRowProcessorEndpoint {

  static final Log LOG = LogFactory.getLog(TestRowProcessorEndpoint.class);

  private static final byte[] TABLE = Bytes.toBytes("testtable");
  private final static byte[] ROW = Bytes.toBytes("testrow");
  private final static byte[] ROW2 = Bytes.toBytes("testrow2");
  private final static byte[] FAM = Bytes.toBytes("friendlist");

  // Column names
  private final static byte[] A = Bytes.toBytes("a");
  private final static byte[] B = Bytes.toBytes("b");
  private final static byte[] C = Bytes.toBytes("c");
  private final static byte[] D = Bytes.toBytes("d");
  private final static byte[] E = Bytes.toBytes("e");
  private final static byte[] F = Bytes.toBytes("f");
  private final static byte[] G = Bytes.toBytes("g");
  private final static byte[] COUNTER = Bytes.toBytes("counter");
  private final static AtomicLong myTimer = new AtomicLong(0);
  private final AtomicInteger failures = new AtomicInteger(0);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static volatile int expectedCounter = 0;
  private static int rowSize, row2Size;

  private volatile static HTable table = null;
  private volatile static boolean swapped = false;
  private volatile CountDownLatch startSignal;
  private volatile CountDownLatch doneSignal;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        RowProcessorEndpoint.class.getName());
    conf.setInt("hbase.client.retries.number", 1);
    conf.setLong("hbase.hregion.row.processor.timeout", 1000L);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  public void prepareTestData() throws Exception {
    try {
      util.getHBaseAdmin().disableTable(TABLE);
      util.getHBaseAdmin().deleteTable(TABLE);
    } catch (Exception e) {
      // ignore table not found
    }
    table = util.createTable(TABLE, FAM);
    {
      Put put = new Put(ROW);
      put.add(FAM, A, Bytes.add(B, C));    // B, C are friends of A
      put.add(FAM, B, Bytes.add(D, E, F)); // D, E, F are friends of B
      put.add(FAM, C, G);                  // G is a friend of C
      table.put(put);
      rowSize = put.size();
    }
    Put put = new Put(ROW2);
    put.add(FAM, D, E);
    put.add(FAM, F, G);
    table.put(put);
    row2Size = put.size();
  }

  @Test
  public void testDoubleScan() throws Throwable {
    prepareTestData();
    RowProcessorProtocol protocol =
        table.coprocessorProxy(RowProcessorProtocol.class, ROW);
    RowProcessorEndpoint.FriendsOfFriendsProcessor processor =
        new RowProcessorEndpoint.FriendsOfFriendsProcessor(ROW, A);
    Set<String> result = protocol.process(processor);

    Set<String> expected =
      new HashSet<String>(Arrays.asList(new String[]{"d", "e", "f", "g"}));
    Get get = new Get(ROW);
    LOG.debug("row keyvalues:" + stringifyKvs(table.get(get).list()));
    assertEquals(expected, result);
  }

  @Test
  public void testReadModifyWrite() throws Throwable {
    prepareTestData();
    failures.set(0);
    int numThreads = 1000;
    concurrentExec(new IncrementRunner(), numThreads);
    Get get = new Get(ROW);
    LOG.debug("row keyvalues:" + stringifyKvs(table.get(get).list()));
    int finalCounter = incrementCounter(table);
    assertEquals(numThreads + 1, finalCounter);
    assertEquals(0, failures.get());
  }

  class IncrementRunner implements Runnable {
    @Override
    public void run() {
      try {
        incrementCounter(table);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  private int incrementCounter(HTable table) throws Throwable {
    RowProcessorProtocol protocol =
        table.coprocessorProxy(RowProcessorProtocol.class, ROW);
    RowProcessorEndpoint.IncrementCounterProcessor processor =
        new RowProcessorEndpoint.IncrementCounterProcessor(ROW);
    int counterValue = protocol.process(processor);
    return counterValue;
  }

  private void concurrentExec(
      final Runnable task, final int numThreads) throws Throwable {
    startSignal = new CountDownLatch(numThreads);
    doneSignal = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            startSignal.countDown();
            startSignal.await();
            task.run();
          } catch (Throwable e) {
            failures.incrementAndGet();
            e.printStackTrace();
          }
          doneSignal.countDown();
        }
      }).start();
    }
    doneSignal.await();
  }

  @Test
  public void testMultipleRows() throws Throwable {
    prepareTestData();
    failures.set(0);
    int numThreads = 1000;
    concurrentExec(new SwapRowsRunner(), numThreads);
    LOG.debug("row keyvalues:" +
              stringifyKvs(table.get(new Get(ROW)).list()));
    LOG.debug("row2 keyvalues:" +
              stringifyKvs(table.get(new Get(ROW2)).list()));
    assertEquals(rowSize, table.get(new Get(ROW)).list().size());
    assertEquals(row2Size, table.get(new Get(ROW2)).list().size());
    assertEquals(0, failures.get());
  }

  class SwapRowsRunner implements Runnable {
    @Override
    public void run() {
      try {
        swapRows(table);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  private void swapRows(HTable table) throws Throwable {
    RowProcessorProtocol protocol =
        table.coprocessorProxy(RowProcessorProtocol.class, ROW);
    RowProcessorEndpoint.RowSwapProcessor processor =
        new RowProcessorEndpoint.RowSwapProcessor(ROW, ROW2);
    protocol.process(processor);
  }

  @Test
  public void testTimeout() throws Throwable {
    prepareTestData();
    RowProcessorProtocol protocol =
        table.coprocessorProxy(RowProcessorProtocol.class, ROW);
    RowProcessorEndpoint.TimeoutProcessor processor =
        new RowProcessorEndpoint.TimeoutProcessor(ROW);
    boolean exceptionCaught = false;
    try {
      protocol.process(processor);
    } catch (Exception e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);
  }

  /**
   * This class defines two RowProcessors:
   * IncrementCounterProcessor and FriendsOfFriendsProcessor.
   *
   * We define the RowProcessors as the inner class of the endpoint.
   * So they can be loaded with the endpoint on the coprocessor.
   */
  public static class RowProcessorEndpoint extends BaseRowProcessorEndpoint
      implements RowProcessorProtocol {

    public static class IncrementCounterProcessor extends
        BaseRowProcessor<Integer> implements Writable {
      int counter = 0;
      byte[] row = new byte[0];

      /**
       * Empty constructor for Writable
       */
      IncrementCounterProcessor() {
      }

      IncrementCounterProcessor(byte[] row) {
        this.row = row;
      }

      @Override
      public Collection<byte[]> getRowsToLock() {
        return Collections.singleton(row);
      }

      @Override
      public Integer getResult() {
        return counter;
      }

      @Override
      public boolean readOnly() {
        return false;
      }

      @Override
      public void process(long now, HRegion region,
          List<KeyValue> mutations, WALEdit walEdit) throws IOException {
        // Scan current counter
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        Scan scan = new Scan(row, row);
        scan.addColumn(FAM, COUNTER);
        doScan(region, scan, kvs);
        counter = kvs.size() == 0 ? 0 :
          Bytes.toInt(kvs.iterator().next().getValue());

        // Assert counter value
        assertEquals(expectedCounter, counter);

        // Increment counter and send it to both memstore and wal edit
        counter += 1;
        expectedCounter += 1;


        KeyValue kv =
            new KeyValue(row, FAM, COUNTER, now, Bytes.toBytes(counter));
        mutations.add(kv);
        walEdit.add(kv);

        // We can also inject some meta data to the walEdit
        KeyValue metaKv = new KeyValue(
            row, HLog.METAFAMILY,
            Bytes.toBytes("I just increment counter"),
            Bytes.toBytes(counter));
        walEdit.add(metaKv);
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        this.row = Bytes.readByteArray(in);
        this.counter = in.readInt();
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, row);
        out.writeInt(counter);
      }

    }

    public static class FriendsOfFriendsProcessor extends
        BaseRowProcessor<Set<String>> implements Writable {
      byte[] row = null;
      byte[] person = null;
      final Set<String> result = new HashSet<String>();

      /**
       * Empty constructor for Writable
       */
      FriendsOfFriendsProcessor() {
      }

      FriendsOfFriendsProcessor(byte[] row, byte[] person) {
        this.row = row;
        this.person = person;
      }

      @Override
      public Collection<byte[]> getRowsToLock() {
        return Collections.singleton(row);
      }

      @Override
      public Set<String> getResult() {
        return result;
      }

      @Override
      public boolean readOnly() {
        return true;
      }

      @Override
      public void process(long now, HRegion region,
          List<KeyValue> mutations, WALEdit walEdit) throws IOException {
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        { // First scan to get friends of the person
          Scan scan = new Scan(row, row);
          scan.addColumn(FAM, person);
          doScan(region, scan, kvs);
        }

        // Second scan to get friends of friends
        Scan scan = new Scan(row, row);
        for (KeyValue kv : kvs) {
          byte[] friends = kv.getValue();
          for (byte f : friends) {
            scan.addColumn(FAM, new byte[]{f});
          }
        }
        doScan(region, scan, kvs);

        // Collect result
        result.clear();
        for (KeyValue kv : kvs) {
          for (byte b : kv.getValue()) {
            result.add((char)b + "");
          }
        }
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        this.person = Bytes.readByteArray(in);
        this.row = Bytes.readByteArray(in);
        int size = in.readInt();
        result.clear();
        for (int i = 0; i < size; ++i) {
          result.add(Text.readString(in));
        }
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, person);
        Bytes.writeByteArray(out, row);
        out.writeInt(result.size());
        for (String s : result) {
          Text.writeString(out, s);
        }
      }
    }

    public static class RowSwapProcessor extends
        BaseRowProcessor<Set<String>> implements Writable {
      byte[] row1 = new byte[0];
      byte[] row2 = new byte[0];

      /**
       * Empty constructor for Writable
       */
      RowSwapProcessor() {
      }

      RowSwapProcessor(byte[] row1, byte[] row2) {
        this.row1 = row1;
        this.row2 = row2;
      }

      @Override
      public Collection<byte[]> getRowsToLock() {
        List<byte[]> rows = new ArrayList<byte[]>();
        rows.add(row1);
        rows.add(row2);
        return rows;
      }

      @Override
      public boolean readOnly() {
        return false;
      }

      @Override
      public void process(long now, HRegion region,
          List<KeyValue> mutations, WALEdit walEdit) throws IOException {

        // Override the time to avoid race-condition in the unit test caused by
        // inacurate timer on some machines
        now = myTimer.getAndIncrement();

        // Scan both rows
        List<KeyValue> kvs1 = new ArrayList<KeyValue>();
        List<KeyValue> kvs2 = new ArrayList<KeyValue>();
        doScan(region, new Scan(row1, row1), kvs1);
        doScan(region, new Scan(row2, row2), kvs2);

        // Assert swapped
        if (swapped) {
          assertEquals(rowSize, kvs2.size());
          assertEquals(row2Size, kvs1.size());
        } else {
          assertEquals(rowSize, kvs1.size());
          assertEquals(row2Size, kvs2.size());
        }
        swapped = !swapped;

        // Add and delete keyvalues
        List<List<KeyValue>> kvs = new ArrayList<List<KeyValue>>();
        kvs.add(kvs1);
        kvs.add(kvs2);
        byte[][] rows = new byte[][]{row1, row2};
        for (int i = 0; i < kvs.size(); ++i) {
          for (KeyValue kv : kvs.get(i)) {
            // Delete from the current row and add to the other row
            KeyValue kvDelete =
                new KeyValue(rows[i], kv.getFamily(), kv.getQualifier(),
                    kv.getTimestamp(), KeyValue.Type.Delete);
            KeyValue kvAdd =
                new KeyValue(rows[1 - i], kv.getFamily(), kv.getQualifier(),
                    now, kv.getValue());
            mutations.add(kvDelete);
            walEdit.add(kvDelete);
            mutations.add(kvAdd);
            walEdit.add(kvAdd);
          }
        }
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        this.row1 = Bytes.readByteArray(in);
        this.row2 = Bytes.readByteArray(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, row1);
        Bytes.writeByteArray(out, row2);
      }

      @Override
      public String getName() {
        return "swap";
      }
    }

    public static class TimeoutProcessor extends
        BaseRowProcessor<Void> implements Writable {

      byte[] row = new byte[0];

      /**
       * Empty constructor for Writable
       */
      public TimeoutProcessor() {
      }

      public TimeoutProcessor(byte[] row) {
        this.row = row;
      }

      public Collection<byte[]> getRowsToLock() {
        return Collections.singleton(row);
      }

      @Override
      public void process(long now, HRegion region,
          List<KeyValue> mutations, WALEdit walEdit) throws IOException {
        try {
          // Sleep for a long time so it timeout
          Thread.sleep(100 * 1000L);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }

      @Override
      public boolean readOnly() {
        return true;
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        this.row = Bytes.readByteArray(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, row);
      }

      @Override
      public String getName() {
        return "timeout";
      }
    }

    public static void doScan(
        HRegion region, Scan scan, List<KeyValue> result) throws IOException {
      InternalScanner scanner = null;
      try {
        scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        scanner = region.getScanner(scan);
        result.clear();
        scanner.next(result);
      } finally {
        if (scanner != null) scanner.close();
      }
    }
  }

  static String stringifyKvs(Collection<KeyValue> kvs) {
    StringBuilder out = new StringBuilder();
    out.append("[");
    if (kvs != null) {
      for (KeyValue kv : kvs) {
        byte[] col = kv.getQualifier();
        byte[] val = kv.getValue();
        if (Bytes.equals(col, COUNTER)) {
          out.append(Bytes.toStringBinary(col) + ":" +
                     Bytes.toInt(val) + " ");
        } else {
          out.append(Bytes.toStringBinary(col) + ":" +
                     Bytes.toStringBinary(val) + " ");
        }
      }
    }
    out.append("]");
    return out.toString();
  }

}
