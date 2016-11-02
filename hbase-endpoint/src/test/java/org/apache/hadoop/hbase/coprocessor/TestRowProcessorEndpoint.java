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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.RowProcessorClient;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.FriendsOfFriendsProcessorRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.FriendsOfFriendsProcessorResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.IncCounterProcessorRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.IncCounterProcessorResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.RowSwapProcessorRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.RowSwapProcessorResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.TimeoutProcessorRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.TimeoutProcessorResponse;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessRequest;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessResponse;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.RowProcessorService;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Verifies ProcessEndpoint works.
 * The tested RowProcessor performs two scans and a read-modify-write.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestRowProcessorEndpoint {

  private static final Log LOG = LogFactory.getLog(TestRowProcessorEndpoint.class);

  private static final TableName TABLE = TableName.valueOf("testtable");
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

  private volatile static Table table = null;
  private volatile static boolean swapped = false;
  private volatile CountDownLatch startSignal;
  private volatile CountDownLatch doneSignal;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        RowProcessorEndpoint.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    conf.setLong("hbase.hregion.row.processor.timeout", 1000L);
    conf.setLong(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH, 2048);
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
      put.addColumn(FAM, A, Bytes.add(B, C));    // B, C are friends of A
      put.addColumn(FAM, B, Bytes.add(D, E, F)); // D, E, F are friends of B
      put.addColumn(FAM, C, G);                  // G is a friend of C
      table.put(put);
      rowSize = put.size();
    }
    Put put = new Put(ROW2);
    put.addColumn(FAM, D, E);
    put.addColumn(FAM, F, G);
    table.put(put);
    row2Size = put.size();
  }

  @Test
  public void testDoubleScan() throws Throwable {
    prepareTestData();

    CoprocessorRpcChannel channel = table.coprocessorService(ROW);
    RowProcessorEndpoint.FriendsOfFriendsProcessor processor =
        new RowProcessorEndpoint.FriendsOfFriendsProcessor(ROW, A);
    RowProcessorService.BlockingInterface service =
        RowProcessorService.newBlockingStub(channel);
    ProcessRequest request = RowProcessorClient.getRowProcessorPB(processor);
    ProcessResponse protoResult = service.process(null, request);
    FriendsOfFriendsProcessorResponse response =
        FriendsOfFriendsProcessorResponse.parseFrom(protoResult.getRowProcessorResult());
    Set<String> result = new HashSet<String>();
    result.addAll(response.getResultList());
    Set<String> expected =
      new HashSet<String>(Arrays.asList(new String[]{"d", "e", "f", "g"}));
    Get get = new Get(ROW);
    LOG.debug("row keyvalues:" + stringifyKvs(table.get(get).listCells()));
    assertEquals(expected, result);
  }

  @Test
  public void testReadModifyWrite() throws Throwable {
    prepareTestData();
    failures.set(0);
    int numThreads = 100;
    concurrentExec(new IncrementRunner(), numThreads);
    Get get = new Get(ROW);
    LOG.debug("row keyvalues:" + stringifyKvs(table.get(get).listCells()));
    int finalCounter = incrementCounter(table);
    int failureNumber = failures.get();
    if (failureNumber > 0) {
      LOG.debug("We failed " + failureNumber + " times during test");
    }
    assertEquals(numThreads + 1 - failureNumber, finalCounter);
  }

  class IncrementRunner implements Runnable {
    @Override
    public void run() {
      try {
        incrementCounter(table);
      } catch (Throwable e) {
        failures.incrementAndGet();
        e.printStackTrace();
      }
    }
  }

  private int incrementCounter(Table table) throws Throwable {
    CoprocessorRpcChannel channel = table.coprocessorService(ROW);
    RowProcessorEndpoint.IncrementCounterProcessor processor =
        new RowProcessorEndpoint.IncrementCounterProcessor(ROW);
    RowProcessorService.BlockingInterface service =
        RowProcessorService.newBlockingStub(channel);
    ProcessRequest request = RowProcessorClient.getRowProcessorPB(processor);
    ProcessResponse protoResult = service.process(null, request);
    IncCounterProcessorResponse response = IncCounterProcessorResponse
        .parseFrom(protoResult.getRowProcessorResult());
    Integer result = response.getResponse();
    return result;
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
    int numThreads = 100;
    concurrentExec(new SwapRowsRunner(), numThreads);
    LOG.debug("row keyvalues:" +
              stringifyKvs(table.get(new Get(ROW)).listCells()));
    LOG.debug("row2 keyvalues:" +
              stringifyKvs(table.get(new Get(ROW2)).listCells()));
    int failureNumber = failures.get();
    if (failureNumber > 0) {
      LOG.debug("We failed " + failureNumber + " times during test");
    }
    if (!swapped) {
      assertEquals(rowSize, table.get(new Get(ROW)).listCells().size());
      assertEquals(row2Size, table.get(new Get(ROW2)).listCells().size());
    } else {
      assertEquals(rowSize, table.get(new Get(ROW2)).listCells().size());
      assertEquals(row2Size, table.get(new Get(ROW)).listCells().size());
    }
  }

  class SwapRowsRunner implements Runnable {
    @Override
    public void run() {
      try {
        swapRows(table);
      } catch (Throwable e) {
        failures.incrementAndGet();
        e.printStackTrace();
      }
    }
  }

  private void swapRows(Table table) throws Throwable {
    CoprocessorRpcChannel channel = table.coprocessorService(ROW);
    RowProcessorEndpoint.RowSwapProcessor processor =
        new RowProcessorEndpoint.RowSwapProcessor(ROW, ROW2);
    RowProcessorService.BlockingInterface service =
        RowProcessorService.newBlockingStub(channel);
    ProcessRequest request = RowProcessorClient.getRowProcessorPB(processor);
    service.process(null, request);
  }

  @Test
  public void testTimeout() throws Throwable {
    prepareTestData();
    CoprocessorRpcChannel channel = table.coprocessorService(ROW);
    RowProcessorEndpoint.TimeoutProcessor processor =
        new RowProcessorEndpoint.TimeoutProcessor(ROW);
    RowProcessorService.BlockingInterface service =
        RowProcessorService.newBlockingStub(channel);
    ProcessRequest request = RowProcessorClient.getRowProcessorPB(processor);
    boolean exceptionCaught = false;
    try {
      service.process(null, request);
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
  public static class RowProcessorEndpoint<S extends Message,T extends Message>
  extends BaseRowProcessorEndpoint<S,T> implements CoprocessorService {
    public static class IncrementCounterProcessor extends
        BaseRowProcessor<IncrementCounterProcessorTestProtos.IncCounterProcessorRequest,
        IncrementCounterProcessorTestProtos.IncCounterProcessorResponse> {
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
      public IncCounterProcessorResponse getResult() {
        IncCounterProcessorResponse.Builder i = IncCounterProcessorResponse.newBuilder();
        i.setResponse(counter);
        return i.build();
      }

      @Override
      public boolean readOnly() {
        return false;
      }

      @Override
      public void process(long now, HRegion region,
          List<Mutation> mutations, WALEdit walEdit) throws IOException {
        // Scan current counter
        List<Cell> kvs = new ArrayList<Cell>();
        Scan scan = new Scan(row, row);
        scan.addColumn(FAM, COUNTER);
        doScan(region, scan, kvs);
        counter = kvs.size() == 0 ? 0 :
          Bytes.toInt(CellUtil.cloneValue(kvs.iterator().next()));

        // Assert counter value
        assertEquals(expectedCounter, counter);

        // Increment counter and send it to both memstore and wal edit
        counter += 1;
        expectedCounter += 1;


        Put p = new Put(row);
        KeyValue kv =
            new KeyValue(row, FAM, COUNTER, now, Bytes.toBytes(counter));
        p.add(kv);
        mutations.add(p);
        walEdit.add(kv);

        // We can also inject some meta data to the walEdit
        KeyValue metaKv = new KeyValue(
            row, WALEdit.METAFAMILY,
            Bytes.toBytes("I just increment counter"),
            Bytes.toBytes(counter));
        walEdit.add(metaKv);
      }

      @Override
      public IncCounterProcessorRequest getRequestData() throws IOException {
        IncCounterProcessorRequest.Builder builder = IncCounterProcessorRequest.newBuilder();
        builder.setCounter(counter);
        builder.setRow(ByteStringer.wrap(row));
        return builder.build();
      }

      @Override
      public void initialize(IncCounterProcessorRequest msg) {
        this.row = msg.getRow().toByteArray();
        this.counter = msg.getCounter();
      }
    }

    public static class FriendsOfFriendsProcessor extends
        BaseRowProcessor<FriendsOfFriendsProcessorRequest, FriendsOfFriendsProcessorResponse> {
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
      public FriendsOfFriendsProcessorResponse getResult() {
        FriendsOfFriendsProcessorResponse.Builder builder = 
            FriendsOfFriendsProcessorResponse.newBuilder();
        builder.addAllResult(result);
        return builder.build();
      }

      @Override
      public boolean readOnly() {
        return true;
      }

      @Override
      public void process(long now, HRegion region,
          List<Mutation> mutations, WALEdit walEdit) throws IOException {
        List<Cell> kvs = new ArrayList<Cell>();
        { // First scan to get friends of the person
          Scan scan = new Scan(row, row);
          scan.addColumn(FAM, person);
          doScan(region, scan, kvs);
        }

        // Second scan to get friends of friends
        Scan scan = new Scan(row, row);
        for (Cell kv : kvs) {
          byte[] friends = CellUtil.cloneValue(kv);
          for (byte f : friends) {
            scan.addColumn(FAM, new byte[]{f});
          }
        }
        doScan(region, scan, kvs);

        // Collect result
        result.clear();
        for (Cell kv : kvs) {
          for (byte b : CellUtil.cloneValue(kv)) {
            result.add((char)b + "");
          }
        }
      }

      @Override
      public FriendsOfFriendsProcessorRequest getRequestData() throws IOException {
        FriendsOfFriendsProcessorRequest.Builder builder =
            FriendsOfFriendsProcessorRequest.newBuilder();
        builder.setPerson(ByteStringer.wrap(person));
        builder.setRow(ByteStringer.wrap(row));
        builder.addAllResult(result);
        FriendsOfFriendsProcessorRequest f = builder.build();
        return f;
      }

      @Override
      public void initialize(FriendsOfFriendsProcessorRequest request) 
          throws IOException {
        this.person = request.getPerson().toByteArray();
        this.row = request.getRow().toByteArray();
        result.clear();
        result.addAll(request.getResultList());
      }
    }

    public static class RowSwapProcessor extends
        BaseRowProcessor<RowSwapProcessorRequest, RowSwapProcessorResponse> {
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
      public RowSwapProcessorResponse getResult() {
        return RowSwapProcessorResponse.getDefaultInstance();
      }

      @Override
      public void process(long now, HRegion region,
          List<Mutation> mutations, WALEdit walEdit) throws IOException {

        // Override the time to avoid race-condition in the unit test caused by
        // inacurate timer on some machines
        now = myTimer.getAndIncrement();

        // Scan both rows
        List<Cell> kvs1 = new ArrayList<Cell>();
        List<Cell> kvs2 = new ArrayList<Cell>();
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
        List<List<Cell>> kvs = new ArrayList<List<Cell>>();
        kvs.add(kvs1);
        kvs.add(kvs2);
        byte[][] rows = new byte[][]{row1, row2};
        for (int i = 0; i < kvs.size(); ++i) {
          for (Cell kv : kvs.get(i)) {
            // Delete from the current row and add to the other row
            Delete d = new Delete(rows[i]);
            KeyValue kvDelete =
                new KeyValue(rows[i], CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), 
                    kv.getTimestamp(), KeyValue.Type.Delete);
            d.addDeleteMarker(kvDelete);
            Put p = new Put(rows[1 - i]);
            KeyValue kvAdd =
                new KeyValue(rows[1 - i], CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv),
                    now, CellUtil.cloneValue(kv));
            p.add(kvAdd);
            mutations.add(d);
            walEdit.add(kvDelete);
            mutations.add(p);
            walEdit.add(kvAdd);
          }
        }
      }

      @Override
      public String getName() {
        return "swap";
      }

      @Override
      public RowSwapProcessorRequest getRequestData() throws IOException {
        RowSwapProcessorRequest.Builder builder = RowSwapProcessorRequest.newBuilder();
        builder.setRow1(ByteStringer.wrap(row1));
        builder.setRow2(ByteStringer.wrap(row2));
        return builder.build();
      }

      @Override
      public void initialize(RowSwapProcessorRequest msg) {
        this.row1 = msg.getRow1().toByteArray();
        this.row2 = msg.getRow2().toByteArray();
      }
    }

    public static class TimeoutProcessor extends
        BaseRowProcessor<TimeoutProcessorRequest, TimeoutProcessorResponse> {

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
      public TimeoutProcessorResponse getResult() {
        return TimeoutProcessorResponse.getDefaultInstance();
      }

      @Override
      public void process(long now, HRegion region,
          List<Mutation> mutations, WALEdit walEdit) throws IOException {
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
      public String getName() {
        return "timeout";
      }

      @Override
      public TimeoutProcessorRequest getRequestData() throws IOException {
        TimeoutProcessorRequest.Builder builder = TimeoutProcessorRequest.newBuilder();
        builder.setRow(ByteStringer.wrap(row));
        return builder.build();
      }

      @Override
      public void initialize(TimeoutProcessorRequest msg) throws IOException {
        this.row = msg.getRow().toByteArray();
      }
    }

    public static void doScan(
        HRegion region, Scan scan, List<Cell> result) throws IOException {
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

  static String stringifyKvs(Collection<Cell> kvs) {
    StringBuilder out = new StringBuilder();
    out.append("[");
    if (kvs != null) {
      for (Cell kv : kvs) {
        byte[] col = CellUtil.cloneQualifier(kv);
        byte[] val = CellUtil.cloneValue(kv);
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
