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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.RowProcessorClient;
import org.apache.hadoop.hbase.coprocessor.BaseRowProcessorEndpoint;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.IncCounterProcessorRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.IncrementCounterProcessorTestProtos.IncCounterProcessorResponse;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessRequest;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessResponse;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.RowProcessorService;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test HRegion#processRowsWithLocks
 */
@Category(MediumTests.class)
public class TestRegionProcessRowsWithLocks {

  private static final Log LOG = LogFactory.getLog(TestRegionProcessRowsWithLocks.class);

  private static final TableName TABLE = TableName.valueOf("testtable");
  private final static byte[] ROW = Bytes.toBytes("testrow");
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

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static volatile int expectedCounter = 0;

  private volatile static Table table = null;
  private static final AtomicBoolean throwsException = new AtomicBoolean(false);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        RowProcessorEndpoint.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    conf.setLong("hbase.hregion.row.processor.timeout", 1000L);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  public void prepareTestData() throws IOException {
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
    }
    expectedCounter = 0;
  }

  @Test
  public void testProcessNormal() throws ServiceException, IOException {
    prepareTestData();
    List<HRegion> regions = util.getHBaseCluster().getRegions(TABLE);
    HRegion region = regions.get(0);
    long startMemstoreSize = region.getMemstoreSize();
    long startFlushableSize = region.getStore(FAM).getFlushableSize();
    int finalCounter = incrementCounter(table);
    assertEquals(expectedCounter, finalCounter);
    Get get = new Get(ROW);
    Result result = table.get(get);
    LOG.debug("row keyvalues:" + stringifyKvs(result.listCells()));
    int getR = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell(FAM, COUNTER)));
    assertEquals(expectedCounter, getR);

    long endMemstoreSize = region.getMemstoreSize();
    long endFlushableSize = region.getStore(FAM).getFlushableSize();
    Assert.assertEquals("Should equal.", (endMemstoreSize - startMemstoreSize),
        (endFlushableSize - startFlushableSize));
  }

  @Test
  public void testProcessExceptionAndRollBack() throws IOException {
    prepareTestData();
    List<HRegion> regions = util.getHBaseCluster().getRegions(TABLE);
    HRegion region = regions.get(0);
    long startMemstoreSize = region.getMemstoreSize();
    long startFlushableSize = region.getStore(FAM).getFlushableSize();
    WAL wal = region.getWAL();
    wal.registerWALActionsListener(new WALActionsListener.Base() {
      @Override
      public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey, WALEdit logEdit)
          throws IOException {
        if (throwsException.get()) {
          throwsException.set(false);
          throw new IOException("throw test IOException");
        }
      }
    });
    try {
      incrementCounter(table);
      Assert.fail("Should throw IOException.");
    } catch (ServiceException | IOException e) {
    }

    long endMemstoreSize = region.getMemstoreSize();
    long endFlushableSize = region.getStore(FAM).getFlushableSize();
    LOG.info(
        "MemstoreSize deta=" + (endMemstoreSize - startMemstoreSize) + ",FlushableSize deta=" + (
            endFlushableSize - startFlushableSize));
    Assert.assertEquals("Should equal.", (endMemstoreSize - startMemstoreSize),
        (endFlushableSize - startFlushableSize));
  }

  private int incrementCounter(Table table) throws ServiceException, IOException {
    CoprocessorRpcChannel channel = table.coprocessorService(ROW);
    RowProcessorEndpoint.IncrementCounterProcessor processor =
        new RowProcessorEndpoint.IncrementCounterProcessor(ROW);
    RowProcessorService.BlockingInterface service = RowProcessorService.newBlockingStub(channel);
    ProcessRequest request = RowProcessorClient.getRowProcessorPB(processor);
    ProcessResponse protoResult = service.process(null, request);
    IncCounterProcessorResponse response =
        IncCounterProcessorResponse.parseFrom(protoResult.getRowProcessorResult());
    Integer result = response.getResponse();
    return result;
  }

  /**
   * This class defines two RowProcessors:
   * IncrementCounterProcessor and FriendsOfFriendsProcessor.
   * We define the RowProcessors as the inner class of the endpoint.
   * So they can be loaded with the endpoint on the coprocessor.
   */
  public static class RowProcessorEndpoint<S extends Message, T extends Message>
      extends BaseRowProcessorEndpoint<S, T> implements CoprocessorService {
    public static class IncrementCounterProcessor
        extends BaseRowProcessor<IncCounterProcessorRequest, IncCounterProcessorResponse> {
      int counter = 0;
      byte[] row = new byte[0];

      /**
       * Empty constructor for Writable
       */
      public IncrementCounterProcessor() {
      }

      public IncrementCounterProcessor(byte[] row) {
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
      public void process(long now, HRegion region, List<Mutation> mutations, WALEdit walEdit)
          throws IOException {
        // Scan current counter
        List<Cell> kvs = new ArrayList<Cell>();
        Scan scan = new Scan(row, row);
        scan.addColumn(FAM, COUNTER);
        doScan(region, scan, kvs);
        LOG.info("kvs.size()="+kvs.size());
        counter = kvs.size() == 0 ? 0 : Bytes.toInt(CellUtil.cloneValue(kvs.iterator().next()));
        LOG.info("counter=" + counter);

        // Assert counter value
        assertEquals(expectedCounter, counter);

        // Increment counter and send it to both memstore and wal edit
        counter += 1;
        expectedCounter += 1;

        Put p = new Put(row);
        KeyValue kv = new KeyValue(row, FAM, COUNTER, now, Bytes.toBytes(counter));
        p.add(kv);
        mutations.add(p);
        walEdit.add(kv);

        // We can also inject some meta data to the walEdit
        KeyValue metaKv =
            new KeyValue(row, WALEdit.METAFAMILY, Bytes.toBytes("I just increment counter"),
                Bytes.toBytes(counter));
        walEdit.add(metaKv);
        throwsException.set(true);
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

    public static void doScan(HRegion region, Scan scan, List<Cell> result) throws IOException {
      InternalScanner scanner = null;
      try {
        scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        scanner = region.getScanner(scan);
        result.clear();
        scanner.next(result);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
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
          out.append(Bytes.toStringBinary(col) + ":" + Bytes.toInt(val) + " ");
        } else {
          out.append(Bytes.toStringBinary(col) + ":" + Bytes.toStringBinary(val) + " ");
        }
      }
    }
    out.append("]");
    return out.toString();
  }

}
