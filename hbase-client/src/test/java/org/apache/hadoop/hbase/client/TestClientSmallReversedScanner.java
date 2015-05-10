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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.ClientSmallScanner.SmallScannerCallableFactory;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the ClientSmallReversedScanner.
 */
@Category(SmallTests.class)
public class TestClientSmallReversedScanner {

  Scan scan;
  ExecutorService pool;
  Configuration conf;

  ClusterConnection clusterConn;
  RpcRetryingCallerFactory rpcFactory;
  RpcControllerFactory controllerFactory;
  RpcRetryingCaller<Result[]> caller;

  @Before
  @SuppressWarnings({"deprecation", "unchecked"})
  public void setup() throws IOException {
    clusterConn = Mockito.mock(ClusterConnection.class);
    rpcFactory = Mockito.mock(RpcRetryingCallerFactory.class);
    controllerFactory = Mockito.mock(RpcControllerFactory.class);
    pool = Executors.newSingleThreadExecutor();
    scan = new Scan();
    conf = new Configuration();
    Mockito.when(clusterConn.getConfiguration()).thenReturn(conf);
    // Mock out the RpcCaller
    caller = Mockito.mock(RpcRetryingCaller.class);
    // Return the mock from the factory
    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
  }

  @After
  public void teardown() {
    if (null != pool) {
      pool.shutdownNow();
    }
  }

  /**
   * Create a simple Answer which returns true the first time, and false every time after.
   */
  private Answer<Boolean> createTrueThenFalseAnswer() {
    return new Answer<Boolean>() {
      boolean first = true;

      @Override
      public Boolean answer(InvocationOnMock invocation) {
        if (first) {
          first = false;
          return true;
        }
        return false;
      }
    };
  }

  private SmallScannerCallableFactory getFactory(
      final ScannerCallableWithReplicas callableWithReplicas) {
    return new SmallScannerCallableFactory() {
      @Override
      public ScannerCallableWithReplicas getCallable(ClusterConnection connection, TableName table,
          Scan scan, ScanMetrics scanMetrics, byte[] localStartKey, int cacheNum,
          RpcControllerFactory controllerFactory, ExecutorService pool,
          int primaryOperationTimeout, int retries, int scannerTimeout, Configuration conf,
          RpcRetryingCaller<Result[]> caller) {
        return callableWithReplicas;
      }
    };
  }

  @Test
  public void testContextPresent() throws Exception {
    final KeyValue kv1 = new KeyValue("row1".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv2 = new KeyValue("row2".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv3 = new KeyValue("row3".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);

    ScannerCallableWithReplicas callableWithReplicas = Mockito
        .mock(ScannerCallableWithReplicas.class);

    // Mock out the RpcCaller
    @SuppressWarnings("unchecked")
    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);
    // Return the mock from the factory
    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);

    // Intentionally leave a "default" caching size in the Scan. No matter the value, we
    // should continue based on the server context

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    try (ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn, rpcFactory, controllerFactory, pool,
        Integer.MAX_VALUE)) {

      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(caller.callWithoutRetries(callableWithReplicas, csrs.getScannerTimeout()))
          .thenAnswer(new Answer<Result[]>() {
            int count = 0;

            @Override
            public Result[] answer(InvocationOnMock invocation) {
              Result[] results;
              if (0 == count) {
                results = new Result[] {Result.create(new Cell[] {kv3}),
                    Result.create(new Cell[] {kv2})};
              } else if (1 == count) {
                results = new Result[] {Result.create(new Cell[] {kv1})};
              } else {
                results = new Result[0];
              }
              count++;
              return results;
            }
          });

      // Pass back the context always
      Mockito.when(callableWithReplicas.hasMoreResultsContext()).thenReturn(true);
      // Only have more results the first time
      Mockito.when(callableWithReplicas.getServerHasMoreResults()).thenAnswer(
          createTrueThenFalseAnswer());

      // A mocked HRegionInfo so ClientSmallScanner#nextScanner(...) works right
      HRegionInfo regionInfo = Mockito.mock(HRegionInfo.class);
      Mockito.when(callableWithReplicas.getHRegionInfo()).thenReturn(regionInfo);
      // Trigger the "no more data" branch for #nextScanner(...)
      Mockito.when(regionInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);

      csrs.loadCache();

      Queue<Result> results = csrs.cache;
      Iterator<Result> iter = results.iterator();
      assertEquals(3, results.size());
      for (int i = 3; i >= 1 && iter.hasNext(); i--) {
        Result result = iter.next();
        byte[] row = result.getRow();
        assertEquals("row" + i, new String(row, StandardCharsets.UTF_8));
        assertEquals(1, result.getMap().size());
      }
      assertTrue(csrs.closed);
    }
  }

  @Test
  public void testNoContextFewerRecords() throws Exception {
    final KeyValue kv1 = new KeyValue("row1".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv2 = new KeyValue("row2".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv3 = new KeyValue("row3".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);

    ScannerCallableWithReplicas callableWithReplicas = Mockito
        .mock(ScannerCallableWithReplicas.class);

    // While the server returns 2 records per batch, we expect more records.
    scan.setCaching(2);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    try (ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn, rpcFactory, controllerFactory, pool,
        Integer.MAX_VALUE)) {

      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(caller.callWithoutRetries(callableWithReplicas, csrs.getScannerTimeout()))
          .thenAnswer(new Answer<Result[]>() {
            int count = 0;

            @Override
            public Result[] answer(InvocationOnMock invocation) {
              Result[] results;
              if (0 == count) {
                results = new Result[] {Result.create(new Cell[] {kv3}),
                    Result.create(new Cell[] {kv2})};
              } else if (1 == count) {
                // Return fewer records than expected (2)
                results = new Result[] {Result.create(new Cell[] {kv1})};
              } else {
                throw new RuntimeException("Should not fetch a third batch from the server");
              }
              count++;
              return results;
            }
          });

      // Server doesn't return the context
      Mockito.when(callableWithReplicas.hasMoreResultsContext()).thenReturn(false);
      // getServerHasMoreResults shouldn't be called when hasMoreResultsContext returns false
      Mockito.when(callableWithReplicas.getServerHasMoreResults())
          .thenThrow(new RuntimeException("Should not be called"));

      // A mocked HRegionInfo so ClientSmallScanner#nextScanner(...) works right
      HRegionInfo regionInfo = Mockito.mock(HRegionInfo.class);
      Mockito.when(callableWithReplicas.getHRegionInfo()).thenReturn(regionInfo);
      // Trigger the "no more data" branch for #nextScanner(...)
      Mockito.when(regionInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);

      csrs.loadCache();

      Queue<Result> results = csrs.cache;
      Iterator<Result> iter = results.iterator();
      assertEquals(2, results.size());
      for (int i = 3; i >= 2 && iter.hasNext(); i--) {
        Result result = iter.next();
        byte[] row = result.getRow();
        assertEquals("row" + i, new String(row, StandardCharsets.UTF_8));
        assertEquals(1, result.getMap().size());
      }

      // "consume" the Results
      results.clear();

      csrs.loadCache();

      assertEquals(1, results.size());
      Result result = results.peek();
      assertEquals("row1", new String(result.getRow(), StandardCharsets.UTF_8));
      assertEquals(1, result.getMap().size());

      assertTrue(csrs.closed);
    }
  }

  @Test
  public void testNoContextNoRecords() throws Exception {
    ScannerCallableWithReplicas callableWithReplicas = Mockito
        .mock(ScannerCallableWithReplicas.class);

    // While the server return 2 records per RPC, we expect there to be more records.
    scan.setCaching(2);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    try (ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn, rpcFactory, controllerFactory, pool,
        Integer.MAX_VALUE)) {

      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(caller.callWithoutRetries(callableWithReplicas, csrs.getScannerTimeout()))
          .thenReturn(new Result[0]);

      // Server doesn't return the context
      Mockito.when(callableWithReplicas.hasMoreResultsContext()).thenReturn(false);
      // Only have more results the first time
      Mockito.when(callableWithReplicas.getServerHasMoreResults())
          .thenThrow(new RuntimeException("Should not be called"));

      // A mocked HRegionInfo so ClientSmallScanner#nextScanner(...) works right
      HRegionInfo regionInfo = Mockito.mock(HRegionInfo.class);
      Mockito.when(callableWithReplicas.getHRegionInfo()).thenReturn(regionInfo);
      // Trigger the "no more data" branch for #nextScanner(...)
      Mockito.when(regionInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);

      csrs.loadCache();

      assertEquals(0, csrs.cache.size());
      assertTrue(csrs.closed);
    }
  }

  @Test
  public void testContextNoRecords() throws Exception {
    ScannerCallableWithReplicas callableWithReplicas = Mockito
        .mock(ScannerCallableWithReplicas.class);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    try (ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn, rpcFactory, controllerFactory, pool,
        Integer.MAX_VALUE)) {

      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(caller.callWithoutRetries(callableWithReplicas, csrs.getScannerTimeout()))
          .thenReturn(new Result[0]);

      // Server doesn't return the context
      Mockito.when(callableWithReplicas.hasMoreResultsContext()).thenReturn(true);
      // Only have more results the first time
      Mockito.when(callableWithReplicas.getServerHasMoreResults())
          .thenReturn(false);

      // A mocked HRegionInfo so ClientSmallScanner#nextScanner(...) works right
      HRegionInfo regionInfo = Mockito.mock(HRegionInfo.class);
      Mockito.when(callableWithReplicas.getHRegionInfo()).thenReturn(regionInfo);
      // Trigger the "no more data" branch for #nextScanner(...)
      Mockito.when(regionInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);

      csrs.loadCache();

      assertEquals(0, csrs.cache.size());
      assertTrue(csrs.closed);
    }
  }
}
