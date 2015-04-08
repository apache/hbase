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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientSmallScanner.SmallScannerCallableFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the ClientSmallReversedScanner.
 */
@Category(SmallTests.class)
public class TestClientSmallReversedScanner {

  Scan scan;
  ExecutorService pool;
  Configuration conf;

  HConnection clusterConn;
  RpcRetryingCallerFactory rpcFactory;
  RpcControllerFactory controllerFactory;
  RpcRetryingCaller<Result[]> caller;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws IOException {
    clusterConn = Mockito.mock(HConnection.class);
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
      final RegionServerCallable<Result[]> callableWithReplicas) {
    return new SmallScannerCallableFactory() {
      @Override
      public RegionServerCallable<Result[]> getCallable(final Scan sc, HConnection connection,
          TableName table, byte[] localStartKey, final int cacheNum,
          final RpcControllerFactory rpcControllerFactory) {
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

    @SuppressWarnings("unchecked")
    RegionServerCallable<Result[]> callableWithReplicas = Mockito
        .mock(RegionServerCallable.class);

    // Mock out the RpcCaller
    @SuppressWarnings("unchecked")
    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);
    // Return the mock from the factory
    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);

    // Intentionally leave a "default" caching size in the Scan. No matter the value, we
    // should continue based on the server context

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn);

    try {
      csrs.setRpcRetryingCaller(caller);
      csrs.setRpcControllerFactory(controllerFactory);
      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(
          caller.callWithRetries(callableWithReplicas,
              HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)).thenAnswer(
          new Answer<Result[]>() {
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

      List<Result> results = csrs.cache;
      Iterator<Result> iter = results.iterator();
      assertEquals(3, results.size());
      for (int i = 3; i >= 1 && iter.hasNext(); i--) {
        Result result = iter.next();
        byte[] row = result.getRow();
        assertEquals("row" + i, new String(row, "UTF-8"));
        assertEquals(1, result.getMap().size());
      }
      assertTrue(csrs.closed);
    } finally {
      csrs.close();
    }
  }

  @Test
  public void testNoContextFewerRecords() throws Exception {
    final KeyValue kv1 = new KeyValue("row1".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv2 = new KeyValue("row2".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum), kv3 = new KeyValue("row3".getBytes(), "cf".getBytes(), "cq".getBytes(), 1,
        Type.Maximum);

    @SuppressWarnings("unchecked")
    RegionServerCallable<Result[]> callableWithReplicas = Mockito
        .mock(RegionServerCallable.class);

    // While the server returns 2 records per batch, we expect more records.
    scan.setCaching(2);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn);

    try {
      csrs.setRpcRetryingCaller(caller);
      csrs.setRpcControllerFactory(controllerFactory);
      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(
          caller.callWithRetries(callableWithReplicas,
              HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)).thenAnswer(
          new Answer<Result[]>() {
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

      List<Result> results = csrs.cache;
      Iterator<Result> iter = results.iterator();
      assertEquals(2, results.size());
      for (int i = 3; i >= 2 && iter.hasNext(); i--) {
        Result result = iter.next();
        byte[] row = result.getRow();
        assertEquals("row" + i, new String(row, "UTF-8"));
        assertEquals(1, result.getMap().size());
      }

      // "consume" the Results
      results.clear();

      csrs.loadCache();

      assertEquals(1, results.size());
      Result result = results.get(0);
      assertEquals("row1", new String(result.getRow(), "UTF-8"));
      assertEquals(1, result.getMap().size());

      assertTrue(csrs.closed);
    } finally {
      csrs.close();
    }
  }

  @Test
  public void testNoContextNoRecords() throws Exception {
    @SuppressWarnings("unchecked")
    RegionServerCallable<Result[]> callableWithReplicas = Mockito
        .mock(RegionServerCallable.class);

    // While the server return 2 records per RPC, we expect there to be more records.
    scan.setCaching(2);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn);

    try {
      csrs.setRpcRetryingCaller(caller);
      csrs.setRpcControllerFactory(controllerFactory);
      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(
          caller.callWithRetries(callableWithReplicas,
              HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)).thenReturn(new Result[0]);

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
    } finally {
      csrs.close();
    }
  }

  @Test
  public void testContextNoRecords() throws Exception {
    @SuppressWarnings("unchecked")
    RegionServerCallable<Result[]> callableWithReplicas = Mockito
        .mock(RegionServerCallable.class);

    SmallScannerCallableFactory factory = getFactory(callableWithReplicas);

    ClientSmallReversedScanner csrs = new ClientSmallReversedScanner(conf, scan,
        TableName.valueOf("table"), clusterConn);

    try {
      csrs.setRpcRetryingCaller(caller);
      csrs.setRpcControllerFactory(controllerFactory);
      csrs.setScannerCallableFactory(factory);

      // Return some data the first time, less the second, and none after that
      Mockito.when(
          caller.callWithRetries(callableWithReplicas,
              HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)).thenReturn(new Result[0]);

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
    } finally {
      csrs.close();
    }
  }
}
