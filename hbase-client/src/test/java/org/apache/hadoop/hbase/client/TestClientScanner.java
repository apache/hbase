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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the ClientScanner.
 */
@Category(SmallTests.class)
public class TestClientScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientScanner.class);

  Scan scan;
  ExecutorService pool;
  Configuration conf;

  ConnectionImplementation clusterConn;
  RpcRetryingCallerFactory rpcFactory;
  RpcControllerFactory controllerFactory;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws IOException {
    clusterConn = Mockito.mock(ConnectionImplementation.class);
    rpcFactory = Mockito.mock(RpcRetryingCallerFactory.class);
    controllerFactory = Mockito.mock(RpcControllerFactory.class);
    pool = Executors.newSingleThreadExecutor();
    scan = new Scan();
    conf = new Configuration();
    Mockito.when(clusterConn.getConfiguration()).thenReturn(conf);
  }

  @After
  public void teardown() {
    if (null != pool) {
      pool.shutdownNow();
    }
  }

  private static class MockClientScanner extends ClientSimpleScanner {

    private boolean rpcFinished = false;
    private boolean rpcFinishedFired = false;
    private boolean initialized = false;

    public MockClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
        ConnectionImplementation connection, RpcRetryingCallerFactory rpcFactory,
        RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout)
        throws IOException {
      super(conf, scan, tableName, connection, rpcFactory, controllerFactory, pool,
        primaryOperationTimeout);
    }

    @Override
    protected boolean moveToNextRegion() {
      if (!initialized) {
        initialized = true;
        return super.moveToNextRegion();
      }
      if (!rpcFinished) {
        return super.moveToNextRegion();
      }
      // Enforce that we don't short-circuit more than once
      if (rpcFinishedFired) {
        throw new RuntimeException("Expected nextScanner to only be called once after " +
            " short-circuit was triggered.");
      }
      rpcFinishedFired = true;
      return false;
    }

    public void setRpcFinished(boolean rpcFinished) {
      this.rpcFinished = rpcFinished;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoResultsHint() throws IOException {
    final Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});

    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(),
      Mockito.anyInt())).thenAnswer(new Answer<Result[]>() {
        private int count = 0;
        @Override
        public Result[] answer(InvocationOnMock invocation) throws Throwable {
            ScannerCallableWithReplicas callable = invocation.getArgument(0);
          switch (count) {
            case 0: // initialize
              count++;
              callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.UNKNOWN);
              return results;
            case 1: // detect no more results
            case 2: // close
              count++;
              return new Result[0];
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {

      scanner.setRpcFinished(true);

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      // One for fetching the results
      // One for fetching empty results and quit as we do not have moreResults hint.
      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSizeLimit() throws IOException {
    final Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});

    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(),
      Mockito.anyInt())).thenAnswer(new Answer<Result[]>() {
        private int count = 0;
        @Override
        public Result[] answer(InvocationOnMock invocation) throws Throwable {
          ScannerCallableWithReplicas callable = invocation.getArgument(0);
          switch (count) {
            case 0: // initialize
              count++;
              // if we set no here the implementation will trigger a close
              callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.YES);
              return results;
            case 1: // close
              count++;
              return null;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);

    // Set a much larger cache
    scan.setCaching(100);
    // The single key-value will exit the loop
    scan.setMaxResultSize(1);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {
      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(1)).callWithoutRetries(
          Mockito.any(), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCacheLimit() throws IOException {
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("row2"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    KeyValue kv3 = new KeyValue(Bytes.toBytes("row3"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    final Result[] results = new Result[] {Result.create(new Cell[] {kv1}),
        Result.create(new Cell[] {kv2}), Result.create(new Cell[] {kv3})};

    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(),
      Mockito.anyInt())).thenAnswer(new Answer<Result[]>() {
        private int count = 0;
        @Override
        public Result[] answer(InvocationOnMock invocation) throws Throwable {
          ScannerCallableWithReplicas callable = invocation.getArgument(0);
          switch (count) {
            case 0: // initialize
              count++;
              // if we set no here the implementation will trigger a close
              callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.YES);
              return results;
            case 1: // close
              count++;
              return null;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);

    // Set a small cache
    scan.setCaching(1);
    // Set a very large size
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {
      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(1)).callWithoutRetries(
          Mockito.any(), Mockito.anyInt());

      assertEquals(3, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv2, cs.current());
      assertFalse(cs.advance());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv3, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoMoreResults() throws IOException {
    final Result[] results = new Result[1];
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    results[0] = Result.create(new Cell[] {kv1});

    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(),
      Mockito.anyInt())).thenAnswer(new Answer<Result[]>() {
        private int count = 0;
        @Override
        public Result[] answer(InvocationOnMock invocation) throws Throwable {
          ScannerCallableWithReplicas callable = invocation.getArgument(0);
          switch (count) {
            case 0: // initialize
              count++;
              callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.NO);
              return results;
            case 1: // close
              count++;
              return null;
            default:
              throw new RuntimeException("Expected only 2 invocations");
          }
        }
    });

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {
      scanner.setRpcFinished(true);

      InOrder inOrder = Mockito.inOrder(caller);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(1)).callWithoutRetries(
          Mockito.any(), Mockito.anyInt());

      assertEquals(1, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoreResults() throws IOException {
    final Result[] results1 = new Result[1];
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    results1[0] = Result.create(new Cell[] {kv1});

    final Result[] results2 = new Result[1];
    KeyValue kv2 = new KeyValue(Bytes.toBytes("row2"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
        Type.Maximum);
    results2[0] = Result.create(new Cell[] {kv2});


    RpcRetryingCaller<Result[]> caller = Mockito.mock(RpcRetryingCaller.class);

    Mockito.when(rpcFactory.<Result[]> newCaller()).thenReturn(caller);
    Mockito.when(caller.callWithoutRetries(Mockito.any(),
        Mockito.anyInt())).thenAnswer(new Answer<Result[]>() {
          private int count = 0;
          @Override
          public Result[] answer(InvocationOnMock invocation) throws Throwable {
            ScannerCallableWithReplicas callable = invocation.getArgument(0);
            switch (count) {
              case 0: // initialize
                count++;
                callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.YES);
                return results1;
              case 1:
                count++;
                // The server reports back false WRT more results
                callable.currentScannerCallable.setMoreResultsInRegion(MoreResults.NO);
                return results2;
              case 2: // close
                count++;
                return null;
              default:
                throw new RuntimeException("Expected only 3 invocations");
            }
          }
      });

    // Set a much larger cache and buffer size than we'll provide
    scan.setCaching(100);
    scan.setMaxResultSize(1000*1000);

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
        clusterConn, rpcFactory, controllerFactory, pool, Integer.MAX_VALUE)) {
      InOrder inOrder = Mockito.inOrder(caller);
      scanner.setRpcFinished(true);

      scanner.loadCache();

      inOrder.verify(caller, Mockito.times(2)).callWithoutRetries(
          Mockito.any(), Mockito.anyInt());

      assertEquals(2, scanner.cache.size());
      Result r = scanner.cache.poll();
      assertNotNull(r);
      CellScanner cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv1, cs.current());
      assertFalse(cs.advance());

      r = scanner.cache.poll();
      assertNotNull(r);
      cs = r.cellScanner();
      assertTrue(cs.advance());
      assertEquals(kv2, cs.current());
      assertFalse(cs.advance());
    }
  }

  /**
   * Tests the case where all replicas of a region throw an exception. It should not cause a hang
   * but the exception should propagate to the client
   */
  @Test
  public void testExceptionsFromReplicasArePropagated() throws IOException {
    scan.setConsistency(Consistency.TIMELINE);

    // Mock a caller which calls the callable for ScannerCallableWithReplicas,
    // but throws an exception for the actual scanner calls via callWithRetries.
    rpcFactory = new MockRpcRetryingCallerFactory(conf);
    conf.set(RpcRetryingCallerFactory.CUSTOM_CALLER_CONF_KEY,
      MockRpcRetryingCallerFactory.class.getName());

    // mock 3 replica locations
    when(clusterConn.locateRegion((TableName)any(), (byte[])any(), anyBoolean(),
      anyBoolean(), anyInt())).thenReturn(new RegionLocations(null, null, null));

    try (MockClientScanner scanner = new MockClientScanner(conf, scan, TableName.valueOf(name.getMethodName()),
      clusterConn, rpcFactory, new RpcControllerFactory(conf), pool, Integer.MAX_VALUE)) {
      Iterator<Result> iter = scanner.iterator();
      while (iter.hasNext()) {
        iter.next();
      }
      fail("Should have failed with RetriesExhaustedException");
    } catch (RuntimeException expected) {
      assertThat(expected.getCause(), instanceOf(RetriesExhaustedException.class));
    }
  }

  public static class MockRpcRetryingCallerFactory extends RpcRetryingCallerFactory {

    public MockRpcRetryingCallerFactory(Configuration conf) {
      super(conf);
    }

    @Override
    public <T> RpcRetryingCaller<T> newCaller() {
      return new RpcRetryingCaller<T>() {
        @Override
        public void cancel() {
        }
        @Override
        public T callWithRetries(RetryingCallable<T> callable, int callTimeout)
            throws IOException, RuntimeException {
          throw new IOException("Scanner exception");
        }

        @Override
        public T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
            throws IOException, RuntimeException {
          try {
            return callable.call(callTimeout);
          } catch (IOException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }
}
