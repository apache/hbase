/*
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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Category(MediumTests.class)
public class TestAsyncProcess {
  private static final TableName DUMMY_TABLE =
      TableName.valueOf("DUMMY_TABLE");
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static final byte[] DUMMY_BYTES_3 = "DUMMY_BYTES_3".getBytes();
  private static final byte[] FAILS = "FAILS".getBytes();
  private static final Configuration conf = new Configuration();

  private static ServerName sn = ServerName.valueOf("localhost:10,1254");
  private static ServerName sn2 = ServerName.valueOf("localhost:140,12540");
  private static HRegionInfo hri1 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_1, DUMMY_BYTES_2, false, 1);
  private static HRegionInfo hri2 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_2, HConstants.EMPTY_END_ROW, false, 2);
  private static HRegionInfo hri3 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_3, HConstants.EMPTY_END_ROW, false, 3);
  private static HRegionLocation loc1 = new HRegionLocation(hri1, sn);
  private static HRegionLocation loc2 = new HRegionLocation(hri2, sn);
  private static HRegionLocation loc3 = new HRegionLocation(hri3, sn2);

  private static final String success = "success";
  private static Exception failure = new Exception("failure");

  static class CountingThreadFactory implements ThreadFactory {
    final AtomicInteger nbThreads;
    ThreadFactory realFactory =  Threads.newDaemonThreadFactory("test-TestAsyncProcess");
    @Override
    public Thread newThread(Runnable r) {
      nbThreads.incrementAndGet();
      return realFactory.newThread(r);
    }

    CountingThreadFactory(AtomicInteger nbThreads){
      this.nbThreads = nbThreads;
    }
  }

  static class MyAsyncProcess<Res> extends AsyncProcess<Res> {
    final AtomicInteger nbMultiResponse = new AtomicInteger();
    final AtomicInteger nbActions = new AtomicInteger();

    public MyAsyncProcess(HConnection hc, AsyncProcessCallback<Res> callback, Configuration conf) {
      this(hc, callback, conf, new AtomicInteger());
    }

      public MyAsyncProcess(HConnection hc, AsyncProcessCallback<Res> callback, Configuration conf,
                          AtomicInteger nbThreads) {
      super(hc, DUMMY_TABLE, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new CountingThreadFactory(nbThreads)),
          callback, conf, new RpcRetryingCallerFactory(conf), new RpcControllerFactory(conf));
    }

    @Override
    protected RpcRetryingCaller<MultiResponse> createCaller(
      PayloadCarryingServerCallable callable) {
      if (!(callable instanceof MultiServerCallable)) {
        throw new RuntimeException(
          "can't cast PayloadCarryingServerCallable to be MultiServerCallable!");
      }
      final MultiResponse mr = createMultiResponse(callable.getLocation(),
        ((MultiServerCallable<Row>)callable).getMulti(),
          nbMultiResponse, nbActions);
      return new RpcRetryingCaller<MultiResponse>(100, 10, 9) {
        @Override
        public MultiResponse callWithoutRetries(RetryingCallable<MultiResponse> callable, int to)
        throws IOException, RuntimeException {
          try {
            // sleep one second in order for threadpool to start another thread instead of reusing
            // existing one. 
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // ignore error
          }
          return mr;
        }
      };
    }
  }

  static class CallerWithFailure extends RpcRetryingCaller<MultiResponse>{

    public CallerWithFailure() {
      super(100, 100, 9);
    }

    @Override
    public MultiResponse callWithoutRetries(RetryingCallable<MultiResponse> callable, int to)
      throws IOException, RuntimeException {
      throw new IOException("test");
    }
  }

  static class AsyncProcessWithFailure<Res> extends MyAsyncProcess<Res> {

    public AsyncProcessWithFailure(HConnection hc, Configuration conf) {
      super(hc, null, conf, new AtomicInteger());
      serverTrackerTimeout = 1;
    }

    @Override
    protected RpcRetryingCaller<MultiResponse> createCaller(
      PayloadCarryingServerCallable callable) {
      return new CallerWithFailure();
    }
  }

  static MultiResponse createMultiResponse(final HRegionLocation loc,
      final MultiAction<Row> multi, AtomicInteger nbMultiResponse, AtomicInteger nbActions) {
    final MultiResponse mr = new MultiResponse();
    nbMultiResponse.incrementAndGet();
    for (Map.Entry<byte[], List<Action<Row>>> entry : multi.actions.entrySet()) {
      for (Action a : entry.getValue()) {
        nbActions.incrementAndGet();
        if (Arrays.equals(FAILS, a.getAction().getRow())) {
          mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), failure);
        } else {
          mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), success);
        }
      }
    }
    return mr;
  }
  /**
   * Returns our async process.
   */
  static class MyConnectionImpl extends HConnectionManager.HConnectionImplementation {
    MyAsyncProcess<?> ap;
    final AtomicInteger nbThreads = new AtomicInteger(0);
    final static Configuration c = new Configuration();

    static {
      c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    }

    protected MyConnectionImpl() {
      super(c);
    }

    protected MyConnectionImpl(Configuration conf) {
      super(conf);
    }

    @Override
    protected <R> AsyncProcess createAsyncProcess(TableName tableName,
                                                  ExecutorService pool,
                                                  AsyncProcess.AsyncProcessCallback<R> callback,
                                                  Configuration confn ) {
      ap = new MyAsyncProcess<R>(this, callback, c, nbThreads);
      return ap;
    }

    @Override
    public HRegionLocation locateRegion(final TableName tableName,
                                        final byte[] row) {
      return loc1;
    }

    @Override
    public boolean hasCellBlockSupport() {
      return false;
    }
  }

  /**
   * Returns our async process.
   */
  static class MyConnectionImpl2 extends MyConnectionImpl {
    List<HRegionLocation> hrl;
    final boolean usedRegions[];

    protected MyConnectionImpl2(List<HRegionLocation> hrl) {
      super(c);
      this.hrl = hrl;
      this.usedRegions = new boolean[hrl.size()];
    }

    @Override
    public HRegionLocation locateRegion(final TableName tableName,
                                        final byte[] row) {
      int i = 0;
      for (HRegionLocation hr:hrl){
        if (Arrays.equals(row, hr.getRegionInfo().getStartKey())){
            usedRegions[i] = true;
          return hr;
        }
        i++;
      }
      return null;
    }
  }

  @Test
  public void testSubmit() throws Exception {
    HConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testSubmitWithCB() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());

    while (!(mcb.successCalled.get() == 1) && !ap.hasError()) {
      Thread.sleep(1);
    }
    Assert.assertEquals(mcb.successCalled.get(), 1);
  }

  @Test
  public void testSubmitBusyRegion() throws Exception {
    HConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.incTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    ap.submit(puts, false);
    Assert.assertEquals(puts.size(), 1);

    ap.decTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());
  }


  @Test
  public void testSubmitBusyRegionServer() throws Exception {
    HConnection hc = createHConnection();
    AsyncProcess<Object> ap = new MyAsyncProcess<Object>(hc, null, conf);

    ap.taskCounterPerServer.put(sn2, new AtomicInteger(ap.maxConcurrentTasksPerServer));

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));
    puts.add(createPut(3, true)); // <== this one won't be taken, the rs is busy
    puts.add(createPut(1, true)); // <== this one will make it, the region is already in
    puts.add(createPut(2, true)); // <== new region, but the rs is ok

    ap.submit(puts, false);
    Assert.assertEquals(" puts=" + puts, 1, puts.size());

    ap.taskCounterPerServer.put(sn2, new AtomicInteger(ap.maxConcurrentTasksPerServer - 1));
    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testFail() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, false);
    puts.add(p);

    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());

    while (!ap.hasError()) {
      Thread.sleep(1);
    }

    Assert.assertEquals(0, mcb.successCalled.get());
    Assert.assertEquals(2, mcb.retriableFailure.get());
    Assert.assertEquals(1, mcb.failureCalled.get());

    Assert.assertEquals(1, ap.getErrors().exceptions.size());
    Assert.assertTrue("was: " + ap.getErrors().exceptions.get(0),
        failure.equals(ap.getErrors().exceptions.get(0)));
    Assert.assertTrue("was: " + ap.getErrors().exceptions.get(0),
        failure.equals(ap.getErrors().exceptions.get(0)));

    Assert.assertEquals(1, ap.getFailedOperations().size());
    Assert.assertTrue("was: " + ap.getFailedOperations().get(0),
        p.equals(ap.getFailedOperations().get(0)));
  }

  @Test
  public void testWaitForNextTaskDone() throws IOException {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    final AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ap.tasksSent.incrementAndGet();

    final AtomicBoolean checkPoint = new AtomicBoolean(false);
    final AtomicBoolean checkPoint2 = new AtomicBoolean(false);

    Thread t = new Thread(){
      @Override
      public void run(){
        Threads.sleep(1000);
        Assert.assertFalse(checkPoint.get());
        ap.tasksDone.incrementAndGet();
        checkPoint2.set(true);
      }
    };

    t.start();
    ap.waitForNextTaskDone(0);
    checkPoint.set(true);
    while (!checkPoint2.get()){
      Threads.sleep(1);
    }
  }

  @Test
  public void testSubmitTrue() throws IOException {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    final AsyncProcess<Object> ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ap.tasksSent.incrementAndGet();
    final AtomicInteger ai = new AtomicInteger(1);
    ap.taskCounterPerRegion.put(hri1.getRegionName(), ai);

    final AtomicBoolean checkPoint = new AtomicBoolean(false);
    final AtomicBoolean checkPoint2 = new AtomicBoolean(false);

    Thread t = new Thread(){
      @Override
      public void run(){
        Threads.sleep(1000);
        Assert.assertFalse(checkPoint.get());
        ai.decrementAndGet();
        ap.tasksDone.incrementAndGet();
        checkPoint2.set(true);
      }
    };

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, true);
    puts.add(p);

    ap.submit(puts, false);
    Assert.assertFalse(puts.isEmpty());

    t.start();

    ap.submit(puts, true);
    Assert.assertTrue(puts.isEmpty());

    checkPoint.set(true);
    while (!checkPoint2.get()){
      Threads.sleep(1);
    }
  }

  @Test
  public void testFailAndSuccess() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());

    while (!ap.hasError()) {
      Thread.sleep(1);
    }
    ap.waitUntilDone();
 
    Assert.assertEquals(mcb.successCalled.get(), 2);
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    Assert.assertEquals(1, ap.getErrors().actions.size());


    puts.add(createPut(1, true));
    ap.submit(puts, false);
    Assert.assertTrue(puts.isEmpty());

    while (mcb.successCalled.get() != 3) {
      Thread.sleep(1);
    }
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    ap.clearErrors();
    Assert.assertTrue(ap.getErrors().actions.isEmpty());
  }

  @Test
  public void testFlush() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    ap.submit(puts, false);
    ap.waitUntilDone();

    Assert.assertEquals(mcb.successCalled.get(), 2);
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    Assert.assertEquals(1, ap.getFailedOperations().size());
  }

  @Test
  public void testMaxTask() throws Exception {
    HConnection hc = createHConnection();
    final AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    for (int i = 0; i < 1000; i++) {
      ap.incTaskCounters(Arrays.asList("dummy".getBytes()), sn);
    }

    final Thread myThread = Thread.currentThread();

    Thread t = new Thread() {
      public void run() {
        Threads.sleep(2000);
        myThread.interrupt();
      }
    };

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    t.start();

    try {
      ap.submit(puts, false);
      Assert.fail("We should have been interrupted.");
    } catch (InterruptedIOException expected) {
    }

    final long sleepTime = 2000;

    Thread t2 = new Thread() {
      public void run() {
        Threads.sleep(sleepTime);
        while (ap.tasksDone.get() > 0) {
          ap.decTaskCounters(Arrays.asList("dummy".getBytes()), sn);
        }
      }
    };
    t2.start();

    long start = System.currentTimeMillis();
    ap.submit(new ArrayList<Row>(), false);
    long end = System.currentTimeMillis();

    //Adds 100 to secure us against approximate timing.
    Assert.assertTrue(start + 100L + sleepTime > end);
  }


  private class MyCB implements AsyncProcess.AsyncProcessCallback<Object> {
    private final AtomicInteger successCalled = new AtomicInteger(0);
    private final AtomicInteger failureCalled = new AtomicInteger(0);
    private final AtomicInteger retriableFailure = new AtomicInteger(0);


    @Override
    public void success(int originalIndex, byte[] region, Row row, Object o) {
      successCalled.incrementAndGet();
    }

    @Override
    public boolean failure(int originalIndex, byte[] region, Row row, Throwable t) {
      failureCalled.incrementAndGet();
      return true;
    }

    @Override
    public boolean retriableFailure(int originalIndex, Row row, byte[] region,
                                    Throwable exception) {
      // We retry once only.
      return (retriableFailure.incrementAndGet() < 2);
    }
  }


  private static HConnection createHConnection() throws IOException {
    HConnection hc = Mockito.mock(HConnection.class);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_1), Mockito.anyBoolean())).thenReturn(loc1);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_1))).thenReturn(loc1);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_2), Mockito.anyBoolean())).thenReturn(loc2);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_2))).thenReturn(loc2);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_3), Mockito.anyBoolean())).thenReturn(loc2);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_3))).thenReturn(loc3);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(FAILS), Mockito.anyBoolean())).thenReturn(loc2);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(FAILS))).thenReturn(loc2);

    return hc;
  }

  @Test
  public void testHTablePutSuccess() throws Exception {
    HTable ht = Mockito.mock(HTable.class);
    HConnection hc = createHConnection();
    ht.ap = new MyAsyncProcess<Object>(hc, null, conf);

    Put put = createPut(1, true);

    Assert.assertEquals(0, ht.getWriteBufferSize());
    ht.put(put);
    Assert.assertEquals(0, ht.getWriteBufferSize());
  }

  private void doHTableFailedPut(boolean bufferOn) throws Exception {
    HTable ht = new HTable();
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB(); // This allows to have some hints on what's going on.
    ht.ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ht.setAutoFlush(true, true);
    if (bufferOn) {
      ht.setWriteBufferSize(1024L * 1024L);
    } else {
      ht.setWriteBufferSize(0L);
    }

    Put put = createPut(1, false);

    Assert.assertEquals(0L, ht.currentWriteBufferSize);
    try {
      ht.put(put);
      if (bufferOn) {
        ht.flushCommits();
      }
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals(0L, ht.currentWriteBufferSize);
    Assert.assertEquals(0, mcb.successCalled.get());
    Assert.assertEquals(2, mcb.retriableFailure.get());
    Assert.assertEquals(1, mcb.failureCalled.get());

    // This should not raise any exception, puts have been 'received' before by the catch.
    ht.close();
  }

  @Test
  public void testHTableFailedPutWithBuffer() throws Exception {
    doHTableFailedPut(true);
  }

  @Test
  public void doHTableFailedPutWithoutBuffer() throws Exception {
    doHTableFailedPut(false);
  }

  @Test
  public void testHTableFailedPutAndNewPut() throws Exception {
    HTable ht = new HTable();
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB(); // This allows to have some hints on what's going on.
    ht.ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ht.setAutoFlush(false, true);
    ht.setWriteBufferSize(0);

    Put p = createPut(1, false);
    ht.put(p);

    ht.ap.waitUntilDone(); // Let's do all the retries.

    // We're testing that we're behaving as we were behaving in 0.94: sending exceptions in the
    //  doPut if it fails.
    // This said, it's not a very easy going behavior. For example, when we insert a list of
    //  puts, we may raise an exception in the middle of the list. It's then up to the caller to
    //  manage what was inserted, what was tried but failed, and what was not even tried.
    p = createPut(1, true);
    Assert.assertEquals(0, ht.writeAsyncBuffer.size());
    try {
      ht.put(p);
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals("the put should not been inserted.", 0, ht.writeAsyncBuffer.size());
  }


  @Test
  public void testWithNoClearOnFail() throws IOException {
    HTable ht = new HTable();
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    ht.ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ht.setAutoFlush(false, false);

    Put p = createPut(1, false);
    ht.put(p);
    Assert.assertEquals(0, ht.writeAsyncBuffer.size());
    try {
      ht.flushCommits();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    Assert.assertEquals(1, ht.writeAsyncBuffer.size());

    try {
      ht.close();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    Assert.assertEquals(1, ht.writeAsyncBuffer.size());
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    HTable ht = new HTable();
    ht.connection = new MyConnectionImpl();

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));
    puts.add(createPut(1, false)); // <=== the bad apple, position 4
    puts.add(createPut(1, true));
    puts.add(createPut(1, false)); // <=== another bad apple, position 6

    Object[] res = new Object[puts.size()];
    try {
      ht.processBatch(puts, res);
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }

    Assert.assertEquals(res[0], success);
    Assert.assertEquals(res[1], success);
    Assert.assertEquals(res[2], success);
    Assert.assertEquals(res[3], success);
    Assert.assertEquals(res[4], failure);
    Assert.assertEquals(res[5], success);
    Assert.assertEquals(res[6], failure);
  }

  @Test
  public void testErrorsServers() throws IOException {
    HTable ht = new HTable();
    Configuration configuration = new Configuration(conf);
    configuration.setBoolean(HConnectionManager.RETRIES_BY_SERVER_KEY, true);
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    // set default writeBufferSize
    ht.setWriteBufferSize(configuration.getLong("hbase.client.write.buffer", 2097152));

    MyConnectionImpl mci = new MyConnectionImpl(configuration);
    ht.connection = mci;
    ht.ap = new MyAsyncProcess<Object>(mci, null, configuration);


    Assert.assertNotNull(ht.ap.createServerErrorTracker());
    Assert.assertTrue(ht.ap.serverTrackerTimeout > 200);
    ht.ap.serverTrackerTimeout = 1;

    Put p = createPut(1, false);
    ht.setAutoFlush(false, false);
    ht.put(p);

    try {
      ht.flushCommits();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    // Checking that the ErrorsServers came into play and didn't make us stop immediately
    Assert.assertEquals(ht.ap.tasksSent.get(), 3);
  }

  @Test
  public void testGlobalErrors() throws IOException {
    HTable ht = new HTable();
    Configuration configuration = new Configuration(conf);
    configuration.setBoolean(HConnectionManager.RETRIES_BY_SERVER_KEY, true);
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    ht.connection = new MyConnectionImpl(configuration);
    AsyncProcessWithFailure<Object> ap =
      new AsyncProcessWithFailure<Object>(ht.connection, configuration);
    ht.ap = ap;

    Assert.assertNotNull(ht.ap.createServerErrorTracker());

    Put p = createPut(1, true);
    ht.setAutoFlush(false, false);
    ht.put(p);

    try {
      ht.flushCommits();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    // Checking that the ErrorsServers came into play and didn't make us stop immediately
    Assert.assertEquals(3, ht.ap.tasksSent.get());
  }


  /**
   * This test simulates multiple regions on 2 servers. We should have 2 multi requests and
   *  2 threads: 1 per server, this whatever the number of regions.
   */
  @Test
  public void testThreadCreation() throws Exception {
    final int NB_REGS = 100;
    List<HRegionLocation> hrls = new ArrayList<HRegionLocation>(NB_REGS);
    List<Get> gets = new ArrayList<Get>(NB_REGS);
    for (int i = 0; i < NB_REGS; i++) {
      HRegionInfo hri = new HRegionInfo(
          DUMMY_TABLE, Bytes.toBytes(i * 10L), Bytes.toBytes(i * 10L + 9L), false, i);
      HRegionLocation hrl = new HRegionLocation(hri, i % 2 == 0 ? sn : sn2);
      hrls.add(hrl);

      Get get = new Get(Bytes.toBytes(i * 10L));
      gets.add(get);
    }

    HTable ht = new HTable();
    MyConnectionImpl2 con = new MyConnectionImpl2(hrls);
    ht.connection = con;

    ht.batch(gets);

    Assert.assertEquals(con.ap.nbActions.get(), NB_REGS);
    Assert.assertEquals("1 multi response per server", 2, con.ap.nbMultiResponse.get());
    Assert.assertEquals("1 thread per server", 2, con.nbThreads.get());

    int nbReg = 0;
    for (int i =0; i<NB_REGS; i++){
      if (con.usedRegions[i]) nbReg++;
    }
    Assert.assertEquals("nbReg=" + nbReg, nbReg, NB_REGS);
  }


  /**
   * @param regCnt  the region: 1 to 3.
   * @param success if true, the put will succeed.
   * @return a put
   */
  private Put createPut(int regCnt, boolean success) {
    Put p;
    if (!success) {
      p = new Put(FAILS);
    } else switch (regCnt){
      case 1 :
        p = new Put(DUMMY_BYTES_1);
        break;
      case 2:
        p = new Put(DUMMY_BYTES_2);
        break;
      case 3:
        p = new Put(DUMMY_BYTES_3);
        break;
      default:
        throw new IllegalArgumentException("unknown " + regCnt);
    }

    p.add(DUMMY_BYTES_1, DUMMY_BYTES_1, DUMMY_BYTES_1);

    return p;
  }

  static class MyThreadPoolExecutor extends ThreadPoolExecutor {
    public MyThreadPoolExecutor(int coreThreads, int maxThreads, long keepAliveTime,
        TimeUnit timeunit, BlockingQueue<Runnable> blockingqueue) {
      super(coreThreads, maxThreads, keepAliveTime, timeunit, blockingqueue);
    }

    @Override
    public Future submit(Runnable runnable) {
      throw new OutOfMemoryError("OutOfMemory error thrown by means");
    }
  }

  static class AsyncProcessForThrowableCheck extends AsyncProcess {
    public AsyncProcessForThrowableCheck(HConnection hc, Configuration conf, ExecutorService pool) {
      super(hc, DUMMY_TABLE, pool, null, conf, new RpcRetryingCallerFactory(conf),
          new RpcControllerFactory(conf));
    }
  }

  @Test
  public void testUncheckedException() throws Exception {
    // Test the case pool.submit throws unchecked exception
    HConnection hc = createHConnection();
    MyThreadPoolExecutor myPool =
        new MyThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(200));
    Configuration myConf = new Configuration(conf);
    myConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    AsyncProcess ap = new AsyncProcessForThrowableCheck(hc, myConf, myPool);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(puts, false, null);
    Assert.assertTrue(puts.isEmpty());
  }
}
