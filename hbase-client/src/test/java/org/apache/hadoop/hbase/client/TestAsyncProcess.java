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


import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncRequestFuture;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

@Category({ClientTests.class, MediumTests.class})
public class TestAsyncProcess {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
  private final static Log LOG = LogFactory.getLog(TestAsyncProcess.class);
  private static final TableName DUMMY_TABLE =
      TableName.valueOf("DUMMY_TABLE");
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static final byte[] DUMMY_BYTES_3 = "DUMMY_BYTES_3".getBytes();
  private static final byte[] FAILS = "FAILS".getBytes();
  private static final Configuration conf = new Configuration();

  private static ServerName sn = ServerName.valueOf("s1:1,1");
  private static ServerName sn2 = ServerName.valueOf("s2:2,2");
  private static ServerName sn3 = ServerName.valueOf("s3:3,3");
  private static HRegionInfo hri1 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_1, DUMMY_BYTES_2, false, 1);
  private static HRegionInfo hri2 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_2, HConstants.EMPTY_END_ROW, false, 2);
  private static HRegionInfo hri3 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_3, HConstants.EMPTY_END_ROW, false, 3);
  private static HRegionLocation loc1 = new HRegionLocation(hri1, sn);
  private static HRegionLocation loc2 = new HRegionLocation(hri2, sn);
  private static HRegionLocation loc3 = new HRegionLocation(hri3, sn2);

  // Replica stuff
  private static HRegionInfo hri1r1 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 1),
      hri1r2 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 2);
  private static HRegionInfo hri2r1 = RegionReplicaUtil.getRegionInfoForReplica(hri2, 1);
  private static RegionLocations hrls1 = new RegionLocations(new HRegionLocation(hri1, sn),
      new HRegionLocation(hri1r1, sn2), new HRegionLocation(hri1r2, sn3));
  private static RegionLocations hrls2 = new RegionLocations(new HRegionLocation(hri2, sn2),
      new HRegionLocation(hri2r1, sn3));
  private static RegionLocations hrls3 = new RegionLocations(new HRegionLocation(hri3, sn3), null);

  private static final String success = "success";
  private static Exception failure = new Exception("failure");

  private static int NB_RETRIES = 3;

  @BeforeClass
  public static void beforeClass(){
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, NB_RETRIES);
  }

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

  static class MyAsyncProcess extends AsyncProcess {
    final AtomicInteger nbMultiResponse = new AtomicInteger();
    final AtomicInteger nbActions = new AtomicInteger();
    public List<AsyncRequestFuture> allReqs = new ArrayList<AsyncRequestFuture>();
    public AtomicInteger callsCt = new AtomicInteger();

    @Override
    protected <Res> AsyncRequestFutureImpl<Res> createAsyncRequestFuture(TableName tableName,
        List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
        Batch.Callback<Res> callback, Object[] results, boolean needResults) {
      // Test HTable has tableName of null, so pass DUMMY_TABLE
      AsyncRequestFutureImpl<Res> r = super.createAsyncRequestFuture(
          DUMMY_TABLE, actions, nonceGroup, pool, callback, results, needResults);
      allReqs.add(r);
      return r;
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf) {
      this(hc, conf, new AtomicInteger());
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf, AtomicInteger nbThreads) {
      super(hc, conf, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new CountingThreadFactory(nbThreads)),
            new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(conf));
    }

    public MyAsyncProcess(
        ClusterConnection hc, Configuration conf, boolean useGlobalErrors) {
      super(hc, conf, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new CountingThreadFactory(new AtomicInteger())),
          new RpcRetryingCallerFactory(conf), useGlobalErrors, new RpcControllerFactory(conf));
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf, boolean useGlobalErrors,
        @SuppressWarnings("unused") boolean dummy) {
      super(hc, conf, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
              new SynchronousQueue<Runnable>(), new CountingThreadFactory(new AtomicInteger())) {
        @Override
        public void execute(Runnable command) {
          throw new RejectedExecutionException("test under failure");
        }
      },
          new RpcRetryingCallerFactory(conf), useGlobalErrors, new RpcControllerFactory(conf));
    }

    @Override
    public <Res> AsyncRequestFuture submit(TableName tableName, List<? extends Row> rows,
        boolean atLeastOne, Callback<Res> callback, boolean needResults)
            throws InterruptedIOException {
      // We use results in tests to check things, so override to always save them.
      return super.submit(DUMMY_TABLE, rows, atLeastOne, callback, true);
    }

    @Override
    protected RpcRetryingCaller<MultiResponse> createCaller(
        PayloadCarryingServerCallable callable) {
      callsCt.incrementAndGet();
      MultiServerCallable callable1 = (MultiServerCallable) callable;
      final MultiResponse mr = createMultiResponse(
          callable1.getMulti(), nbMultiResponse, nbActions, new ResponseGenerator() {
            @Override
            public void addResponse(MultiResponse mr, byte[] regionName, Action<Row> a) {
              if (Arrays.equals(FAILS, a.getAction().getRow())) {
                mr.add(regionName, a.getOriginalIndex(), failure);
              } else {
                mr.add(regionName, a.getOriginalIndex(), success);
              }
            }
          });

      return new RpcRetryingCallerImpl<MultiResponse>(100, 10, 9) {
        @Override
        public MultiResponse callWithoutRetries(RetryingCallable<MultiResponse> callable,
                                                int callTimeout)
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

  static class CallerWithFailure extends RpcRetryingCallerImpl<MultiResponse>{

    private final IOException e;

    public CallerWithFailure(IOException e) {
      super(100, 100, 9);
      this.e = e;
    }

    @Override
    public MultiResponse callWithoutRetries(RetryingCallable<MultiResponse> callable,
                                            int callTimeout)
        throws IOException, RuntimeException {
      throw e;
    }
  }


  static class AsyncProcessWithFailure extends MyAsyncProcess {

    private final IOException ioe;

    public AsyncProcessWithFailure(ClusterConnection hc, Configuration conf, IOException ioe) {
      super(hc, conf, true);
      this.ioe = ioe;
      serverTrackerTimeout = 1;
    }

    @Override
    protected RpcRetryingCaller<MultiResponse> createCaller(
      PayloadCarryingServerCallable callable) {
      callsCt.incrementAndGet();
      return new CallerWithFailure(ioe);
    }
  }

  class MyAsyncProcessWithReplicas extends MyAsyncProcess {
    private Set<byte[]> failures = new TreeSet<byte[]>(new Bytes.ByteArrayComparator());
    private long primarySleepMs = 0, replicaSleepMs = 0;
    private Map<ServerName, Long> customPrimarySleepMs = new HashMap<ServerName, Long>();
    private final AtomicLong replicaCalls = new AtomicLong(0);

    public void addFailures(HRegionInfo... hris) {
      for (HRegionInfo hri : hris) {
        failures.add(hri.getRegionName());
      }
    }

    public long getReplicaCallCount() {
      return replicaCalls.get();
    }

    public void setPrimaryCallDelay(ServerName server, long primaryMs) {
      customPrimarySleepMs.put(server, primaryMs);
    }

    public MyAsyncProcessWithReplicas(ClusterConnection hc, Configuration conf) {
      super(hc, conf);
    }

    public void setCallDelays(long primaryMs, long replicaMs) {
      this.primarySleepMs = primaryMs;
      this.replicaSleepMs = replicaMs;
    }

    @Override
    protected RpcRetryingCaller<MultiResponse> createCaller(
        PayloadCarryingServerCallable payloadCallable) {
      MultiServerCallable<Row> callable = (MultiServerCallable) payloadCallable;
      final MultiResponse mr = createMultiResponse(
          callable.getMulti(), nbMultiResponse, nbActions, new ResponseGenerator() {
            @Override
            public void addResponse(MultiResponse mr, byte[] regionName, Action<Row> a) {
              if (failures.contains(regionName)) {
                mr.add(regionName, a.getOriginalIndex(), failure);
              } else {
                boolean isStale = !RegionReplicaUtil.isDefaultReplica(a.getReplicaId());
                mr.add(regionName, a.getOriginalIndex(),
                    Result.create(new Cell[0], null, isStale));
              }
            }
          });
      // Currently AsyncProcess either sends all-replica, or all-primary request.
      final boolean isDefault = RegionReplicaUtil.isDefaultReplica(
          callable.getMulti().actions.values().iterator().next().iterator().next().getReplicaId());
      final ServerName server = ((MultiServerCallable<?>)callable).getServerName();
      String debugMsg = "Call to " + server + ", primary=" + isDefault + " with "
          + callable.getMulti().actions.size() + " entries: ";
      for (byte[] region : callable.getMulti().actions.keySet()) {
        debugMsg += "[" + Bytes.toStringBinary(region) + "], ";
      }
      LOG.debug(debugMsg);
      if (!isDefault) {
        replicaCalls.incrementAndGet();
      }

      return new RpcRetryingCallerImpl<MultiResponse>(100, 10, 9) {
        @Override
        public MultiResponse callWithoutRetries(RetryingCallable<MultiResponse> callable,
                                                int callTimeout)
        throws IOException, RuntimeException {
          long sleep = -1;
          if (isDefault) {
            Long customSleep = customPrimarySleepMs.get(server);
            sleep = (customSleep == null ? primarySleepMs : customSleep.longValue());
          } else {
            sleep = replicaSleepMs;
          }
          if (sleep != 0) {
            try {
              Thread.sleep(sleep);
            } catch (InterruptedException e) {
            }
          }
          return mr;
        }
      };
    }
  }

  static MultiResponse createMultiResponse(final MultiAction<Row> multi,
      AtomicInteger nbMultiResponse, AtomicInteger nbActions, ResponseGenerator gen) {
    final MultiResponse mr = new MultiResponse();
    nbMultiResponse.incrementAndGet();
    for (Map.Entry<byte[], List<Action<Row>>> entry : multi.actions.entrySet()) {
      byte[] regionName = entry.getKey();
      for (Action<Row> a : entry.getValue()) {
        nbActions.incrementAndGet();
        gen.addResponse(mr, regionName, a);
      }
    }
    return mr;
  }

  private static interface ResponseGenerator {
    void addResponse(final MultiResponse mr, byte[] regionName, Action<Row> a);
  }

  /**
   * Returns our async process.
   */
  static class MyConnectionImpl extends ConnectionImplementation {
    public static class TestRegistry implements Registry {
      @Override
      public void init(Connection connection) {}

      @Override
      public RegionLocations getMetaRegionLocation() throws IOException {
        return null;
      }

      @Override
      public String getClusterId() {
        return "testClusterId";
      }

      @Override
      public int getCurrentNrHRS() throws IOException {
        return 1;
      }
    }

    final AtomicInteger nbThreads = new AtomicInteger(0);

    protected MyConnectionImpl(Configuration conf) throws IOException {
      super(setupConf(conf), null, null);
    }

    private static Configuration setupConf(Configuration conf) {
      conf.setClass(RegistryFactory.REGISTRY_IMPL_CONF_KEY, TestRegistry.class, Registry.class);
      return conf;
    }

    @Override
    public RegionLocations locateRegion(TableName tableName,
        byte[] row, boolean useCache, boolean retry, int replicaId) throws IOException {
      return new RegionLocations(loc1);
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

    protected MyConnectionImpl2(List<HRegionLocation> hrl) throws IOException {
      super(conf);
      this.hrl = hrl;
      this.usedRegions = new boolean[hrl.size()];
    }

    @Override
    public RegionLocations locateRegion(TableName tableName,
        byte[] row, boolean useCache, boolean retry, int replicaId) throws IOException {
      int i = 0;
      for (HRegionLocation hr : hrl){
        if (Arrays.equals(row, hr.getRegionInfo().getStartKey())) {
          usedRegions[i] = true;
          return new RegionLocations(hr);
        }
        i++;
      }
      return null;
    }
  }

  @Test
  public void testSubmit() throws Exception {
    ClusterConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess(hc, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testSubmitWithCB() throws Exception {
    ClusterConnection hc = createHConnection();
    final AtomicInteger updateCalled = new AtomicInteger(0);
    Batch.Callback<Object> cb = new Batch.Callback<Object>() {
      @Override
      public void update(byte[] region, byte[] row, Object result) {
        updateCalled.incrementAndGet();
      }
    };
    AsyncProcess ap = new MyAsyncProcess(hc, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    final AsyncRequestFuture ars = ap.submit(DUMMY_TABLE, puts, false, cb, false);
    Assert.assertTrue(puts.isEmpty());
    ars.waitUntilDone();
    Assert.assertEquals(updateCalled.get(), 1);
  }

  @Test
  public void testSubmitBusyRegion() throws Exception {
    ClusterConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess(hc, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.incTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(puts.size(), 1);

    ap.decTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(0, puts.size());
  }


  @Test
  public void testSubmitBusyRegionServer() throws Exception {
    ClusterConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess(hc, conf);

    ap.taskCounterPerServer.put(sn2, new AtomicInteger(ap.maxConcurrentTasksPerServer));

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));
    puts.add(createPut(3, true)); // <== this one won't be taken, the rs is busy
    puts.add(createPut(1, true)); // <== this one will make it, the region is already in
    puts.add(createPut(2, true)); // <== new region, but the rs is ok

    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(" puts=" + puts, 1, puts.size());

    ap.taskCounterPerServer.put(sn2, new AtomicInteger(ap.maxConcurrentTasksPerServer - 1));
    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testFail() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), conf, false);

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, false);
    puts.add(p);

    AsyncRequestFuture ars = ap.submit(DUMMY_TABLE, puts, false, null, true);
    Assert.assertEquals(0, puts.size());
    ars.waitUntilDone();
    verifyResult(ars, false);
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());

    Assert.assertEquals(1, ars.getErrors().exceptions.size());
    Assert.assertTrue("was: " + ars.getErrors().exceptions.get(0),
        failure.equals(ars.getErrors().exceptions.get(0)));
    Assert.assertTrue("was: " + ars.getErrors().exceptions.get(0),
        failure.equals(ars.getErrors().exceptions.get(0)));

    Assert.assertEquals(1, ars.getFailedOperations().size());
    Assert.assertTrue("was: " + ars.getFailedOperations().get(0),
        p.equals(ars.getFailedOperations().get(0)));
  }


  @Test
  public void testSubmitTrue() throws IOException {
    final AsyncProcess ap = new MyAsyncProcess(createHConnection(), conf, false);
    ap.tasksInProgress.incrementAndGet();
    final AtomicInteger ai = new AtomicInteger(1);
    ap.taskCounterPerRegion.put(hri1.getRegionName(), ai);

    final AtomicBoolean checkPoint = new AtomicBoolean(false);
    final AtomicBoolean checkPoint2 = new AtomicBoolean(false);

    Thread t = new Thread(){
      @Override
      public void run(){
        Threads.sleep(1000);
        Assert.assertFalse(checkPoint.get()); // TODO: this is timing-dependent
        ai.decrementAndGet();
        ap.tasksInProgress.decrementAndGet();
        checkPoint2.set(true);
      }
    };

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, true);
    puts.add(p);

    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertFalse(puts.isEmpty());

    t.start();

    ap.submit(DUMMY_TABLE, puts, true, null, false);
    Assert.assertTrue(puts.isEmpty());

    checkPoint.set(true);
    while (!checkPoint2.get()){
      Threads.sleep(1);
    }
  }

  @Test
  public void testFailAndSuccess() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), conf, false);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    AsyncRequestFuture ars = ap.submit(DUMMY_TABLE, puts, false, null, true);
    Assert.assertTrue(puts.isEmpty());
    ars.waitUntilDone();
    verifyResult(ars, false, true, true);
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());
    ap.callsCt.set(0);
    Assert.assertEquals(1, ars.getErrors().actions.size());

    puts.add(createPut(1, true));
    // Wait for AP to be free. While ars might have the result, ap counters are decreased later.
    ap.waitUntilDone();
    ars = ap.submit(DUMMY_TABLE, puts, false, null, true);
    Assert.assertEquals(0, puts.size());
    ars.waitUntilDone();
    Assert.assertEquals(1, ap.callsCt.get());
    verifyResult(ars, true);
  }

  @Test
  public void testFlush() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), conf, false);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    AsyncRequestFuture ars = ap.submit(DUMMY_TABLE, puts, false, null, true);
    ars.waitUntilDone();
    verifyResult(ars, false, true, true);
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());

    Assert.assertEquals(1, ars.getFailedOperations().size());
  }

  @Test
  public void testMaxTask() throws Exception {
    final AsyncProcess ap = new MyAsyncProcess(createHConnection(), conf, false);

    for (int i = 0; i < 1000; i++) {
      ap.incTaskCounters(Arrays.asList("dummy".getBytes()), sn);
    }

    final Thread myThread = Thread.currentThread();

    Thread t = new Thread() {
      @Override
      public void run() {
        Threads.sleep(2000);
        myThread.interrupt();
      }
    };

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    t.start();

    try {
      ap.submit(DUMMY_TABLE, puts, false, null, false);
      Assert.fail("We should have been interrupted.");
    } catch (InterruptedIOException expected) {
    }

    final long sleepTime = 2000;

    Thread t2 = new Thread() {
      @Override
      public void run() {
        Threads.sleep(sleepTime);
        while (ap.tasksInProgress.get() > 0) {
          ap.decTaskCounters(Arrays.asList("dummy".getBytes()), sn);
        }
      }
    };
    t2.start();

    long start = System.currentTimeMillis();
    ap.submit(DUMMY_TABLE, new ArrayList<Row>(), false, null, false);
    long end = System.currentTimeMillis();

    //Adds 100 to secure us against approximate timing.
    Assert.assertTrue(start + 100L + sleepTime > end);
  }

  private static ClusterConnection createHConnection() throws IOException {
    ClusterConnection hc = createHConnectionCommon();
    setMockLocation(hc, DUMMY_BYTES_1, new RegionLocations(loc1));
    setMockLocation(hc, DUMMY_BYTES_2, new RegionLocations(loc2));
    setMockLocation(hc, DUMMY_BYTES_3, new RegionLocations(loc3));
    setMockLocation(hc, FAILS, new RegionLocations(loc2));
    return hc;
  }

  private static ClusterConnection createHConnectionWithReplicas() throws IOException {
    ClusterConnection hc = createHConnectionCommon();
    setMockLocation(hc, DUMMY_BYTES_1, hrls1);
    setMockLocation(hc, DUMMY_BYTES_2, hrls2);
    setMockLocation(hc, DUMMY_BYTES_3, hrls3);
    return hc;
  }

  private static void setMockLocation(ClusterConnection hc, byte[] row,
      RegionLocations result) throws IOException {
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE), Mockito.eq(row),
        Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyInt())).thenReturn(result);
  }

  private static ClusterConnection createHConnectionCommon() {
    ClusterConnection hc = Mockito.mock(ClusterConnection.class);
    NonceGenerator ng = Mockito.mock(NonceGenerator.class);
    Mockito.when(ng.getNonceGroup()).thenReturn(HConstants.NO_NONCE);
    Mockito.when(hc.getNonceGenerator()).thenReturn(ng);
    Mockito.when(hc.getConfiguration()).thenReturn(conf);
    return hc;
  }

  @Test
  public void testHTablePutSuccess() throws Exception {
    BufferedMutatorImpl ht = Mockito.mock(BufferedMutatorImpl.class);
    ht.ap = new MyAsyncProcess(createHConnection(), conf, true);

    Put put = createPut(1, true);

    Assert.assertEquals(0, ht.getWriteBufferSize());
    ht.mutate(put);
    Assert.assertEquals(0, ht.getWriteBufferSize());
  }

  private void doHTableFailedPut(boolean bufferOn) throws Exception {
    ClusterConnection conn = createHConnection();
    HTable ht = new HTable(conn, new BufferedMutatorParams(DUMMY_TABLE));
    MyAsyncProcess ap = new MyAsyncProcess(conn, conf, true);
    ht.mutator.ap = ap;
    if (bufferOn) {
      ht.setWriteBufferSize(1024L * 1024L);
    } else {
      ht.setWriteBufferSize(0L);
    }

    Put put = createPut(1, false);

    Assert.assertEquals(0L, ht.mutator.currentWriteBufferSize.get());
    try {
      ht.put(put);
      if (bufferOn) {
        ht.flushCommits();
      }
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals(0L, ht.mutator.currentWriteBufferSize.get());
    // The table should have sent one request, maybe after multiple attempts
    AsyncRequestFuture ars = null;
    for (AsyncRequestFuture someReqs : ap.allReqs) {
      if (someReqs.getResults().length == 0) continue;
      Assert.assertTrue(ars == null);
      ars = someReqs;
    }
    Assert.assertTrue(ars != null);
    verifyResult(ars, false);

    // This should not raise any exception, puts have been 'received' before by the catch.
    ht.close();
  }

  @Test
  public void testHTableFailedPutWithBuffer() throws Exception {
    doHTableFailedPut(true);
  }

  @Test
  public void testHTableFailedPutWithoutBuffer() throws Exception {
    doHTableFailedPut(false);
  }

  @Test
  public void testHTableFailedPutAndNewPut() throws Exception {
    ClusterConnection conn = createHConnection();
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, null, null,
        new BufferedMutatorParams(DUMMY_TABLE).writeBufferSize(0));
    MyAsyncProcess ap = new MyAsyncProcess(conn, conf, true);
    mutator.ap = ap;

    Put p = createPut(1, false);
    mutator.mutate(p);

    ap.waitUntilDone(); // Let's do all the retries.

    // We're testing that we're behaving as we were behaving in 0.94: sending exceptions in the
    //  doPut if it fails.
    // This said, it's not a very easy going behavior. For example, when we insert a list of
    //  puts, we may raise an exception in the middle of the list. It's then up to the caller to
    //  manage what was inserted, what was tried but failed, and what was not even tried.
    p = createPut(1, true);
    Assert.assertEquals(0, mutator.writeAsyncBuffer.size());
    try {
      mutator.mutate(p);
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals("the put should not been inserted.", 0, mutator.writeAsyncBuffer.size());
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    ClusterConnection conn = new MyConnectionImpl(conf);
    HTable ht = new HTable(conn, new BufferedMutatorParams(DUMMY_TABLE));
    ht.multiAp = new MyAsyncProcess(conn, conf, false);

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
    Configuration configuration = new Configuration(conf);
    ClusterConnection conn = new MyConnectionImpl(configuration);
    BufferedMutatorImpl mutator =
        new BufferedMutatorImpl(conn, null, null, new BufferedMutatorParams(DUMMY_TABLE));
    configuration.setBoolean(ConnectionImplementation.RETRIES_BY_SERVER_KEY, true);

    MyAsyncProcess ap = new MyAsyncProcess(conn, configuration, true);
    mutator.ap = ap;

    Assert.assertNotNull(mutator.ap.createServerErrorTracker());
    Assert.assertTrue(mutator.ap.serverTrackerTimeout > 200);
    mutator.ap.serverTrackerTimeout = 1;

    Put p = createPut(1, false);
    mutator.mutate(p);

    try {
      mutator.flush();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    // Checking that the ErrorsServers came into play and didn't make us stop immediately
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());
  }

  @Test
  public void testGlobalErrors() throws IOException {
    ClusterConnection conn = new MyConnectionImpl(conf);
    BufferedMutatorImpl mutator = (BufferedMutatorImpl) conn.getBufferedMutator(DUMMY_TABLE);
    AsyncProcessWithFailure ap = new AsyncProcessWithFailure(conn, conf, new IOException("test"));
    mutator.ap = ap;

    Assert.assertNotNull(mutator.ap.createServerErrorTracker());

    Put p = createPut(1, true);
    mutator.mutate(p);

    try {
      mutator.flush();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    // Checking that the ErrorsServers came into play and didn't make us stop immediately
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());
  }


  @Test
  public void testCallQueueTooLarge() throws IOException {
    ClusterConnection conn = new MyConnectionImpl(conf);
    BufferedMutatorImpl mutator = (BufferedMutatorImpl) conn.getBufferedMutator(DUMMY_TABLE);
    AsyncProcessWithFailure ap = new AsyncProcessWithFailure(conn, conf, new CallQueueTooBigException());
    mutator.ap = ap;

    Assert.assertNotNull(mutator.ap.createServerErrorTracker());

    Put p = createPut(1, true);
    mutator.mutate(p);

    try {
      mutator.flush();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    // Checking that the ErrorsServers came into play and didn't make us stop immediately
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());
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

    MyConnectionImpl2 con = new MyConnectionImpl2(hrls);
    HTable ht = new HTable(con, new BufferedMutatorParams(DUMMY_TABLE));
    MyAsyncProcess ap = new MyAsyncProcess(con, conf, con.nbThreads);
    ht.multiAp = ap;

    ht.batch(gets, null);

    Assert.assertEquals(ap.nbActions.get(), NB_REGS);
    Assert.assertEquals("1 multi response per server", 2, ap.nbMultiResponse.get());
    Assert.assertEquals("1 thread per server", 2, con.nbThreads.get());

    int nbReg = 0;
    for (int i =0; i<NB_REGS; i++){
      if (con.usedRegions[i]) nbReg++;
    }
    Assert.assertEquals("nbReg=" + nbReg, nbReg, NB_REGS);
  }

  @Test
  public void testReplicaReplicaSuccess() throws Exception {
    // Main call takes too long so replicas succeed, except for one region w/o replicas.
    // One region has no replica, so the main call succeeds for it.
    MyAsyncProcessWithReplicas ap = createReplicaAp(10, 1000, 0);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2, DUMMY_BYTES_3);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[3]);
    verifyReplicaResult(ars, RR.TRUE, RR.TRUE, RR.FALSE);
    Assert.assertEquals(2, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaPrimarySuccessWoReplicaCalls() throws Exception {
    // Main call succeeds before replica calls are kicked off.
    MyAsyncProcessWithReplicas ap = createReplicaAp(1000, 10, 0);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2, DUMMY_BYTES_3);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[3]);
    verifyReplicaResult(ars, RR.FALSE, RR.FALSE, RR.FALSE);
    Assert.assertEquals(0, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaParallelCallsSucceed() throws Exception {
    // Either main or replica can succeed.
    MyAsyncProcessWithReplicas ap = createReplicaAp(0, 0, 0);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[2]);
    verifyReplicaResult(ars, RR.DONT_CARE, RR.DONT_CARE);
    long replicaCalls = ap.getReplicaCallCount();
    Assert.assertTrue(replicaCalls >= 0);
    Assert.assertTrue(replicaCalls <= 2);
  }

  @Test
  public void testReplicaPartialReplicaCall() throws Exception {
    // One server is slow, so the result for its region comes from replica, whereas
    // the result for other region comes from primary before replica calls happen.
    // There should be no replica call for that region at all.
    MyAsyncProcessWithReplicas ap = createReplicaAp(1000, 0, 0);
    ap.setPrimaryCallDelay(sn2, 2000);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[2]);
    verifyReplicaResult(ars, RR.FALSE, RR.TRUE);
    Assert.assertEquals(1, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaMainFailsBeforeReplicaCalls() throws Exception {
    // Main calls fail before replica calls can start - this is currently not handled.
    // It would probably never happen if we can get location (due to retries),
    // and it would require additional synchronization.
    MyAsyncProcessWithReplicas ap = createReplicaAp(1000, 0, 0, 0);
    ap.addFailures(hri1, hri2);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[2]);
    verifyReplicaResult(ars, RR.FAILED, RR.FAILED);
    Assert.assertEquals(0, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaReplicaSuccessWithParallelFailures() throws Exception {
    // Main calls fails after replica calls start. For two-replica region, one replica call
    // also fails. Regardless, we get replica results for both regions.
    MyAsyncProcessWithReplicas ap = createReplicaAp(0, 1000, 1000, 0);
    ap.addFailures(hri1, hri1r2, hri2);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[2]);
    verifyReplicaResult(ars, RR.TRUE, RR.TRUE);
    Assert.assertEquals(2, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaAllCallsFailForOneRegion() throws Exception {
    // For one of the region, all 3, main and replica, calls fail. For the other, replica
    // call fails but its exception should not be visible as it did succeed.
    MyAsyncProcessWithReplicas ap = createReplicaAp(500, 1000, 0, 0);
    ap.addFailures(hri1, hri1r1, hri1r2, hri2r1);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncRequestFuture ars = ap.submitAll(DUMMY_TABLE, rows, null, new Object[2]);
    verifyReplicaResult(ars, RR.FAILED, RR.FALSE);
    // We should get 3 exceptions, for main + 2 replicas for DUMMY_BYTES_1
    Assert.assertEquals(3, ars.getErrors().getNumExceptions());
    for (int i = 0; i < ars.getErrors().getNumExceptions(); ++i) {
      Assert.assertArrayEquals(DUMMY_BYTES_1, ars.getErrors().getRow(i).getRow());
    }
  }

  private MyAsyncProcessWithReplicas createReplicaAp(
      int replicaAfterMs, int primaryMs, int replicaMs) throws Exception {
    return createReplicaAp(replicaAfterMs, primaryMs, replicaMs, -1);
  }

  private MyAsyncProcessWithReplicas createReplicaAp(
      int replicaAfterMs, int primaryMs, int replicaMs, int retries) throws Exception {
    // TODO: this is kind of timing dependent... perhaps it should detect from createCaller
    //       that the replica call has happened and that way control the ordering.
    Configuration conf = new Configuration();
    ClusterConnection conn = createHConnectionWithReplicas();
    conf.setInt(AsyncProcess.PRIMARY_CALL_TIMEOUT_KEY, replicaAfterMs * 1000);
    if (retries >= 0) {
      conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    }
    MyAsyncProcessWithReplicas ap = new MyAsyncProcessWithReplicas(conn, conf);
    ap.setCallDelays(primaryMs, replicaMs);
    return ap;
  }

  private static List<Get> makeTimelineGets(byte[]... rows) {
    List<Get> result = new ArrayList<Get>();
    for (byte[] row : rows) {
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      result.add(get);
    }
    return result;
  }

  private void verifyResult(AsyncRequestFuture ars, boolean... expected) throws Exception {
    Object[] actual = ars.getResults();
    Assert.assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; ++i) {
      Assert.assertEquals(expected[i], !(actual[i] instanceof Throwable));
    }
  }

  /** After reading TheDailyWtf, I always wanted to create a MyBoolean enum like this! */
  private enum RR {
    TRUE,
    FALSE,
    DONT_CARE,
    FAILED
  }

  private void verifyReplicaResult(AsyncRequestFuture ars, RR... expecteds) throws Exception {
    Object[] actuals = ars.getResults();
    Assert.assertEquals(expecteds.length, actuals.length);
    for (int i = 0; i < expecteds.length; ++i) {
      Object actual = actuals[i];
      RR expected = expecteds[i];
      Assert.assertEquals(actual.toString(), expected == RR.FAILED, actual instanceof Throwable);
      if (expected != RR.FAILED && expected != RR.DONT_CARE) {
        Assert.assertEquals(expected == RR.TRUE, ((Result)actual).isStale());
      }
    }
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

    p.addColumn(DUMMY_BYTES_1, DUMMY_BYTES_1, DUMMY_BYTES_1);

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
    public AsyncProcessForThrowableCheck(ClusterConnection hc, Configuration conf,
        ExecutorService pool) {
      super(hc, conf, pool, new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(
          conf));
    }
  }

  @Test
  public void testUncheckedException() throws Exception {
    // Test the case pool.submit throws unchecked exception
    ClusterConnection hc = createHConnection();
    MyThreadPoolExecutor myPool =
        new MyThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(200));
    AsyncProcess ap = new AsyncProcessForThrowableCheck(hc, conf, myPool);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testWaitForMaximumCurrentTasks() throws InterruptedException, BrokenBarrierException {
    final AtomicLong tasks = new AtomicLong(0);
    final AtomicInteger max = new AtomicInteger(0);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          barrier.await();
          AsyncProcess.waitForMaximumCurrentTasks(max.get(), tasks, 1);
        } catch (InterruptedIOException e) {
          Assert.fail(e.getMessage());
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (BrokenBarrierException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    // First test that our runnable thread only exits when tasks is zero.
    Thread t = new Thread(runnable);
    t.start();
    barrier.await();
    t.join();
    // Now assert we stay running if max == zero and tasks is > 0.
    barrier.reset();
    tasks.set(1000000);
    t = new Thread(runnable);
    t.start();
    barrier.await();
    while (tasks.get() > 0) {
      assertTrue(t.isAlive());
      tasks.set(tasks.get() - 1);
    }
    t.join();
  }
}
