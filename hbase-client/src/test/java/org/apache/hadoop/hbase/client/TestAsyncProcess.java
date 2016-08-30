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


import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncRequestFuture;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncRequestFutureImpl;
import org.apache.hadoop.hbase.client.AsyncProcess.ListRowAccess;
import org.apache.hadoop.hbase.client.AsyncProcess.TaskCountChecker;
import org.apache.hadoop.hbase.client.AsyncProcess.RowChecker.ReturnCode;
import org.apache.hadoop.hbase.client.AsyncProcess.RowCheckerHost;
import org.apache.hadoop.hbase.client.AsyncProcess.RequestSizeChecker;
import org.apache.hadoop.hbase.client.AsyncProcess.RowChecker;
import org.apache.hadoop.hbase.client.AsyncProcess.SubmittedSizeChecker;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestAsyncProcess {
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
    private static int rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    @Override
    protected <Res> AsyncRequestFutureImpl<Res> createAsyncRequestFuture(TableName tableName,
        List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
        Batch.Callback<Res> callback, Object[] results, boolean needResults) {
      // Test HTable has tableName of null, so pass DUMMY_TABLE
      AsyncRequestFutureImpl<Res> r = super.createAsyncRequestFuture(
          DUMMY_TABLE, actions, nonceGroup, pool, callback, results, needResults);
      allReqs.add(r);
      callsCt.incrementAndGet();
      return r;
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf) {
      this(hc, conf, new AtomicInteger());
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf, AtomicInteger nbThreads) {
      super(hc, conf, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new CountingThreadFactory(nbThreads)),
            new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(conf), rpcTimeout);
    }

    public MyAsyncProcess(
        ClusterConnection hc, Configuration conf, boolean useGlobalErrors) {
      super(hc, conf, new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new CountingThreadFactory(new AtomicInteger())),
          new RpcRetryingCallerFactory(conf), useGlobalErrors, new RpcControllerFactory(conf), rpcTimeout);
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
          new RpcRetryingCallerFactory(conf), useGlobalErrors, new RpcControllerFactory(conf), rpcTimeout);
    }

    @Override
    public <Res> AsyncRequestFuture submit(TableName tableName, RowAccess<? extends Row> rows,
        boolean atLeastOne, Callback<Res> callback, boolean needResults)
            throws InterruptedIOException {
      // We use results in tests to check things, so override to always save them.
      return super.submit(DUMMY_TABLE, rows, atLeastOne, callback, true);
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

      return new RpcRetryingCaller<MultiResponse>(100, 10, 9) {
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

  static class CallerWithFailure extends RpcRetryingCaller<MultiResponse>{

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

      return new RpcRetryingCaller<MultiResponse>(100, 10, 9) {
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
  static class MyConnectionImpl extends ConnectionManager.HConnectionImplementation {
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
      public boolean isTableOnlineState(TableName tableName, boolean enabled) throws IOException {
        return false;
      }

      @Override
      public int getCurrentNrHRS() throws IOException {
        return 1;
      }
    }

    final AtomicInteger nbThreads = new AtomicInteger(0);

    protected MyConnectionImpl(Configuration conf) throws IOException {
      super(setupConf(conf), false);
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

  @Rule
  public Timeout timeout = Timeout.millis(10000); // 10 seconds max per method tested

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
  public void testListRowAccess() {
    int count = 10;
    List<String> values = new LinkedList<>();
    for (int i = 0; i != count; ++i) {
      values.add(String.valueOf(i));
    }

    ListRowAccess<String> taker = new ListRowAccess(values);
    assertEquals(count, taker.size());

    int restoreCount = 0;
    int takeCount = 0;
    Iterator<String> it = taker.iterator();
    while (it.hasNext()) {
      String v = it.next();
      assertEquals(String.valueOf(takeCount), v);
      ++takeCount;
      it.remove();
      if (Math.random() >= 0.5) {
        break;
      }
    }
    assertEquals(count, taker.size() + takeCount);

    it = taker.iterator();
    while (it.hasNext()) {
      String v = it.next();
      assertEquals(String.valueOf(takeCount), v);
      ++takeCount;
      it.remove();
    }
    assertEquals(0, taker.size());
    assertEquals(count, takeCount);
  }
  private static long calculateRequestCount(long putSizePerServer, long maxHeapSizePerRequest) {
    if (putSizePerServer <= maxHeapSizePerRequest) {
      return 1;
    } else if (putSizePerServer % maxHeapSizePerRequest == 0) {
      return putSizePerServer / maxHeapSizePerRequest;
    } else {
      return putSizePerServer / maxHeapSizePerRequest + 1;
    }
  }

  @Test
  public void testSubmitSameSizeOfRequest() throws Exception {
    long writeBuffer = 2 * 1024 * 1024;
    long putsHeapSize = writeBuffer;
    doSubmitRequest(writeBuffer, putsHeapSize);
  }
  @Test
  public void testIllegalArgument() throws IOException {
    ClusterConnection conn = createHConnection();
    final long maxHeapSizePerRequest = conn.getConfiguration().getLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
      AsyncProcess.DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, -1);
    try {
      MyAsyncProcess ap = new MyAsyncProcess(conn, conf, true);
      fail("The maxHeapSizePerRequest must be bigger than zero");
    } catch (IllegalArgumentException e) {
    }
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, maxHeapSizePerRequest);
  }
  @Test
  public void testSubmitLargeRequestWithUnlimitedSize() throws Exception {
    long maxHeapSizePerRequest = Long.MAX_VALUE;
    long putsHeapSize = 2 * 1024 * 1024;
    doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
  }

  @Test(timeout=300000)
  public void testSubmitRandomSizeRequest() throws Exception {
    Random rn = new Random();
    final long limit = 10 * 1024 * 1024;
    for (int count = 0; count != 2; ++count) {
      long maxHeapSizePerRequest = Math.max(1, (Math.abs(rn.nextLong()) % limit));
      long putsHeapSize = Math.max(1, (Math.abs(rn.nextLong()) % limit));
      LOG.info("[testSubmitRandomSizeRequest] maxHeapSizePerRequest=" + maxHeapSizePerRequest + ", putsHeapSize=" + putsHeapSize);
      doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
    }
  }

  @Test
  public void testSubmitSmallRequest() throws Exception {
    long maxHeapSizePerRequest = 2 * 1024 * 1024;
    long putsHeapSize = 100;
    doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
  }

  @Test(timeout=120000)
  public void testSubmitLargeRequest() throws Exception {
    long maxHeapSizePerRequest = 2 * 1024 * 1024;
    long putsHeapSize = maxHeapSizePerRequest * 2;
    doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
  }

  private void doSubmitRequest(long maxHeapSizePerRequest, long putsHeapSize) throws Exception {
    ClusterConnection conn = createHConnection();
    final long defaultHeapSizePerRequest = conn.getConfiguration().getLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
      AsyncProcess.DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, maxHeapSizePerRequest);
    BufferedMutatorParams bufferParam = new BufferedMutatorParams(DUMMY_TABLE);

    // sn has two regions
    long putSizeSN = 0;
    long putSizeSN2 = 0;
    List<Put> puts = new ArrayList<>();
    while ((putSizeSN + putSizeSN2) <= putsHeapSize) {
      Put put1 = new Put(DUMMY_BYTES_1);
      put1.addColumn(DUMMY_BYTES_1, DUMMY_BYTES_1, DUMMY_BYTES_1);
      Put put2 = new Put(DUMMY_BYTES_2);
      put2.addColumn(DUMMY_BYTES_2, DUMMY_BYTES_2, DUMMY_BYTES_2);
      Put put3 = new Put(DUMMY_BYTES_3);
      put3.addColumn(DUMMY_BYTES_3, DUMMY_BYTES_3, DUMMY_BYTES_3);
      putSizeSN += (put1.heapSize() + put2.heapSize());
      putSizeSN2 += put3.heapSize();
      puts.add(put1);
      puts.add(put2);
      puts.add(put3);
    }

    int minCountSnRequest = (int) calculateRequestCount(putSizeSN, maxHeapSizePerRequest);
    int minCountSn2Request = (int) calculateRequestCount(putSizeSN2, maxHeapSizePerRequest);
    LOG.info("Total put count:" + puts.size() + ", putSizeSN:"+ putSizeSN + ", putSizeSN2:" + putSizeSN2
      + ", maxHeapSizePerRequest:" + maxHeapSizePerRequest
      + ", minCountSnRequest:" + minCountSnRequest
      + ", minCountSn2Request:" + minCountSn2Request);
    try (HTable ht = new HTable(conn, bufferParam)) {
      MyAsyncProcess ap = new MyAsyncProcess(conn, conf, true);
      ht.mutator.ap = ap;

      Assert.assertEquals(0L, ht.mutator.currentWriteBufferSize.get());
      ht.put(puts);
      List<AsyncRequestFuture> reqs = ap.allReqs;

      int actualSnReqCount = 0;
      int actualSn2ReqCount = 0;
      for (AsyncRequestFuture req : reqs) {
        if (!(req instanceof AsyncRequestFutureImpl)) {
          continue;
        }
        AsyncRequestFutureImpl ars = (AsyncRequestFutureImpl) req;
        if (ars.getRequestHeapSize().containsKey(sn)) {
          ++actualSnReqCount;
        }
        if (ars.getRequestHeapSize().containsKey(sn2)) {
          ++actualSn2ReqCount;
        }
      }
      // If the server is busy, the actual count may be incremented.
      assertEquals(true, minCountSnRequest <= actualSnReqCount);
      assertEquals(true, minCountSn2Request <= actualSn2ReqCount);
      Map<ServerName, Long> sizePerServers = new HashMap<>();
      for (AsyncRequestFuture req : reqs) {
        if (!(req instanceof AsyncRequestFutureImpl)) {
          continue;
        }
        AsyncRequestFutureImpl ars = (AsyncRequestFutureImpl) req;
        Map<ServerName, List<Long>> requestHeapSize = ars.getRequestHeapSize();
        for (Map.Entry<ServerName, List<Long>> entry : requestHeapSize.entrySet()) {
          long sum = 0;
          for (long size : entry.getValue()) {
            assertEquals(true, size <= maxHeapSizePerRequest);
            sum += size;
          }
          assertEquals(true, sum <= maxHeapSizePerRequest);
          long value = sizePerServers.containsKey(entry.getKey()) ? sizePerServers.get(entry.getKey()) : 0L;
          sizePerServers.put(entry.getKey(), value + sum);
        }
      }
      assertEquals(true, sizePerServers.containsKey(sn));
      assertEquals(true, sizePerServers.containsKey(sn2));
      assertEquals(false, sizePerServers.containsKey(sn3));
      assertEquals(putSizeSN, (long) sizePerServers.get(sn));
      assertEquals(putSizeSN2, (long) sizePerServers.get(sn2));
    }
    // restore config.
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, defaultHeapSizePerRequest);
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

    for (int i = 0; i != ap.maxConcurrentTasksPerRegion; ++i) {
      ap.incTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    }
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
    final AtomicInteger ai = new AtomicInteger(ap.maxConcurrentTasksPerRegion);
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
    Assert.assertEquals(2, ap.callsCt.get());
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
    Mockito.when(hc.locateRegions(Mockito.eq(DUMMY_TABLE), Mockito.anyBoolean(), Mockito.anyBoolean()))
           .thenReturn(Arrays.asList(loc1, loc2, loc3));
    setMockLocation(hc, FAILS, new RegionLocations(loc2));
    return hc;
  }

  private static ClusterConnection createHConnectionWithReplicas() throws IOException {
    ClusterConnection hc = createHConnectionCommon();
    setMockLocation(hc, DUMMY_BYTES_1, hrls1);
    setMockLocation(hc, DUMMY_BYTES_2, hrls2);
    setMockLocation(hc, DUMMY_BYTES_3, hrls3);
    List<HRegionLocation> locations = new ArrayList<HRegionLocation>();
    for (HRegionLocation loc : hrls1.getRegionLocations()) {
      locations.add(loc);
    }
    for (HRegionLocation loc : hrls2.getRegionLocations()) {
      locations.add(loc);
    }
    for (HRegionLocation loc : hrls3.getRegionLocations()) {
      locations.add(loc);
    }
    Mockito.when(hc.locateRegions(Mockito.eq(DUMMY_TABLE), Mockito.anyBoolean(), Mockito.anyBoolean()))
           .thenReturn(locations);
    return hc;
  }

  private static void setMockLocation(ClusterConnection hc, byte[] row,
      RegionLocations result) throws IOException {
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE), Mockito.eq(row),
        Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyInt())).thenReturn(result);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE), Mockito.eq(row),
        Mockito.anyBoolean(), Mockito.anyBoolean())).thenReturn(result);
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
    Assert.assertEquals(0, mutator.getWriteBuffer().size());
    try {
      mutator.mutate(p);
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals("the put should not been inserted.", 0, mutator.getWriteBuffer().size());
  }

  @Test
  public void testTaskCheckerHost() throws IOException {
    final int maxTotalConcurrentTasks = 100;
    final int maxConcurrentTasksPerServer = 2;
    final int maxConcurrentTasksPerRegion = 1;
    final AtomicLong tasksInProgress = new AtomicLong(0);
    final Map<ServerName, AtomicInteger> taskCounterPerServer = new HashMap<>();
    final Map<byte[], AtomicInteger> taskCounterPerRegion = new HashMap<>();
    TaskCountChecker countChecker = new TaskCountChecker(
      maxTotalConcurrentTasks,
      maxConcurrentTasksPerServer,
      maxConcurrentTasksPerRegion,
      tasksInProgress, taskCounterPerServer, taskCounterPerRegion);
    final long maxHeapSizePerRequest = 2 * 1024 * 1024;
    // unlimiited
    RequestSizeChecker sizeChecker = new RequestSizeChecker(maxHeapSizePerRequest);
    RowCheckerHost checkerHost = new RowCheckerHost(Arrays.asList(countChecker, sizeChecker));

    ReturnCode loc1Code = checkerHost.canTakeOperation(loc1, maxHeapSizePerRequest);
    assertEquals(RowChecker.ReturnCode.INCLUDE, loc1Code);

    ReturnCode loc1Code_2 = checkerHost.canTakeOperation(loc1, maxHeapSizePerRequest);
    // rejected for size
    assertNotEquals(RowChecker.ReturnCode.INCLUDE, loc1Code_2);

    ReturnCode loc2Code = checkerHost.canTakeOperation(loc2, maxHeapSizePerRequest);
    // rejected for size
    assertNotEquals(RowChecker.ReturnCode.INCLUDE, loc2Code);

    // fill the task slots for loc3.
    taskCounterPerRegion.put(loc3.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(loc3.getServerName(), new AtomicInteger(100));

    ReturnCode loc3Code = checkerHost.canTakeOperation(loc3, 1L);
    // rejected for count
    assertNotEquals(RowChecker.ReturnCode.INCLUDE, loc3Code);

    // release the task slots for loc3.
    taskCounterPerRegion.put(loc3.getRegionInfo().getRegionName(), new AtomicInteger(0));
    taskCounterPerServer.put(loc3.getServerName(), new AtomicInteger(0));

    ReturnCode loc3Code_2 = checkerHost.canTakeOperation(loc3, 1L);
    assertEquals(RowChecker.ReturnCode.INCLUDE, loc3Code_2);
  }

  @Test
  public void testRequestSizeCheckerr() throws IOException {
    final long maxHeapSizePerRequest = 2 * 1024 * 1024;
    final ClusterConnection conn = createHConnection();
    RequestSizeChecker checker = new RequestSizeChecker(maxHeapSizePerRequest);

    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    // accept the data located on loc1 region.
    ReturnCode acceptCode = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
    assertEquals(RowChecker.ReturnCode.INCLUDE, acceptCode);
    checker.notifyFinal(acceptCode, loc1, maxHeapSizePerRequest);

    // the sn server reachs the limit.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    // the request to sn2 server should be accepted.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc3, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }
  }

  @Test
  public void testSubmittedSizeChecker() {
    final long maxHeapSizeSubmit = 2 * 1024 * 1024;
    SubmittedSizeChecker checker = new SubmittedSizeChecker(maxHeapSizeSubmit);

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }

    for (int i = 0; i != 10; ++i) {
      checker.notifyFinal(ReturnCode.INCLUDE, loc1, maxHeapSizeSubmit);
    }

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.END, include);
    }
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc2, 100000);
      assertEquals(ReturnCode.END, include);
    }
    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }
  }
  @Test
  public void testTaskCountChecker() throws InterruptedIOException {
    long rowSize = 12345;
    int maxTotalConcurrentTasks = 100;
    int maxConcurrentTasksPerServer = 2;
    int maxConcurrentTasksPerRegion = 1;
    AtomicLong tasksInProgress = new AtomicLong(0);
    Map<ServerName, AtomicInteger> taskCounterPerServer = new HashMap<>();
    Map<byte[], AtomicInteger> taskCounterPerRegion = new HashMap<>();
    TaskCountChecker checker = new TaskCountChecker(
      maxTotalConcurrentTasks,
      maxConcurrentTasksPerServer,
      maxConcurrentTasksPerRegion,
      tasksInProgress, taskCounterPerServer, taskCounterPerRegion);

    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, rowSize);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }
    // add loc1 region.
    ReturnCode code = checker.canTakeOperation(loc1, rowSize);
    assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    checker.notifyFinal(code, loc1, rowSize);

    // fill the task slots for loc1.
    taskCounterPerRegion.put(loc1.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(loc1.getServerName(), new AtomicInteger(100));

    // the region was previously accepted, so it must be accpted now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(loc1, rowSize);
      assertEquals(RowChecker.ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, loc1, rowSize);
    }

    // fill the task slots for loc3.
    taskCounterPerRegion.put(loc3.getRegionInfo().getRegionName(), new AtomicInteger(100));
    taskCounterPerServer.put(loc3.getServerName(), new AtomicInteger(100));

    // no task slots.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode excludeCode = checker.canTakeOperation(loc3, rowSize);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, excludeCode);
      checker.notifyFinal(excludeCode, loc3, rowSize);
    }

    // release the tasks for loc3.
    taskCounterPerRegion.put(loc3.getRegionInfo().getRegionName(), new AtomicInteger(0));
    taskCounterPerServer.put(loc3.getServerName(), new AtomicInteger(0));

    // add loc3 region.
    ReturnCode code3 = checker.canTakeOperation(loc3, rowSize);
    assertEquals(RowChecker.ReturnCode.INCLUDE, code3);
    checker.notifyFinal(code3, loc3, rowSize);

    // the region was previously accepted, so it must be accpted now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(loc3, rowSize);
      assertEquals(RowChecker.ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, loc3, rowSize);
    }

    checker.reset();
    // the region was previously accepted,
    // but checker have reseted and task slots for loc1 is full.
    // So it must be rejected now.
    for (int i = 0; i != maxConcurrentTasksPerRegion * 5; ++i) {
      ReturnCode includeCode = checker.canTakeOperation(loc1, rowSize);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, includeCode);
      checker.notifyFinal(includeCode, loc1, rowSize);
    }
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
    configuration.setBoolean(ConnectionManager.RETRIES_BY_SERVER_KEY, true);

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

    ht.batch(gets, new Object[gets.size()]);

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
    MyAsyncProcessWithReplicas ap = createReplicaAp(1000, 0, 0, 1);
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
    MyAsyncProcessWithReplicas ap = createReplicaAp(0, 1000, 1000, 1);
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
    MyAsyncProcessWithReplicas ap = createReplicaAp(500, 1000, 0, 1);
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
    if (retries > 0) {
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
    private static int rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    public AsyncProcessForThrowableCheck(ClusterConnection hc, Configuration conf,
        ExecutorService pool) {
      super(hc, conf, pool, new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(
          conf), rpcTimeout);
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
  public void testWaitForMaximumCurrentTasks() throws Exception {
    final AtomicLong tasks = new AtomicLong(0);
    final AtomicInteger max = new AtomicInteger(0);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AsyncProcess ap = new MyAsyncProcess(createHConnection(), conf);
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          barrier.await();
          ap.waitForMaximumCurrentTasks(max.get(), tasks, 1, null);
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