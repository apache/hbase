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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.hadoop.hbase.client.AsyncProcessTask.ListRowAccess;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.apache.hadoop.hbase.client.AsyncProcessTask.SubmittedRows;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@Category({ClientTests.class, MediumTests.class})
public class TestAsyncProcess {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
  private static final Log LOG = LogFactory.getLog(TestAsyncProcess.class);
  private static final TableName DUMMY_TABLE =
      TableName.valueOf("DUMMY_TABLE");
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static final byte[] DUMMY_BYTES_3 = "DUMMY_BYTES_3".getBytes();
  private static final byte[] FAILS = "FAILS".getBytes();
  private static final Configuration CONF = new Configuration();
  private static final ConnectionConfiguration CONNECTION_CONFIG = new ConnectionConfiguration(CONF);
  private static final ServerName sn = ServerName.valueOf("s1:1,1");
  private static final ServerName sn2 = ServerName.valueOf("s2:2,2");
  private static final ServerName sn3 = ServerName.valueOf("s3:3,3");
  private static final HRegionInfo hri1 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_1, DUMMY_BYTES_2, false, 1);
  private static final HRegionInfo hri2 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_2, HConstants.EMPTY_END_ROW, false, 2);
  private static final HRegionInfo hri3 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_3, HConstants.EMPTY_END_ROW, false, 3);
  private static final HRegionLocation loc1 = new HRegionLocation(hri1, sn);
  private static final HRegionLocation loc2 = new HRegionLocation(hri2, sn);
  private static final HRegionLocation loc3 = new HRegionLocation(hri3, sn2);

  // Replica stuff
  private static final HRegionInfo hri1r1 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 1),
      hri1r2 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 2);
  private static final HRegionInfo hri2r1 = RegionReplicaUtil.getRegionInfoForReplica(hri2, 1);
  private static final RegionLocations hrls1 = new RegionLocations(new HRegionLocation(hri1, sn),
      new HRegionLocation(hri1r1, sn2), new HRegionLocation(hri1r2, sn3));
  private static final RegionLocations hrls2 = new RegionLocations(new HRegionLocation(hri2, sn2),
      new HRegionLocation(hri2r1, sn3));
  private static final RegionLocations hrls3 = new RegionLocations(new HRegionLocation(hri3, sn3), null);

  private static final String success = "success";
  private static Exception failure = new Exception("failure");

  private static final int NB_RETRIES = 3;

  private static final int RPC_TIMEOUT = CONF.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
  private static final int OPERATION_TIMEOUT = CONF.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  @BeforeClass
  public static void beforeClass(){
    CONF.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, NB_RETRIES);
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

    private long previousTimeout = -1;
    final ExecutorService service;
    @Override
    protected <Res> AsyncRequestFutureImpl<Res> createAsyncRequestFuture(
      AsyncProcessTask task, List<Action> actions, long nonceGroup) {
      // Test HTable has tableName of null, so pass DUMMY_TABLE
      AsyncProcessTask wrap = new AsyncProcessTask(task){
        @Override
        public TableName getTableName() {
          return DUMMY_TABLE;
        }
      };
      AsyncRequestFutureImpl<Res> r = new MyAsyncRequestFutureImpl<Res>(
          wrap, actions, nonceGroup, this);
      allReqs.add(r);
      return r;
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf) {
      this(hc, conf, new AtomicInteger());
    }

    public MyAsyncProcess(ClusterConnection hc, Configuration conf, AtomicInteger nbThreads) {
      super(hc, conf, new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(conf));
      service = new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
          new SynchronousQueue<>(), new CountingThreadFactory(nbThreads));
    }

    public MyAsyncProcess(
        ClusterConnection hc, Configuration conf, boolean useGlobalErrors) {
      super(hc, conf,
          new RpcRetryingCallerFactory(conf), useGlobalErrors, new RpcControllerFactory(conf));
      service = Executors.newFixedThreadPool(5);
    }

    public <CResult> AsyncRequestFuture submit(ExecutorService pool, TableName tableName,
        List<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
        boolean needResults) throws InterruptedIOException {
      AsyncProcessTask task = AsyncProcessTask.newBuilder(callback)
              .setPool(pool == null ? service : pool)
              .setTableName(tableName)
              .setRowAccess(rows)
              .setSubmittedRows(atLeastOne ? SubmittedRows.AT_LEAST_ONE : SubmittedRows.NORMAL)
              .setNeedResults(needResults)
              .setRpcTimeout(RPC_TIMEOUT)
              .setOperationTimeout(OPERATION_TIMEOUT)
              .build();
      return submit(task);
    }

    public <CResult> AsyncRequestFuture submit(TableName tableName,
        final List<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
        boolean needResults) throws InterruptedIOException {
      return submit(null, tableName, rows, atLeastOne, callback, needResults);
    }

    @Override
    public <Res> AsyncRequestFuture submit(AsyncProcessTask<Res> task)
            throws InterruptedIOException {
      previousTimeout = task.getRpcTimeout();
      // We use results in tests to check things, so override to always save them.
      AsyncProcessTask<Res> wrap = new AsyncProcessTask<Res>(task) {
        @Override
        public boolean getNeedResults() {
          return true;
        }
      };
      return super.submit(wrap);
    }

    @Override
    protected RpcRetryingCaller<AbstractResponse> createCaller(
        CancellableRegionServerCallable callable, int rpcTimeout) {
      callsCt.incrementAndGet();
      MultiServerCallable callable1 = (MultiServerCallable) callable;
      final MultiResponse mr = createMultiResponse(
          callable1.getMulti(), nbMultiResponse, nbActions,
          new ResponseGenerator() {
            @Override
            public void addResponse(MultiResponse mr, byte[] regionName, Action a) {
              if (Arrays.equals(FAILS, a.getAction().getRow())) {
                mr.add(regionName, a.getOriginalIndex(), failure);
              } else {
                mr.add(regionName, a.getOriginalIndex(), success);
              }
            }
          });

      return new RpcRetryingCallerImpl<AbstractResponse>(100, 500, 10, 9) {
        @Override
        public AbstractResponse callWithoutRetries(RetryingCallable<AbstractResponse> callable,
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



  static class MyAsyncRequestFutureImpl<Res> extends AsyncRequestFutureImpl<Res> {

    public MyAsyncRequestFutureImpl(AsyncProcessTask task, List<Action> actions,
      long nonceGroup, AsyncProcess asyncProcess) {
      super(task, actions, nonceGroup, asyncProcess);
    }

    @Override
    protected void updateStats(ServerName server, Map<byte[], MultiResponse.RegionResult> results) {
      // Do nothing for avoiding the NPE if we test the ClientBackofPolicy.
    }

  }

  static class CallerWithFailure extends RpcRetryingCallerImpl<AbstractResponse>{

    private final IOException e;

    public CallerWithFailure(IOException e) {
      super(100, 500, 100, 9);
      this.e = e;
    }

    @Override
    public AbstractResponse callWithoutRetries(RetryingCallable<AbstractResponse> callable,
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
    protected RpcRetryingCaller<AbstractResponse> createCaller(
      CancellableRegionServerCallable callable, int rpcTimeout) {
      callsCt.incrementAndGet();
      return new CallerWithFailure(ioe);
    }
  }

  /**
   * Make the backoff time always different on each call.
   */
  static class MyClientBackoffPolicy implements ClientBackoffPolicy {
    private final Map<ServerName, AtomicInteger> count = new HashMap<>();
    @Override
    public long getBackoffTime(ServerName serverName, byte[] region, ServerStatistics stats) {
      AtomicInteger inc = count.get(serverName);
      if (inc == null) {
        inc = new AtomicInteger(0);
        count.put(serverName, inc);
      }
      return inc.getAndIncrement();
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
    protected RpcRetryingCaller<AbstractResponse> createCaller(
        CancellableRegionServerCallable payloadCallable, int rpcTimeout) {
      MultiServerCallable callable = (MultiServerCallable) payloadCallable;
      final MultiResponse mr = createMultiResponse(
          callable.getMulti(), nbMultiResponse, nbActions, new ResponseGenerator() {
            @Override
            public void addResponse(MultiResponse mr, byte[] regionName, Action a) {
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
      final ServerName server = ((MultiServerCallable)callable).getServerName();
      String debugMsg = "Call to " + server + ", primary=" + isDefault + " with "
          + callable.getMulti().actions.size() + " entries: ";
      for (byte[] region : callable.getMulti().actions.keySet()) {
        debugMsg += "[" + Bytes.toStringBinary(region) + "], ";
      }
      LOG.debug(debugMsg);
      if (!isDefault) {
        replicaCalls.incrementAndGet();
      }

      return new RpcRetryingCallerImpl<AbstractResponse>(100, 500, 10, 9) {
        @Override
        public MultiResponse callWithoutRetries(RetryingCallable<AbstractResponse> callable,
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

  static MultiResponse createMultiResponse(final MultiAction multi,
      AtomicInteger nbMultiResponse, AtomicInteger nbActions, ResponseGenerator gen) {
    final MultiResponse mr = new MultiResponse();
    nbMultiResponse.incrementAndGet();
    for (Map.Entry<byte[], List<Action>> entry : multi.actions.entrySet()) {
      byte[] regionName = entry.getKey();
      for (Action a : entry.getValue()) {
        nbActions.incrementAndGet();
        gen.addResponse(mr, regionName, a);
      }
    }
    return mr;
  }

  private static interface ResponseGenerator {
    void addResponse(final MultiResponse mr, byte[] regionName, Action a);
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
      super(CONF);
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
  public void testSubmitLargeRequestWithUnlimitedSize() throws Exception {
    long maxHeapSizePerRequest = Long.MAX_VALUE;
    long putsHeapSize = 2 * 1024 * 1024;
    doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
  }

  @Test(timeout=300000)
  public void testSubmitRandomSizeRequest() throws Exception {
    Random rn = new Random();
    final long limit = 10 * 1024 * 1024;
    final int requestCount = 1 + (int) (rn.nextDouble() * 3);
    long putsHeapSize = Math.abs(rn.nextLong()) % limit;
    long maxHeapSizePerRequest = putsHeapSize / requestCount;
    LOG.info("[testSubmitRandomSizeRequest] maxHeapSizePerRequest=" + maxHeapSizePerRequest +
        ", putsHeapSize=" + putsHeapSize);
    doSubmitRequest(maxHeapSizePerRequest, putsHeapSize);
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
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    final long defaultHeapSizePerRequest = conn.getConfiguration().getLong(
      SimpleRequestController.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
      SimpleRequestController.DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    conn.getConfiguration().setLong(SimpleRequestController.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, maxHeapSizePerRequest);

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

    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    try (HTable ht = new HTable(conn, mutator)) {
      Assert.assertEquals(0L, ht.mutator.getCurrentWriteBufferSize());
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
    conn.getConfiguration().setLong(SimpleRequestController.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, defaultHeapSizePerRequest);
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
  }

  @Test
  public void testSubmit() throws Exception {
    ClusterConnection hc = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(hc, CONF);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
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
    MyAsyncProcess ap = new MyAsyncProcess(hc, CONF);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    final AsyncRequestFuture ars = ap.submit(null, DUMMY_TABLE, puts, false, cb, false);
    Assert.assertTrue(puts.isEmpty());
    ars.waitUntilDone();
    Assert.assertEquals(updateCalled.get(), 1);
  }

  @Test
  public void testSubmitBusyRegion() throws Exception {
    ClusterConnection conn = createHConnection();
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF);
    SimpleRequestController controller = (SimpleRequestController) ap.requestController;
    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    for (int i = 0; i != controller.maxConcurrentTasksPerRegion; ++i) {
      ap.incTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    }
    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(puts.size(), 1);

    ap.decTaskCounters(Arrays.asList(hri1.getRegionName()), sn);
    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(0, puts.size());
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
  }


  @Test
  public void testSubmitBusyRegionServer() throws Exception {
    ClusterConnection conn = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF);
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    SimpleRequestController controller = (SimpleRequestController) ap.requestController;
    controller.taskCounterPerServer.put(sn2, new AtomicInteger(controller.maxConcurrentTasksPerServer));

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));
    puts.add(createPut(3, true)); // <== this one won't be taken, the rs is busy
    puts.add(createPut(1, true)); // <== this one will make it, the region is already in
    puts.add(createPut(2, true)); // <== new region, but the rs is ok

    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
    Assert.assertEquals(" puts=" + puts, 1, puts.size());

    controller.taskCounterPerServer.put(sn2, new AtomicInteger(controller.maxConcurrentTasksPerServer - 1));
    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
    Assert.assertTrue(puts.isEmpty());
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
  }

  @Test
  public void testFail() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), CONF, false);

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, false);
    puts.add(p);

    AsyncRequestFuture ars = ap.submit(null, DUMMY_TABLE, puts, false, null, true);
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
    ClusterConnection conn = createHConnection();
    final MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, false);
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    SimpleRequestController controller = (SimpleRequestController) ap.requestController;
    controller.tasksInProgress.incrementAndGet();
    final AtomicInteger ai = new AtomicInteger(controller.maxConcurrentTasksPerRegion);
    controller.taskCounterPerRegion.put(hri1.getRegionName(), ai);

    final AtomicBoolean checkPoint = new AtomicBoolean(false);
    final AtomicBoolean checkPoint2 = new AtomicBoolean(false);

    Thread t = new Thread(){
      @Override
      public void run(){
        Threads.sleep(1000);
        Assert.assertFalse(checkPoint.get()); // TODO: this is timing-dependent
        ai.decrementAndGet();
        controller.tasksInProgress.decrementAndGet();
        checkPoint2.set(true);
      }
    };

    List<Put> puts = new ArrayList<Put>();
    Put p = createPut(1, true);
    puts.add(p);

    ap.submit(null, DUMMY_TABLE, puts, false, null, false);
    Assert.assertFalse(puts.isEmpty());

    t.start();

    ap.submit(null, DUMMY_TABLE, puts, true, null, false);
    Assert.assertTrue(puts.isEmpty());

    checkPoint.set(true);
    while (!checkPoint2.get()){
      Threads.sleep(1);
    }
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
  }

  @Test
  public void testFailAndSuccess() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), CONF, false);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    AsyncRequestFuture ars = ap.submit(null, DUMMY_TABLE, puts, false, null, true);
    Assert.assertTrue(puts.isEmpty());
    ars.waitUntilDone();
    verifyResult(ars, false, true, true);
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());
    ap.callsCt.set(0);
    Assert.assertEquals(1, ars.getErrors().actions.size());

    puts.add(createPut(1, true));
    // Wait for AP to be free. While ars might have the result, ap counters are decreased later.
    ap.waitForMaximumCurrentTasks(0, null);
    ars = ap.submit(null, DUMMY_TABLE, puts, false, null, true);
    Assert.assertEquals(0, puts.size());
    ars.waitUntilDone();
    Assert.assertEquals(1, ap.callsCt.get());
    verifyResult(ars, true);
  }

  @Test
  public void testFlush() throws Exception {
    MyAsyncProcess ap = new MyAsyncProcess(createHConnection(), CONF, false);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, false));
    puts.add(createPut(1, true));
    puts.add(createPut(1, true));

    AsyncRequestFuture ars = ap.submit(null, DUMMY_TABLE, puts, false, null, true);
    ars.waitUntilDone();
    verifyResult(ars, false, true, true);
    Assert.assertEquals(NB_RETRIES + 1, ap.callsCt.get());

    Assert.assertEquals(1, ars.getFailedOperations().size());
  }

  @Test
  public void testTaskCountWithoutClientBackoffPolicy() throws IOException, InterruptedException {
    ClusterConnection hc = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(hc, CONF, false);
    testTaskCount(ap);
  }

  @Test
  public void testTaskCountWithClientBackoffPolicy() throws IOException, InterruptedException {
    Configuration copyConf = new Configuration(CONF);
    copyConf.setBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE, true);
    MyClientBackoffPolicy bp = new MyClientBackoffPolicy();
    ClusterConnection conn = createHConnection();
    Mockito.when(conn.getConfiguration()).thenReturn(copyConf);
    Mockito.when(conn.getStatisticsTracker()).thenReturn(ServerStatisticTracker.create(copyConf));
    Mockito.when(conn.getBackoffPolicy()).thenReturn(bp);
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    MyAsyncProcess ap = new MyAsyncProcess(conn, copyConf, false);
    testTaskCount(ap);
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
  }

  private void testTaskCount(MyAsyncProcess ap) throws InterruptedIOException, InterruptedException {
    SimpleRequestController controller = (SimpleRequestController) ap.requestController;
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i != 3; ++i) {
      puts.add(createPut(1, true));
      puts.add(createPut(2, true));
      puts.add(createPut(3, true));
    }
    ap.submit(null, DUMMY_TABLE, puts, true, null, false);
    ap.waitForMaximumCurrentTasks(0, null);
    // More time to wait if there are incorrect task count.
    TimeUnit.SECONDS.sleep(1);
    assertEquals(0, controller.tasksInProgress.get());
    for (AtomicInteger count : controller.taskCounterPerRegion.values()) {
      assertEquals(0, count.get());
    }
    for (AtomicInteger count : controller.taskCounterPerServer.values()) {
      assertEquals(0, count.get());
    }
  }

  @Test
  public void testMaxTask() throws Exception {
    ClusterConnection conn = createHConnection();
    final String defaultClazz = conn.getConfiguration().get(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY);
    conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class.getName());
    final MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, false);
    SimpleRequestController controller = (SimpleRequestController) ap.requestController;


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
      ap.submit(null, DUMMY_TABLE, puts, false, null, false);
      Assert.fail("We should have been interrupted.");
    } catch (InterruptedIOException expected) {
    }

    final long sleepTime = 2000;

    Thread t2 = new Thread() {
      @Override
      public void run() {
        Threads.sleep(sleepTime);
        while (controller.tasksInProgress.get() > 0) {
          ap.decTaskCounters(Arrays.asList("dummy".getBytes()), sn);
        }
      }
    };
    t2.start();

    long start = System.currentTimeMillis();
    ap.submit(null, DUMMY_TABLE, new ArrayList<Row>(), false, null, false);
    long end = System.currentTimeMillis();

    //Adds 100 to secure us against approximate timing.
    Assert.assertTrue(start + 100L + sleepTime > end);
    if (defaultClazz != null) {
      conn.getConfiguration().set(RequestControllerFactory.REQUEST_CONTROLLER_IMPL_CONF_KEY,
        defaultClazz);
    }
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
    Mockito.when(hc.getConfiguration()).thenReturn(CONF);
    Mockito.when(hc.getConnectionConfiguration()).thenReturn(CONNECTION_CONFIG);
    return hc;
  }

  @Test
  public void testHTablePutSuccess() throws Exception {
    ClusterConnection conn = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl ht = new BufferedMutatorImpl(conn, bufferParam, ap);

    Put put = createPut(1, true);

    Assert.assertEquals(conn.getConnectionConfiguration().getWriteBufferSize(), ht.getWriteBufferSize());
    Assert.assertEquals(0, ht.getCurrentWriteBufferSize());
    ht.mutate(put);
    ht.flush();
    Assert.assertEquals(0, ht.getCurrentWriteBufferSize());
  }

  @Test
  public void testBufferedMutatorImplWithSharedPool() throws Exception {
    ClusterConnection conn = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutator ht = new BufferedMutatorImpl(conn, bufferParam, ap);

    ht.close();
    assertFalse(ap.service.isShutdown());
  }

  private void doHTableFailedPut(boolean bufferOn) throws Exception {
    ClusterConnection conn = createHConnection();
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    if (bufferOn) {
      bufferParam.writeBufferSize(1024L * 1024L);
    } else {
      bufferParam.writeBufferSize(0L);
    }
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    HTable ht = new HTable(conn, mutator);

    Put put = createPut(1, false);

    Assert.assertEquals(0L, ht.mutator.getCurrentWriteBufferSize());
    try {
      ht.put(put);
      if (bufferOn) {
        ht.flushCommits();
      }
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals(0L, ht.mutator.getCurrentWriteBufferSize());
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
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE)
            .writeBufferSize(0);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);

    Put p = createPut(1, false);
    mutator.mutate(p);

    ap.waitForMaximumCurrentTasks(0, null); // Let's do all the retries.

    // We're testing that we're behaving as we were behaving in 0.94: sending exceptions in the
    //  doPut if it fails.
    // This said, it's not a very easy going behavior. For example, when we insert a list of
    //  puts, we may raise an exception in the middle of the list. It's then up to the caller to
    //  manage what was inserted, what was tried but failed, and what was not even tried.
    p = createPut(1, true);
    Assert.assertEquals(0, mutator.size());
    try {
      mutator.mutate(p);
      Assert.fail();
    } catch (RetriesExhaustedException expected) {
    }
    Assert.assertEquals("the put should not been inserted.", 0, mutator.size());
  }

  @Test
  public void testAction() {
    Action action_0 = new Action(new Put(Bytes.toBytes("abc")), 10);
    Action action_1 = new Action(new Put(Bytes.toBytes("ccc")), 10);
    Action action_2 = new Action(new Put(Bytes.toBytes("ccc")), 10);
    Action action_3 = new Action(new Delete(Bytes.toBytes("ccc")), 10);
    assertFalse(action_0.equals(action_1));
    assertTrue(action_0.equals(action_0));
    assertTrue(action_1.equals(action_2));
    assertTrue(action_2.equals(action_1));
    assertFalse(action_0.equals(new Put(Bytes.toBytes("abc"))));
    assertTrue(action_2.equals(action_3));
    assertFalse(action_0.equals(action_3));
    assertEquals(0, action_0.compareTo(action_0));
    assertEquals(-1, action_0.compareTo(action_1));
    assertEquals(1, action_1.compareTo(action_0));
    assertEquals(0, action_1.compareTo(action_2));
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    ClusterConnection conn = new MyConnectionImpl(CONF);
    MyAsyncProcess ap = new MyAsyncProcess(conn, CONF, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    HTable ht = new HTable(conn, mutator);
    ht.multiAp = new MyAsyncProcess(conn, CONF, false);

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
      ht.batch(puts, res);
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
    Configuration configuration = new Configuration(CONF);
    ClusterConnection conn = new MyConnectionImpl(configuration);
    MyAsyncProcess ap = new MyAsyncProcess(conn, configuration, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    configuration.setBoolean(ConnectionImplementation.RETRIES_BY_SERVER_KEY, true);

    Assert.assertNotNull(ap.createServerErrorTracker());
    Assert.assertTrue(ap.serverTrackerTimeout > 200);
    ap.serverTrackerTimeout = 1;

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
  public void testReadAndWriteTimeout() throws IOException {
    final long readTimeout = 10 * 1000;
    final long writeTimeout = 20 * 1000;
    Configuration copyConf = new Configuration(CONF);
    copyConf.setLong(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, readTimeout);
    copyConf.setLong(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, writeTimeout);
    ClusterConnection conn = createHConnection();
    Mockito.when(conn.getConfiguration()).thenReturn(copyConf);
    MyAsyncProcess ap = new MyAsyncProcess(conn, copyConf, true);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    try (HTable ht = new HTable(conn, mutator)) {
      ht.multiAp = ap;
      List<Get> gets = new LinkedList<>();
      gets.add(new Get(DUMMY_BYTES_1));
      gets.add(new Get(DUMMY_BYTES_2));
      try {
        ht.get(gets);
      } catch (ClassCastException e) {
        // No result response on this test.
      }
      assertEquals(readTimeout, ap.previousTimeout);
      ap.previousTimeout = -1;

      try {
        ht.existsAll(gets);
      } catch (ClassCastException e) {
        // No result response on this test.
      }
      assertEquals(readTimeout, ap.previousTimeout);
      ap.previousTimeout = -1;

      List<Delete> deletes = new LinkedList<>();
      deletes.add(new Delete(DUMMY_BYTES_1));
      deletes.add(new Delete(DUMMY_BYTES_2));
      ht.delete(deletes);
      assertEquals(writeTimeout, ap.previousTimeout);
    }
  }

  @Test
  public void testGlobalErrors() throws IOException {
    ClusterConnection conn = new MyConnectionImpl(CONF);
    AsyncProcessWithFailure ap = new AsyncProcessWithFailure(conn, CONF, new IOException("test"));
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);

    Assert.assertNotNull(ap.createServerErrorTracker());

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
    ClusterConnection conn = new MyConnectionImpl(CONF);
    AsyncProcessWithFailure ap = new AsyncProcessWithFailure(conn, CONF, new CallQueueTooBigException());
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    Assert.assertNotNull(ap.createServerErrorTracker());
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
    MyAsyncProcess ap = new MyAsyncProcess(con, CONF, con.nbThreads);
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(con , bufferParam, ap);
    HTable ht = new HTable(con, mutator);
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
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[3])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
    verifyReplicaResult(ars, RR.TRUE, RR.TRUE, RR.FALSE);
    Assert.assertEquals(2, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaPrimarySuccessWoReplicaCalls() throws Exception {
    // Main call succeeds before replica calls are kicked off.
    MyAsyncProcessWithReplicas ap = createReplicaAp(1000, 10, 0);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2, DUMMY_BYTES_3);
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[3])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
    verifyReplicaResult(ars, RR.FALSE, RR.FALSE, RR.FALSE);
    Assert.assertEquals(0, ap.getReplicaCallCount());
  }

  @Test
  public void testReplicaParallelCallsSucceed() throws Exception {
    // Either main or replica can succeed.
    MyAsyncProcessWithReplicas ap = createReplicaAp(0, 0, 0);
    List<Get> rows = makeTimelineGets(DUMMY_BYTES_1, DUMMY_BYTES_2);
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[2])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
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
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[2])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
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
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[2])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
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
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[2])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
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
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(ap.service)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(rows)
            .setResults(new Object[2])
            .setSubmittedRows(SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = ap.submit(task);
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

  private static BufferedMutatorParams createBufferedMutatorParams(MyAsyncProcess ap, TableName name) {
    return new BufferedMutatorParams(name)
            .pool(ap.service)
            .rpcTimeout(RPC_TIMEOUT)
            .opertationTimeout(OPERATION_TIMEOUT);
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
    public AsyncProcessForThrowableCheck(ClusterConnection hc, Configuration conf) {
      super(hc, conf, new RpcRetryingCallerFactory(conf), false, new RpcControllerFactory(
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
    AsyncProcess ap = new AsyncProcessForThrowableCheck(hc, CONF);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(myPool)
            .setRpcTimeout(RPC_TIMEOUT)
            .setOperationTimeout(OPERATION_TIMEOUT)
            .setTableName(DUMMY_TABLE)
            .setRowAccess(puts)
            .setSubmittedRows(SubmittedRows.NORMAL)
            .build();
    ap.submit(task);
    Assert.assertTrue(puts.isEmpty());
  }

  /**
   * Test and make sure we could use a special pause setting when retry with
   * CallQueueTooBigException, see HBASE-17114
   * @throws Exception if unexpected error happened during test
   */
  @Test
  public void testRetryPauseWithCallQueueTooBigException() throws Exception {
    Configuration myConf = new Configuration(CONF);
    final long specialPause = 500L;
    final int retries = 1;
    myConf.setLong(HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE, specialPause);
    myConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    ClusterConnection conn = new MyConnectionImpl(myConf);
    AsyncProcessWithFailure ap =
        new AsyncProcessWithFailure(conn, myConf, new CallQueueTooBigException());
    BufferedMutatorParams bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    BufferedMutatorImpl mutator = new BufferedMutatorImpl(conn, bufferParam, ap);

    Assert.assertNotNull(mutator.getAsyncProcess().createServerErrorTracker());

    Put p = createPut(1, true);
    mutator.mutate(p);

    long startTime = System.currentTimeMillis();
    try {
      mutator.flush();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    long actualSleep = System.currentTimeMillis() - startTime;
    long expectedSleep = 0L;
    for (int i = 0; i < retries; i++) {
      expectedSleep += ConnectionUtils.getPauseTime(specialPause, i);
      // Prevent jitter in CollectionUtils#getPauseTime to affect result
      actualSleep += (long) (specialPause * 0.01f);
    }
    LOG.debug("Expected to sleep " + expectedSleep + "ms, actually slept " + actualSleep + "ms");
    Assert.assertTrue("Expected to sleep " + expectedSleep + " but actually " + actualSleep + "ms",
      actualSleep >= expectedSleep);

    // check and confirm normal IOE will use the normal pause
    final long normalPause =
        myConf.getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    ap = new AsyncProcessWithFailure(conn, myConf, new IOException());
    bufferParam = createBufferedMutatorParams(ap, DUMMY_TABLE);
    mutator = new BufferedMutatorImpl(conn, bufferParam, ap);
    Assert.assertNotNull(mutator.getAsyncProcess().createServerErrorTracker());
    mutator.mutate(p);
    startTime = System.currentTimeMillis();
    try {
      mutator.flush();
      Assert.fail();
    } catch (RetriesExhaustedWithDetailsException expected) {
    }
    actualSleep = System.currentTimeMillis() - startTime;
    expectedSleep = 0L;
    for (int i = 0; i < retries; i++) {
      expectedSleep += ConnectionUtils.getPauseTime(normalPause, i);
    }
    // plus an additional pause to balance the program execution time
    expectedSleep += normalPause;
    LOG.debug("Expected to sleep " + expectedSleep + "ms, actually slept " + actualSleep + "ms");
    Assert.assertTrue("Slept for too long: " + actualSleep + "ms", actualSleep <= expectedSleep);
  }
}
