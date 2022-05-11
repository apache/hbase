/*
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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HTableMultiplexer provides a thread-safe non blocking PUT API across all the tables. Each put
 * will be sharded into different buffer queues based on its destination region server. So each
 * region server buffer queue will only have the puts which share the same destination. And each
 * queue will have a flush worker thread to flush the puts request to the region server. If any
 * queue is full, the HTableMultiplexer starts to drop the Put requests for that particular queue.
 * </p>
 * Also all the puts will be retried as a configuration number before dropping. And the
 * HTableMultiplexer can report the number of buffered requests and the number of the failed
 * (dropped) requests in total or on per region server basis.
 * <p/>
 * This class is thread safe.
 * @deprecated since 2.2.0, will be removed in 3.0.0, without replacement. Please use
 *             {@link BufferedMutator} for batching mutations.
 */
@Deprecated
@InterfaceAudience.Public
public class HTableMultiplexer {
  private static final Logger LOG = LoggerFactory.getLogger(HTableMultiplexer.class.getName());

  public static final String TABLE_MULTIPLEXER_FLUSH_PERIOD_MS =
    "hbase.tablemultiplexer.flush.period.ms";
  public static final String TABLE_MULTIPLEXER_INIT_THREADS = "hbase.tablemultiplexer.init.threads";
  public static final String TABLE_MULTIPLEXER_MAX_RETRIES_IN_QUEUE =
    "hbase.client.max.retries.in.queue";

  /** The map between each region server to its flush worker */
  private final Map<HRegionLocation, FlushWorker> serverToFlushWorkerMap =
    new ConcurrentHashMap<>();

  private final Configuration workerConf;
  private final ClusterConnection conn;
  private final ExecutorService pool;
  private final int maxAttempts;
  private final int perRegionServerBufferQueueSize;
  private final int maxKeyValueSize;
  private final ScheduledExecutorService executor;
  private final long flushPeriod;

  /**
   * @param conf                           The HBaseConfiguration
   * @param perRegionServerBufferQueueSize determines the max number of the buffered Put ops for
   *                                       each region server before dropping the request.
   */
  public HTableMultiplexer(Configuration conf, int perRegionServerBufferQueueSize)
    throws IOException {
    this(ConnectionFactory.createConnection(conf), conf, perRegionServerBufferQueueSize);
  }

  /**
   * @param conn                           The HBase connection.
   * @param conf                           The HBase configuration
   * @param perRegionServerBufferQueueSize determines the max number of the buffered Put ops for
   *                                       each region server before dropping the request.
   */
  public HTableMultiplexer(Connection conn, Configuration conf,
    int perRegionServerBufferQueueSize) {
    this.conn = (ClusterConnection) conn;
    this.pool = HTable.getDefaultExecutor(conf);
    // how many times we could try in total, one more than retry number
    this.maxAttempts = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER) + 1;
    this.perRegionServerBufferQueueSize = perRegionServerBufferQueueSize;
    this.maxKeyValueSize = HTable.getMaxKeyValueSize(conf);
    this.flushPeriod = conf.getLong(TABLE_MULTIPLEXER_FLUSH_PERIOD_MS, 100);
    int initThreads = conf.getInt(TABLE_MULTIPLEXER_INIT_THREADS, 10);
    this.executor = Executors.newScheduledThreadPool(initThreads,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("HTableFlushWorker-%d").build());

    this.workerConf = HBaseConfiguration.create(conf);
    // We do not do the retry because we need to reassign puts to different queues if regions are
    // moved.
    this.workerConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
  }

  /**
   * Closes the internal {@link Connection}. Does nothing if the {@link Connection} has already been
   * closed.
   * @throws IOException If there is an error closing the connection.
   */
  public synchronized void close() throws IOException {
    if (!getConnection().isClosed()) {
      getConnection().close();
    }
  }

  /**
   * The put request will be buffered by its corresponding buffer queue. Return false if the queue
   * is already full. nn * @return true if the request can be accepted by its corresponding buffer
   * queue.
   */
  public boolean put(TableName tableName, final Put put) {
    return put(tableName, put, this.maxAttempts);
  }

  /**
   * The puts request will be buffered by their corresponding buffer queue. Return the list of puts
   * which could not be queued. nn * @return the list of puts which could not be queued
   */
  public List<Put> put(TableName tableName, final List<Put> puts) {
    if (puts == null) return null;

    List<Put> failedPuts = null;
    boolean result;
    for (Put put : puts) {
      result = put(tableName, put, this.maxAttempts);
      if (result == false) {

        // Create the failed puts list if necessary
        if (failedPuts == null) {
          failedPuts = new ArrayList<>();
        }
        // Add the put to the failed puts list
        failedPuts.add(put);
      }
    }
    return failedPuts;
  }

  /**
   * @deprecated Use {@link #put(TableName, List) } instead.
   */
  @Deprecated
  public List<Put> put(byte[] tableName, final List<Put> puts) {
    return put(TableName.valueOf(tableName), puts);
  }

  /**
   * The put request will be buffered by its corresponding buffer queue. And the put request will be
   * retried before dropping the request. Return false if the queue is already full.
   * @return true if the request can be accepted by its corresponding buffer queue.
   */
  public boolean put(final TableName tableName, final Put put, int maxAttempts) {
    if (maxAttempts <= 0) {
      return false;
    }

    try {
      ConnectionUtils.validatePut(put, maxKeyValueSize);
      // Allow mocking to get at the connection, but don't expose the connection to users.
      ClusterConnection conn = (ClusterConnection) getConnection();
      // AsyncProcess in the FlushWorker should take care of refreshing the location cache
      // as necessary. We shouldn't have to do that here.
      HRegionLocation loc = conn.getRegionLocation(tableName, put.getRow(), false);
      if (loc != null) {
        // Add the put pair into its corresponding queue.
        LinkedBlockingQueue<PutStatus> queue = getQueue(loc);

        // Generate a MultiPutStatus object and offer it into the queue
        PutStatus s = new PutStatus(loc.getRegion(), put, maxAttempts);

        return queue.offer(s);
      }
    } catch (IOException e) {
      LOG.debug("Cannot process the put " + put, e);
    }
    return false;
  }

  /**
   * @deprecated Use {@link #put(TableName, Put) } instead.
   */
  @Deprecated
  public boolean put(final byte[] tableName, final Put put, int retry) {
    return put(TableName.valueOf(tableName), put, retry);
  }

  /**
   * @deprecated Use {@link #put(TableName, Put)} instead.
   */
  @Deprecated
  public boolean put(final byte[] tableName, Put put) {
    return put(TableName.valueOf(tableName), put);
  }

  /**
   * @return the current HTableMultiplexerStatus
   */
  public HTableMultiplexerStatus getHTableMultiplexerStatus() {
    return new HTableMultiplexerStatus(serverToFlushWorkerMap);
  }

  @InterfaceAudience.Private
  LinkedBlockingQueue<PutStatus> getQueue(HRegionLocation addr) {
    FlushWorker worker = serverToFlushWorkerMap.get(addr);
    if (worker == null) {
      synchronized (this.serverToFlushWorkerMap) {
        worker = serverToFlushWorkerMap.get(addr);
        if (worker == null) {
          // Create the flush worker
          worker = new FlushWorker(workerConf, this.conn, addr, this,
            perRegionServerBufferQueueSize, pool, executor);
          this.serverToFlushWorkerMap.put(addr, worker);
          executor.scheduleAtFixedRate(worker, flushPeriod, flushPeriod, TimeUnit.MILLISECONDS);
        }
      }
    }
    return worker.getQueue();
  }

  @InterfaceAudience.Private
  ClusterConnection getConnection() {
    return this.conn;
  }

  /**
   * HTableMultiplexerStatus keeps track of the current status of the HTableMultiplexer. report the
   * number of buffered requests and the number of the failed (dropped) requests in total or on per
   * region server basis.
   * @deprecated since 2.2.0, will be removed in 3.0.0, without replacement. Please use
   *             {@link BufferedMutator} for batching mutations.
   */
  @Deprecated
  @InterfaceAudience.Public
  public static class HTableMultiplexerStatus {
    private long totalFailedPutCounter;
    private long totalBufferedPutCounter;
    private long maxLatency;
    private long overallAverageLatency;
    private Map<String, Long> serverToFailedCounterMap;
    private Map<String, Long> serverToBufferedCounterMap;
    private Map<String, Long> serverToAverageLatencyMap;
    private Map<String, Long> serverToMaxLatencyMap;

    public HTableMultiplexerStatus(Map<HRegionLocation, FlushWorker> serverToFlushWorkerMap) {
      this.totalBufferedPutCounter = 0;
      this.totalFailedPutCounter = 0;
      this.maxLatency = 0;
      this.overallAverageLatency = 0;
      this.serverToBufferedCounterMap = new HashMap<>();
      this.serverToFailedCounterMap = new HashMap<>();
      this.serverToAverageLatencyMap = new HashMap<>();
      this.serverToMaxLatencyMap = new HashMap<>();
      this.initialize(serverToFlushWorkerMap);
    }

    private void initialize(Map<HRegionLocation, FlushWorker> serverToFlushWorkerMap) {
      if (serverToFlushWorkerMap == null) {
        return;
      }

      long averageCalcSum = 0;
      int averageCalcCount = 0;
      for (Map.Entry<HRegionLocation, FlushWorker> entry : serverToFlushWorkerMap.entrySet()) {
        HRegionLocation addr = entry.getKey();
        FlushWorker worker = entry.getValue();

        long bufferedCounter = worker.getTotalBufferedCount();
        long failedCounter = worker.getTotalFailedCount();
        long serverMaxLatency = worker.getMaxLatency();
        AtomicAverageCounter averageCounter = worker.getAverageLatencyCounter();
        // Get sum and count pieces separately to compute overall average
        SimpleEntry<Long, Integer> averageComponents = averageCounter.getComponents();
        long serverAvgLatency = averageCounter.getAndReset();

        this.totalBufferedPutCounter += bufferedCounter;
        this.totalFailedPutCounter += failedCounter;
        if (serverMaxLatency > this.maxLatency) {
          this.maxLatency = serverMaxLatency;
        }
        averageCalcSum += averageComponents.getKey();
        averageCalcCount += averageComponents.getValue();

        this.serverToBufferedCounterMap.put(addr.getHostnamePort(), bufferedCounter);
        this.serverToFailedCounterMap.put(addr.getHostnamePort(), failedCounter);
        this.serverToAverageLatencyMap.put(addr.getHostnamePort(), serverAvgLatency);
        this.serverToMaxLatencyMap.put(addr.getHostnamePort(), serverMaxLatency);
      }
      this.overallAverageLatency = averageCalcCount != 0 ? averageCalcSum / averageCalcCount : 0;
    }

    public long getTotalBufferedCounter() {
      return this.totalBufferedPutCounter;
    }

    public long getTotalFailedCounter() {
      return this.totalFailedPutCounter;
    }

    public long getMaxLatency() {
      return this.maxLatency;
    }

    public long getOverallAverageLatency() {
      return this.overallAverageLatency;
    }

    public Map<String, Long> getBufferedCounterForEachRegionServer() {
      return this.serverToBufferedCounterMap;
    }

    public Map<String, Long> getFailedCounterForEachRegionServer() {
      return this.serverToFailedCounterMap;
    }

    public Map<String, Long> getMaxLatencyForEachRegionServer() {
      return this.serverToMaxLatencyMap;
    }

    public Map<String, Long> getAverageLatencyForEachRegionServer() {
      return this.serverToAverageLatencyMap;
    }
  }

  @InterfaceAudience.Private
  static class PutStatus {
    final RegionInfo regionInfo;
    final Put put;
    final int maxAttempCount;

    public PutStatus(RegionInfo regionInfo, Put put, int maxAttempCount) {
      this.regionInfo = regionInfo;
      this.put = put;
      this.maxAttempCount = maxAttempCount;
    }
  }

  /**
   * Helper to count the average over an interval until reset.
   */
  private static class AtomicAverageCounter {
    private long sum;
    private int count;

    public AtomicAverageCounter() {
      this.sum = 0L;
      this.count = 0;
    }

    public synchronized long getAndReset() {
      long result = this.get();
      this.reset();
      return result;
    }

    public synchronized long get() {
      if (this.count == 0) {
        return 0;
      }
      return this.sum / this.count;
    }

    public synchronized SimpleEntry<Long, Integer> getComponents() {
      return new SimpleEntry<>(sum, count);
    }

    public synchronized void reset() {
      this.sum = 0L;
      this.count = 0;
    }

    public synchronized void add(long value) {
      this.sum += value;
      this.count++;
    }
  }

  @InterfaceAudience.Private
  static class FlushWorker implements Runnable {
    private final HRegionLocation addr;
    private final LinkedBlockingQueue<PutStatus> queue;
    private final HTableMultiplexer multiplexer;
    private final AtomicLong totalFailedPutCount = new AtomicLong(0);
    private final AtomicInteger currentProcessingCount = new AtomicInteger(0);
    private final AtomicAverageCounter averageLatency = new AtomicAverageCounter();
    private final AtomicLong maxLatency = new AtomicLong(0);

    private final AsyncProcess ap;
    private final List<PutStatus> processingList = new ArrayList<>();
    private final ScheduledExecutorService executor;
    private final int maxRetryInQueue;
    private final AtomicInteger retryInQueue = new AtomicInteger(0);
    private final int writeRpcTimeout; // needed to pass in through AsyncProcess constructor
    private final int operationTimeout;
    private final ExecutorService pool;

    public FlushWorker(Configuration conf, ClusterConnection conn, HRegionLocation addr,
      HTableMultiplexer htableMultiplexer, int perRegionServerBufferQueueSize, ExecutorService pool,
      ScheduledExecutorService executor) {
      this.addr = addr;
      this.multiplexer = htableMultiplexer;
      this.queue = new LinkedBlockingQueue<>(perRegionServerBufferQueueSize);
      RpcRetryingCallerFactory rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf);
      RpcControllerFactory rpcControllerFactory = RpcControllerFactory.instantiate(conf);
      this.writeRpcTimeout = conf.getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
        conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
      this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
      this.ap = new AsyncProcess(conn, conf, rpcCallerFactory, rpcControllerFactory);
      this.executor = executor;
      this.maxRetryInQueue = conf.getInt(TABLE_MULTIPLEXER_MAX_RETRIES_IN_QUEUE, 10000);
      this.pool = pool;
    }

    protected LinkedBlockingQueue<PutStatus> getQueue() {
      return this.queue;
    }

    public long getTotalFailedCount() {
      return totalFailedPutCount.get();
    }

    public long getTotalBufferedCount() {
      return (long) queue.size() + currentProcessingCount.get();
    }

    public AtomicAverageCounter getAverageLatencyCounter() {
      return this.averageLatency;
    }

    public long getMaxLatency() {
      return this.maxLatency.getAndSet(0);
    }

    boolean resubmitFailedPut(PutStatus ps, HRegionLocation oldLoc) throws IOException {
      // Decrease the retry count
      final int retryCount = ps.maxAttempCount - 1;

      if (retryCount <= 0) {
        // Update the failed counter and no retry any more.
        return false;
      }

      int cnt = getRetryInQueue().incrementAndGet();
      if (cnt > getMaxRetryInQueue()) {
        // Too many Puts in queue for resubmit, give up this
        getRetryInQueue().decrementAndGet();
        return false;
      }

      final Put failedPut = ps.put;
      // The currentPut is failed. So get the table name for the currentPut.
      final TableName tableName = ps.regionInfo.getTable();

      long delayMs = getNextDelay(retryCount);
      if (LOG.isDebugEnabled()) {
        LOG.debug("resubmitting after " + delayMs + "ms: " + retryCount);
      }

      // HBASE-12198, HBASE-15221, HBASE-15232: AsyncProcess should be responsible for updating
      // the region location cache when the Put original failed with some exception. If we keep
      // re-trying the same Put to the same location, AsyncProcess isn't doing the right stuff
      // that we expect it to.
      getExecutor().schedule(new Runnable() {
        @Override
        public void run() {
          boolean succ = false;
          try {
            succ = FlushWorker.this.getMultiplexer().put(tableName, failedPut, retryCount);
          } finally {
            FlushWorker.this.getRetryInQueue().decrementAndGet();
            if (!succ) {
              FlushWorker.this.getTotalFailedPutCount().incrementAndGet();
            }
          }
        }
      }, delayMs, TimeUnit.MILLISECONDS);
      return true;
    }

    @InterfaceAudience.Private
    long getNextDelay(int retryCount) {
      return ConnectionUtils.getPauseTime(multiplexer.flushPeriod,
        multiplexer.maxAttempts - retryCount - 1);
    }

    @InterfaceAudience.Private
    AtomicInteger getRetryInQueue() {
      return this.retryInQueue;
    }

    @InterfaceAudience.Private
    int getMaxRetryInQueue() {
      return this.maxRetryInQueue;
    }

    @InterfaceAudience.Private
    AtomicLong getTotalFailedPutCount() {
      return this.totalFailedPutCount;
    }

    @InterfaceAudience.Private
    HTableMultiplexer getMultiplexer() {
      return this.multiplexer;
    }

    @InterfaceAudience.Private
    ScheduledExecutorService getExecutor() {
      return this.executor;
    }

    @Override
    public void run() {
      int failedCount = 0;
      try {
        long start = EnvironmentEdgeManager.currentTime();

        // drain all the queued puts into the tmp list
        processingList.clear();
        queue.drainTo(processingList);
        if (processingList.isEmpty()) {
          // Nothing to flush
          return;
        }

        currentProcessingCount.set(processingList.size());
        // failedCount is decreased whenever a Put is success or resubmit.
        failedCount = processingList.size();

        List<Action> retainedActions = new ArrayList<>(processingList.size());
        MultiAction actions = new MultiAction();
        for (int i = 0; i < processingList.size(); i++) {
          PutStatus putStatus = processingList.get(i);
          Action action = new Action(putStatus.put, i);
          actions.add(putStatus.regionInfo.getRegionName(), action);
          retainedActions.add(action);
        }

        // Process this multi-put request
        List<PutStatus> failed = null;
        Object[] results = new Object[actions.size()];
        ServerName server = addr.getServerName();
        Map<ServerName, MultiAction> actionsByServer = Collections.singletonMap(server, actions);
        try {
          AsyncProcessTask task = AsyncProcessTask.newBuilder().setResults(results).setPool(pool)
            .setRpcTimeout(writeRpcTimeout).setOperationTimeout(operationTimeout).build();
          AsyncRequestFuture arf =
            ap.submitMultiActions(task, retainedActions, 0L, null, null, actionsByServer);
          arf.waitUntilDone();
          if (arf.hasError()) {
            // We just log and ignore the exception here since failed Puts will be resubmit again.
            LOG.debug("Caught some exceptions when flushing puts to region server "
              + addr.getHostnamePort(), arf.getErrors());
          }
        } finally {
          for (int i = 0; i < results.length; i++) {
            if (results[i] instanceof Result) {
              failedCount--;
            } else {
              if (failed == null) {
                failed = new ArrayList<>();
              }
              failed.add(processingList.get(i));
            }
          }
        }

        if (failed != null) {
          // Resubmit failed puts
          for (PutStatus putStatus : failed) {
            if (resubmitFailedPut(putStatus, this.addr)) {
              failedCount--;
            }
          }
        }

        long elapsed = EnvironmentEdgeManager.currentTime() - start;
        // Update latency counters
        averageLatency.add(elapsed);
        if (elapsed > maxLatency.get()) {
          maxLatency.set(elapsed);
        }

        // Log some basic info
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Processed " + currentProcessingCount + " put requests for " + addr.getHostnamePort()
              + " and " + failedCount + " failed" + ", latency for this send: " + elapsed);
        }

        // Reset the current processing put count
        currentProcessingCount.set(0);
      } catch (RuntimeException e) {
        // To make findbugs happy
        // Log all the exceptions and move on
        LOG.debug("Caught some exceptions " + e + " when flushing puts to region server "
          + addr.getHostnamePort(), e);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        // Log all the exceptions and move on
        LOG.debug("Caught some exceptions " + e + " when flushing puts to region server "
          + addr.getHostnamePort(), e);
      } finally {
        // Update the totalFailedCount
        this.totalFailedPutCount.addAndGet(failedCount);
      }
    }
  }
}
