/**
 * Copyright The Apache Software Foundation
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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HTableMultiplexer provides a thread-safe non blocking PUT API across all the tables.
 * Each put will be sharded into different buffer queues based on its destination region server.
 * So each region server buffer queue will only have the puts which share the same destination.
 * And each queue will have a flush worker thread to flush the puts request to the region server.
 * If any queue is full, the HTableMultiplexer starts to drop the Put requests for that
 * particular queue.
 *
 * Also all the puts will be retried as a configuration number before dropping.
 * And the HTableMultiplexer can report the number of buffered requests and the number of the
 * failed (dropped) requests in total or on per region server basis.
 *
 * This class is thread safe.
 */
public class HTableMultiplexer {
  private static final Log LOG = LogFactory.getLog(HTableMultiplexer.class.getName());
  private final Map<byte[], HTable> tableNameToHTableMap;

  /** The map between each region server to its corresponding buffer queue */
  private final Map<HServerAddress, LinkedBlockingQueue<PutStatus>>
    serverToBufferQueueMap;

  /** The map between each region server to its flush worker */
  private final Map<HServerAddress, HTableFlushWorker> serverToFlushWorkerMap;

  private final Configuration conf;
  private final HConnection connection;
  private final int retryNum;
  // limit of retried puts scheduled for retry
  private final int retriedInQueueMax;
  private final int perRegionServerBufferQueueSize;
  private final ScheduledExecutorService executor;
  private final long frequency;
  //initial number of threads in the pool
  public static final int INITIAL_NUM_THREADS = 10;

  /**
   *
   * @param conf The HBaseConfiguration
   * @param perRegionServerBufferQueueSize determines the max number of the buffered Put ops
   *         for each region server before dropping the request.
   */
  public HTableMultiplexer(Configuration conf, int perRegionServerBufferQueueSize) {
    this.conf = conf;
    this.connection = HConnectionManager.getConnection(conf);
    this.serverToBufferQueueMap = new ConcurrentHashMap<HServerAddress,
      LinkedBlockingQueue<PutStatus>>();
    this.serverToFlushWorkerMap = new ConcurrentHashMap<HServerAddress, HTableFlushWorker>();
    this.tableNameToHTableMap = new ConcurrentSkipListMap<byte[], HTable>(
            Bytes.BYTES_COMPARATOR);
    this.retryNum = conf.getInt(HConstants.CLIENT_RETRY_NUM_STRING, 10);
    this.retriedInQueueMax = conf.getInt("hbase.client.retried.inQueue", 10000);
    this.perRegionServerBufferQueueSize = perRegionServerBufferQueueSize;
    this.frequency = conf.getLong("hbase.htablemultiplexer.flush.frequency.ms",
        100);
    this.executor = Executors.newScheduledThreadPool(
        INITIAL_NUM_THREADS,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("HTableFlushWorker-%d").build());
  }

  /**
   * The put request will be buffered by its corresponding buffer queue. Return false if the queue
   * is already full.
   * @param table
   * @param put
   * @return true if the request can be accepted by its corresponding buffer queue.
   * @throws IOException
   */
  public boolean put(final byte[] table, final Put put,
      HBaseRPCOptions options) throws IOException {
    return put(table, put, this.retryNum, options);
  }

  /**
   * The puts request will be buffered by their corresponding buffer queue.
   * Return the list of puts which could not be queued.
   * @param table
   * @param put
   * @return the list of puts which could not be queued
   * @throws IOException
   */
  public List<Put> put(final byte[] table, final List<Put> puts,
      HBaseRPCOptions options) throws IOException {
    if (puts == null)
      return null;

    List <Put> failedPuts = null;
    boolean result;
    for (Put put : puts) {
      result = put(table, put, this.retryNum, options);
      if (result == false) {

        // Create the failed puts list if necessary
        if (failedPuts == null) {
          failedPuts = new ArrayList<Put>();
        }
        // Add the put to the failed puts list
        failedPuts.add(put);
      }
    }
    return failedPuts;
  }

  /**
   * The put request will be buffered by its corresponding buffer queue. And the put request will be
   * retried before dropping the request.
   * Return false if the queue is already full.
   * @param table
   * @param put
   * @param retry
   * @return true if the request can be accepted by its corresponding buffer queue.
   * @throws IOException
   */
  public boolean put(final byte[] table, final Put put, int retry,
      HBaseRPCOptions options) throws IOException {
    if (retry <= 0) {
      return false;
    }

    HTable htable = getHTable(table);
    try {
      htable.validatePut(put);
      HRegionLocation loc = htable.getRegionLocation(put.getRow());
      if (loc != null) {
        // Get the server location for the put
        HServerAddress addr = loc.getServerAddress();
        // Add the put pair into its corresponding queue.
        LinkedBlockingQueue<PutStatus> queue = getBufferedQueue(addr);
        // Generate a MultiPutStatus obj and offer it into the queue
        PutStatus s = new PutStatus(loc.getRegionInfo(), put, retry, options);

        return queue.offer(s);
      }
    } catch (Exception e) {
      LOG.debug("Cannot process the put " + put + " because of " + e);
    }
    return false;
  }

  /**
   * @return the current HTableMultiplexerStatus
   */
  public HTableMultiplexerStatus getHTableMultiplexerStatus() {
    return new HTableMultiplexerStatus(this.serverToFlushWorkerMap,
        this.tableNameToHTableMap);
  }

  private HTable getHTable(final byte[] table) throws IOException {
    HTable htable = this.tableNameToHTableMap.get(table);
    if (htable == null) {
      synchronized (this.tableNameToHTableMap) {
        htable = this.tableNameToHTableMap.get(table);
        if (htable == null)  {
          htable = new HTable(conf, table);
          this.tableNameToHTableMap.put(table, htable);
        }
      }
    }
    return htable;
  }

  private LinkedBlockingQueue<PutStatus> getBufferedQueue(
      HServerAddress addr) {
    LinkedBlockingQueue<PutStatus> queue;
    // Add the put pair into its corresponding queue.
    queue = serverToBufferQueueMap.get(addr);
    if (queue == null) {
      // Create the queue for the new region server
      queue = addNewRegionServer(addr);
    }
    return queue;
  }

  private synchronized LinkedBlockingQueue<PutStatus> addNewRegionServer(HServerAddress addr) {
    LinkedBlockingQueue<PutStatus> queue =
      serverToBufferQueueMap.get(addr);
    if (queue == null) {
      // Create a queue for the new region server
      queue = new LinkedBlockingQueue<PutStatus>(perRegionServerBufferQueueSize);
      serverToBufferQueueMap.put(addr, queue);

      // Create the flush worker
      HTableFlushWorker worker = new HTableFlushWorker(addr, queue);
      this.serverToFlushWorkerMap.put(addr, worker);
      executor.scheduleAtFixedRate(worker, frequency, frequency, TimeUnit.MILLISECONDS);
    }
    return queue;
  }

  public static class MultiPutBatchMetrics {
    private Map<String, Long> serverToMinMultiPutBatchSize;
    private Map<String, Long> serverToMaxMultiPutBatchSize;
    private Map<String, Double> serverToAvgMultiPutBatchSize;

    public MultiPutBatchMetrics() {
      this.serverToAvgMultiPutBatchSize = new HashMap<String, Double>();
      this.serverToMinMultiPutBatchSize = new HashMap<String, Long>();
      this.serverToMaxMultiPutBatchSize = new HashMap<String, Long>();
    }

    public void resetMultiPutMetrics(String hostnameWithPort,
        HTableFlushWorker worker) {
      double avg = worker.getAndResetAvgMultiPutCount();
      long min = worker.getAndResetMinMultiPutCount();
      long max = worker.getAndResetMaxMultiPutCount();
      this.serverToAvgMultiPutBatchSize.put(hostnameWithPort, avg);
      this.serverToMinMultiPutBatchSize.put(hostnameWithPort, min);
      this.serverToMaxMultiPutBatchSize.put(hostnameWithPort, max);
    }

    public long getMaxMultiPuBatchSizeForRs(String hostnameWithPort) {
      return this.serverToMaxMultiPutBatchSize.get(hostnameWithPort);
    }

    public long getMinMultiPutBatchSizeForRs(String hostnameWithPort) {
      return this.serverToMinMultiPutBatchSize.get(hostnameWithPort);
    }

    public double getAvgMultiPutBatchSizeForRs(String hostnameWithPort) {
      return this.serverToAvgMultiPutBatchSize.get(hostnameWithPort);
    }
  }

  /**
   * HTableMultiplexerStatus keeps track of the current status of the HTableMultiplexer.
   * report the number of buffered requests, the number of the failed (dropped)
   * requests, the number of succeeded and the number of retried requests
   * in total or on per region server basis.
   *
   * Some notes regarding the usage:
   * 1. The number of buffered requests reported by HTableMultiplexerStatus
   * can be > than the number of puts to HTableMultiplexer, because it reports
   * the number of puts in the queue + the number of puts being processed. If
   * some of the puts being processed need to be retried later, they are added
   * to the queue, before the processing ends. And hence, some puts might be
   * counted twice temporarily. But, eventually it will settle down to the
   * number of puts in the queue.
   *
   * 2. The number of retried puts, is the total number of times we retried
   * puts which failed. So, if a put has to be retried 3 times before it
   * succeeds, it will increase the retry count by 3.
   *
   * 3. The counters in the HTableMultiplexer object are calculated upon
   * instantiation, or when recalculateCounters() is called. Whenever you want
   * to get the latest numbers, you can either create a new one (by calling
   * getHTableMultiplexerStatus() on your HTableMultiplexer instance) or simply
   * do a .recalculateCounters() on the same status instance.
   *
   * 4. Some of the metrics, such as max latency are reset every time the
   * counters are calculated.
   */
  public static class HTableMultiplexerStatus {
    private long totalFailedPutCounter;
    private long totalSucceededPutCounter;
    private long totalRetriedPutCounter;
    private long totalBufferedPutCounter;
    private long maxLatency;
    private long overallAverageLatency;
    private Map<String, Long> serverToFailedCounterMap;
    private Map<String, Long> serverToSucceededCounterMap;
    private Map<String, Long> serverToRetriedCounterMap;
    private Map<String, Long> serverToBufferedCounterMap;
    private Map<String, Long> serverToAverageLatencyMap;
    private Map<String, Long> serverToMaxLatencyMap;
    private MultiPutBatchMetrics metrics;
    private long overallAvgMultiPutSize;
    private Map<HServerAddress, HTableFlushWorker> serverToFlushWorkerMap;
    private Map<byte[], HTable> tableNameToHTableMap;

    public HTableMultiplexerStatus(
        Map<HServerAddress, HTableFlushWorker> serverToFlushWorkerMap,
        Map<byte[], HTable> tableNameToHTableMap) {
      this.totalBufferedPutCounter = 0;
      this.totalFailedPutCounter = 0;
      this.totalSucceededPutCounter = 0;
      this.totalRetriedPutCounter = 0;
      this.maxLatency = 0;
      this.overallAverageLatency = 0;
      this.serverToBufferedCounterMap = new HashMap<String, Long>();
      this.serverToFailedCounterMap = new HashMap<String, Long>();
      this.serverToSucceededCounterMap = new HashMap<String, Long>();
      this.serverToRetriedCounterMap = new HashMap<String, Long>();
      this.serverToAverageLatencyMap = new HashMap<String, Long>();
      this.serverToMaxLatencyMap = new HashMap<String, Long>();
      this.serverToFlushWorkerMap = serverToFlushWorkerMap;
      this.tableNameToHTableMap = tableNameToHTableMap;
      this.metrics = new MultiPutBatchMetrics();
      this.initialize();
    }

    private void initialize() {
      if (serverToFlushWorkerMap == null) {
        return;
      }
      long averageCalcSum = 0;
      int averageCalcCount = 0;
      int avgMultiPutBatchSizeSum = 0;
      for (Map.Entry<HServerAddress, HTableFlushWorker> entry : serverToFlushWorkerMap
          .entrySet()) {
        HServerAddress addr = entry.getKey();
        HTableFlushWorker worker = entry.getValue();

        long bufferedCounter = worker.getTotalBufferedCount();
        long failedCounter = worker.getTotalFailedCount();
        long succeededCounter = worker.getTotalSucceededCount();
        long retriedCounter = worker.getTotalRetriedCount();
        long serverMaxLatency = worker.getAndResetMaxLatency();
        AtomicAverageCounter averageCounter = worker.getAverageLatencyCounter();
        // Get sum and count pieces separately to compute overall average
        SimpleEntry<Long, Integer> averageComponents =
          averageCounter.getComponents();
        long serverAvgLatency = (long) averageCounter.getAndReset();

        this.totalBufferedPutCounter += bufferedCounter;
        this.totalFailedPutCounter += failedCounter;
        this.totalSucceededPutCounter += succeededCounter;
        this.totalRetriedPutCounter += retriedCounter;
        if (serverMaxLatency > this.maxLatency) {
          this.maxLatency = serverMaxLatency;
        }
        averageCalcSum += averageComponents.getKey();
        averageCalcCount += averageComponents.getValue();

        String hostnameWithPort = addr.getHostNameWithPort();
        this.serverToBufferedCounterMap.put(hostnameWithPort, bufferedCounter);
        this.serverToFailedCounterMap.put(hostnameWithPort, failedCounter);
        this.serverToSucceededCounterMap.put(hostnameWithPort, succeededCounter);
        this.serverToRetriedCounterMap.put(hostnameWithPort, retriedCounter);
        this.serverToAverageLatencyMap.put(hostnameWithPort, serverAvgLatency);
        this.serverToMaxLatencyMap.put(hostnameWithPort, serverMaxLatency);
        this.metrics.resetMultiPutMetrics(hostnameWithPort, worker);
        avgMultiPutBatchSizeSum += this.metrics.getAvgMultiPutBatchSizeForRs(hostnameWithPort);
      }
      this.overallAverageLatency = averageCalcCount != 0
                                   ? averageCalcSum / averageCalcCount
                                   : 0;
      int flushMapSize = serverToFlushWorkerMap.size();
      this.overallAvgMultiPutSize = flushMapSize != 0
                                    ? avgMultiPutBatchSizeSum / flushMapSize
                                    : 0;
    }

    private void resetCounters() {
      this.totalBufferedPutCounter = 0;
      this.totalFailedPutCounter = 0;
      this.totalSucceededPutCounter = 0;
      this.totalRetriedPutCounter = 0;
      this.maxLatency = 0;
      this.serverToBufferedCounterMap.clear();
      this.serverToFailedCounterMap.clear();
      this.serverToSucceededCounterMap.clear();
      this.serverToAverageLatencyMap.clear();
      this.serverToMaxLatencyMap.clear();
      this.overallAverageLatency = 0;
      this.overallAvgMultiPutSize = 0;
    }

    /**
     * Recalculate all the counters, since the state of the HTM might have
     * changed since we last calculated them, if we are interested in the latest
     * values.
     */
    public void recalculateCounters() {
      resetCounters();
      initialize();
    }

    public long getTotalBufferedCounter() {
      return this.totalBufferedPutCounter;
    }

    public long getTotalFailedCounter() {
      return this.totalFailedPutCounter;
    }

    public long getTotalSucccededPutCounter() {
      return this.totalSucceededPutCounter;
    }

    public long getTotalRetriedPutCounter() {
      return this.totalRetriedPutCounter;
    }

    public long getMaxLatency() {
      return this.maxLatency;
    }

    public long getOverallAverageLatency() {
      return this.overallAverageLatency;
    }

    public long getOverallAvgMultiPutSize() {
      return overallAvgMultiPutSize;
    }

    public void resetMultiPutMetrics() {
      for (Entry<HServerAddress, HTableFlushWorker> entry : serverToFlushWorkerMap.entrySet()) {
        HServerAddress addr = entry.getKey();
        HTableFlushWorker worker = entry.getValue();
        metrics.resetMultiPutMetrics(addr.getHostNameWithPort(), worker);
      }
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

    public MultiPutBatchMetrics getMetrics() {
      return metrics;
    }

    public int getStoredHTableCount() {
      return this.tableNameToHTableMap.size();
    }
  }

  private static class PutStatus {
    private final HRegionInfo regionInfo;
    private final Put put;
    private final int maxRetryCount;
    private final HBaseRPCOptions options;

    public PutStatus(final HRegionInfo regionInfo, final Put put,
        final int maxRetryCount, final HBaseRPCOptions options) {
      this.regionInfo = regionInfo;
      this.put = put;
      this.maxRetryCount = maxRetryCount;
      this.options = options;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    public Put getPut() {
      return put;
    }

    /**
     * @deprecated Use {@link #getMaxRetryCount()} instead.
     * @return
     */
    @SuppressWarnings("unused")
    @Deprecated
    public int getRetryCount() {
      return getMaxRetryCount();
    }

    public int getMaxRetryCount() {
      return maxRetryCount;
    }

    public HBaseRPCOptions getOptions () {
      return options;
    }

  }

  /**
   * Helper to count the average over an interval until reset.
   */
  public static class AtomicAverageCounter {
    private long sum;
    private int count;

    public AtomicAverageCounter() {
      this.sum = 0L;
      this.count = 0;
    }

    public synchronized double getAndReset() {
      double result = this.get();
      this.reset();
      return result;
    }

    public synchronized double get() {
      if (this.count == 0) {
        return 0;
      }
      return this.sum / this.count;
    }

    public synchronized SimpleEntry<Long, Integer> getComponents() {
      return new SimpleEntry<Long, Integer>(sum, count);
    }

    public synchronized void reset() {
      this.sum = 0l;
      this.count = 0;
    }

    public synchronized void add(long value) {
      this.sum += value;
      this.count++;
    }
  }

  private class HTableFlushWorker implements Runnable {
    private final HServerAddress addr;
    private final LinkedBlockingQueue<PutStatus> queue;
    private final AtomicLong totalFailedPutCount;
    private final AtomicLong totalSucceededPutCount;
    private final AtomicLong totalRetriedPutCount;
    private final AtomicInteger currentProcessingPutCount;
    private final AtomicInteger minProcessingPutCount;
    private final AtomicInteger maxProcessingPutCount;
    private final AtomicAverageCounter avgProcessingPutCount;
    private final AtomicAverageCounter averageLatency;
    private final AtomicLong maxLatency;
    // how many retried puts we have inserted in the queue
    private final AtomicInteger retriedPutsInQueue;

    public HTableFlushWorker(HServerAddress addr, LinkedBlockingQueue<PutStatus> queue) {
      this.addr = addr;
      this.queue = queue;
      this.totalFailedPutCount = new AtomicLong(0);
      this.totalSucceededPutCount = new AtomicLong(0);
      this.totalRetriedPutCount = new AtomicLong(0);
      this.currentProcessingPutCount = new AtomicInteger(0);
      this.minProcessingPutCount = new AtomicInteger(0);
      this.maxProcessingPutCount = new AtomicInteger(0);
      this.avgProcessingPutCount = new AtomicAverageCounter();
      this.averageLatency = new AtomicAverageCounter();
      this.maxLatency = new AtomicLong(0);
      this.retriedPutsInQueue = new AtomicInteger(0);
    }

    public long getTotalFailedCount() {
      return totalFailedPutCount.get();
    }

    public long getTotalSucceededCount() {
      return totalSucceededPutCount.get();
    }

    public long getTotalRetriedCount() {
      return totalRetriedPutCount.get();
    }

    public long getTotalBufferedCount() {
      return queue.size() + currentProcessingPutCount.get();
    }

    public AtomicAverageCounter getAverageLatencyCounter() {
      return this.averageLatency;
    }

    /**
     * @deprecated Use {@link #getAndResetMaxLatency()} instead.
     * @return
     */
    @SuppressWarnings("unused")
    @Deprecated
    public long getMaxLatency() {
      return this.maxLatency.getAndSet(0);
    }

    public long getAndResetMaxLatency() {
      return this.maxLatency.getAndSet(0);
    }

    /**
     * Resubmit logic for failed put. If we have exhausted the retry count we
     * return false. Otherwise, we schedule over executor when the failed put is
     * going to resubmitted. Depending on how many times it has failed, the time
     * when it is going to be tried again will increase.
     */
    private boolean resubmitFailedPut(PutStatus failedPutStatus, HServerAddress oldLoc) throws IOException{
      final Put failedPut = failedPutStatus.getPut();
      // The currentPut is failed. So get the table name for the currentPut.
      final byte[] tableName = failedPutStatus.getRegionInfo().getTableDesc().getName();
      // Decrease the retry count
      final int retryCount = failedPutStatus.getMaxRetryCount() - 1;

      if (retryCount <= 0) {
        // Update the failed counter and no retry any more.
        return false;
      } else {
        final HBaseRPCOptions options = failedPutStatus.getOptions();
        // schedule the retry of the failed put
        int currPuts = retriedPutsInQueue.incrementAndGet();
        // only schedule the put if we are below limit of allowed scheduled-retriable puts
        if (currPuts <= HTableMultiplexer.this.retriedInQueueMax) {
          Runnable actuallyReinsert = new Runnable() {
            @Override
            public void run() {
              retriedPutsInQueue.decrementAndGet();
              try {
                HTableMultiplexer.this.put(tableName, failedPut, retryCount,
                    options);
              } catch (IOException e) {
                // Log all the exceptions and move on
                LOG.debug("Caught some exceptions " + e
                    + " when reinserting puts to region server "
                    + addr.getHostNameWithPort());
              }
            }
          };
          // Wait at most DEFAULT_HBASE_RPC_TIMEOUT
          long waitTimeMs = Math.max(conf.getInt(
              HConstants.HBASE_RPC_TIMEOUT_KEY,
              HConstants.DEFAULT_HBASE_RPC_TIMEOUT), (long) (frequency * Math
              .pow(2, HTableMultiplexer.this.retryNum - retryCount)));
          executor.schedule(actuallyReinsert,
              waitTimeMs,
              TimeUnit.MILLISECONDS);
          return true;
        } else {
          // limit for allowed scheduled-retriable puts is reached, put is failed.
          retriedPutsInQueue.decrementAndGet(); // decrement since we optimistically incremented
          return false;
        }
      }
    }

    public long getAndResetMaxMultiPutCount() {
      return this.maxProcessingPutCount.getAndSet(0);
    }

    public long getAndResetMinMultiPutCount() {
      return this.minProcessingPutCount.getAndSet(0);
    }

    public double getAndResetAvgMultiPutCount() {
      return this.avgProcessingPutCount.getAndReset();
    }

    @Override
    public void run() {
      long start = EnvironmentEdgeManager.currentTimeMillis();
      long elapsed = 0;
      List<PutStatus> processingList = new ArrayList<PutStatus>();
      int completelyFailed = 0;
      try {
        // drain all the queued puts into the tmp list
        queue.drainTo(processingList);
        currentProcessingPutCount.set(processingList.size());
        if (minProcessingPutCount.get() > currentProcessingPutCount.get()) {
          minProcessingPutCount.set(currentProcessingPutCount.get());
        } else if (maxProcessingPutCount.get() < currentProcessingPutCount
            .get()) {
          maxProcessingPutCount.set(currentProcessingPutCount.get());
        }
        avgProcessingPutCount.add(currentProcessingPutCount.get());
        if (processingList.size() > 0) {
          MultiPut mput = new MultiPut(this.addr);
          HBaseRPCOptions options = null;
          for (PutStatus putStatus : processingList) {
            // Update the MultiPut
            mput.add(putStatus.getRegionInfo().getRegionName(),
                putStatus.getPut());
            if (putStatus.getOptions() != null) {
              options = putStatus.getOptions();
            }
          }

          List<PutStatus> putsForResubmit;

          // Process this multiput request
          Map<String, HRegionFailureInfo> failureInfo = new HashMap<String, HRegionFailureInfo>();
          try {
            List<Put> failed = connection.processListOfMultiPut(Arrays.asList(mput),
                null, options, failureInfo);
            if (failed != null) {
              if (failed.size() == processingList.size()) {
                // All the puts for this region server are failed. Going to retry
                // it later
                putsForResubmit = processingList;
              } else {
                putsForResubmit = new ArrayList<>();
                Set<Put> failedPutSet = new HashSet<Put>(failed);
                for (PutStatus putStatus : processingList) {
                  if (failedPutSet.contains(putStatus.getPut())) {
                    putsForResubmit.add(putStatus);
                  }
                }
              }
            } else {
              putsForResubmit = Collections.emptyList();
            }
          } catch (PreemptiveFastFailException e) {
            // Client is not blocking on us. So, let us treat this
            // as a normal failure, and retry.
            putsForResubmit = processingList;
          }

          long putsToRetry = 0;
          for (PutStatus putStatus : putsForResubmit) {
            if (!resubmitFailedPut(putStatus, this.addr)) {
              completelyFailed++;
            }
          }
          putsToRetry = putsForResubmit.size() - completelyFailed;


          // Update the totalFailedCount
          this.totalFailedPutCount.addAndGet(completelyFailed);
          // Update the totalSucceededPutCount
          this.totalSucceededPutCount.addAndGet(processingList.size()
              - completelyFailed - putsToRetry);
          // Updated the total retried put counts.
          this.totalRetriedPutCount.addAndGet(putsToRetry);

          elapsed = EnvironmentEdgeManager.currentTimeMillis() - start;
          // Update latency counters
          averageLatency.add(elapsed);
          if (elapsed > maxLatency.get()) {
            maxLatency.set(elapsed);
          }

          // Log some basic info
          if (LOG.isDebugEnabled()) {
            LOG.debug("Processed " + currentProcessingPutCount
                + " put requests for " + addr.getHostNameWithPort() + " and "
                + completelyFailed + " failed" + ", latency for this send: "
                + elapsed);
          }

          // Reset the current processing put count
          currentProcessingPutCount.set(0);
        }
      } catch (Exception e) {
        // Log all the exceptions and move on
        LOG.debug("Caught some exceptions " + e
            + " when flushing puts to region server "
            + addr.getHostNameWithPort());
      }
    }

  }
}
