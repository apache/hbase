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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncProcessCallback;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

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
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HTableMultiplexer {
  private static final Log LOG = LogFactory.getLog(HTableMultiplexer.class.getName());
  private static int poolID = 0;
  
  static final String TABLE_MULTIPLEXER_FLUSH_FREQ_MS = "hbase.tablemultiplexer.flush.frequency.ms";

  /** The map between each region server to its corresponding buffer queue */
  private final Map<HRegionLocation, LinkedBlockingQueue<PutStatus>> serverToBufferQueueMap =
      new ConcurrentHashMap<HRegionLocation, LinkedBlockingQueue<PutStatus>>();

  /** The map between each region server to its flush worker */
  private final Map<HRegionLocation, HTableFlushWorker> serverToFlushWorkerMap =
      new ConcurrentHashMap<HRegionLocation, HTableFlushWorker>();

  private final Configuration conf;
  private final HConnection conn;
  private final ExecutorService pool;
  private final int retryNum;
  private int perRegionServerBufferQueueSize;
  private final int maxKeyValueSize;
  
  /**
   * @param conf The HBaseConfiguration
   * @param perRegionServerBufferQueueSize determines the max number of the buffered Put ops for
   *          each region server before dropping the request.
   */
  public HTableMultiplexer(Configuration conf, int perRegionServerBufferQueueSize)
      throws IOException {
    this.conf = conf;
    this.conn = HConnectionManager.createConnection(conf);
    this.pool = HTable.getDefaultExecutor(conf);
    this.retryNum = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.perRegionServerBufferQueueSize = perRegionServerBufferQueueSize;
    this.maxKeyValueSize = HTable.getMaxKeyValueSize(conf);
  }

  /**
   * The put request will be buffered by its corresponding buffer queue. Return false if the queue
   * is already full.
   * @param tableName
   * @param put
   * @return true if the request can be accepted by its corresponding buffer queue.
   * @throws IOException
   */
  public boolean put(TableName tableName, final Put put) throws IOException {
    return put(tableName, put, this.retryNum);
  }

  /**
   * The puts request will be buffered by their corresponding buffer queue. 
   * Return the list of puts which could not be queued.
   * @param tableName
   * @param puts
   * @return the list of puts which could not be queued
   * @throws IOException
   */
  public List<Put> put(TableName tableName, final List<Put> puts)
      throws IOException {
    if (puts == null)
      return null;
    
    List <Put> failedPuts = null;
    boolean result;
    for (Put put : puts) {
      result = put(tableName, put, this.retryNum);
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

  public List<Put> put(byte[] tableName, final List<Put> puts) throws IOException {
    return put(TableName.valueOf(tableName), puts);
  }


  /**
   * The put request will be buffered by its corresponding buffer queue. And the put request will be
   * retried before dropping the request.
   * Return false if the queue is already full.
   * @param tableName
   * @param put
   * @param retry
   * @return true if the request can be accepted by its corresponding buffer queue.
   * @throws IOException
   */
  public boolean put(final TableName tableName, final Put put, int retry)
      throws IOException {
    if (retry <= 0) {
      return false;
    }

    try {
      HTable.validatePut(put, maxKeyValueSize);
      HRegionLocation loc = conn.getRegionLocation(tableName, put.getRow(), false);
      if (loc != null) {
        // Add the put pair into its corresponding queue.

        LinkedBlockingQueue<PutStatus> queue = getQueue(loc);
        // Generate a MultiPutStatus object and offer it into the queue
        PutStatus s = new PutStatus(loc.getRegionInfo(), put, retry);
        
        return queue.offer(s);
      }
    } catch (Exception e) {
      LOG.debug("Cannot process the put " + put + " because of " + e);
    }
    return false;
  }

  public boolean put(final byte[] tableName, final Put put, int retry)
      throws IOException {
    return put(TableName.valueOf(tableName), put, retry);
  }

  /**
   * @return the current HTableMultiplexerStatus
   */
  public HTableMultiplexerStatus getHTableMultiplexerStatus() {
    return new HTableMultiplexerStatus(serverToFlushWorkerMap);
  }

  private LinkedBlockingQueue<PutStatus> getQueue(HRegionLocation addr) {
    LinkedBlockingQueue<PutStatus> queue = serverToBufferQueueMap.get(addr);
    if (queue == null) {
      synchronized (this.serverToBufferQueueMap) {
        queue = serverToBufferQueueMap.get(addr);
        if (queue == null) {
          // Create a queue for the new region server
          queue = new LinkedBlockingQueue<PutStatus>(perRegionServerBufferQueueSize);
          serverToBufferQueueMap.put(addr, queue);

          // Create the flush worker
          HTableFlushWorker worker =
              new HTableFlushWorker(conf, this.conn, addr, this, queue, pool);
          this.serverToFlushWorkerMap.put(addr, worker);

          // Launch a daemon thread to flush the puts
          // from the queue to its corresponding region server.
          String name = "HTableFlushWorker-" + addr.getHostnamePort() + "-" + (poolID++);
          Thread t = new Thread(worker, name);
          t.setDaemon(true);
          t.start();
        }
      }
    }
    return queue;
  }

  /**
   * HTableMultiplexerStatus keeps track of the current status of the HTableMultiplexer.
   * report the number of buffered requests and the number of the failed (dropped) requests
   * in total or on per region server basis.
   */
  static class HTableMultiplexerStatus {
    private long totalFailedPutCounter;
    private long totalBufferedPutCounter;
    private long maxLatency;
    private long overallAverageLatency;
    private Map<String, Long> serverToFailedCounterMap;
    private Map<String, Long> serverToBufferedCounterMap;
    private Map<String, Long> serverToAverageLatencyMap;
    private Map<String, Long> serverToMaxLatencyMap;

    public HTableMultiplexerStatus(
        Map<HRegionLocation, HTableFlushWorker> serverToFlushWorkerMap) {
      this.totalBufferedPutCounter = 0;
      this.totalFailedPutCounter = 0;
      this.maxLatency = 0;
      this.overallAverageLatency = 0;
      this.serverToBufferedCounterMap = new HashMap<String, Long>();
      this.serverToFailedCounterMap = new HashMap<String, Long>();
      this.serverToAverageLatencyMap = new HashMap<String, Long>();
      this.serverToMaxLatencyMap = new HashMap<String, Long>();
      this.initialize(serverToFlushWorkerMap);
    }

    private void initialize(
        Map<HRegionLocation, HTableFlushWorker> serverToFlushWorkerMap) {
      if (serverToFlushWorkerMap == null) {
        return;
      }

      long averageCalcSum = 0;
      int averageCalcCount = 0;
      for (Map.Entry<HRegionLocation, HTableFlushWorker> entry : serverToFlushWorkerMap
          .entrySet()) {
        HRegionLocation addr = entry.getKey();
        HTableFlushWorker worker = entry.getValue();

        long bufferedCounter = worker.getTotalBufferedCount();
        long failedCounter = worker.getTotalFailedCount();
        long serverMaxLatency = worker.getMaxLatency();
        AtomicAverageCounter averageCounter = worker.getAverageLatencyCounter();
        // Get sum and count pieces separately to compute overall average
        SimpleEntry<Long, Integer> averageComponents = averageCounter
            .getComponents();
        long serverAvgLatency = averageCounter.getAndReset();

        this.totalBufferedPutCounter += bufferedCounter;
        this.totalFailedPutCounter += failedCounter;
        if (serverMaxLatency > this.maxLatency) {
          this.maxLatency = serverMaxLatency;
        }
        averageCalcSum += averageComponents.getKey();
        averageCalcCount += averageComponents.getValue();

        this.serverToBufferedCounterMap.put(addr.getHostnamePort(),
            bufferedCounter);
        this.serverToFailedCounterMap
            .put(addr.getHostnamePort(),
            failedCounter);
        this.serverToAverageLatencyMap.put(addr.getHostnamePort(),
            serverAvgLatency);
        this.serverToMaxLatencyMap
            .put(addr.getHostnamePort(),
            serverMaxLatency);
      }
      this.overallAverageLatency = averageCalcCount != 0 ? averageCalcSum
          / averageCalcCount : 0;
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
  
  private static class PutStatus {
    private final HRegionInfo regionInfo;
    private final Put put;
    private final int retryCount;
    public PutStatus(final HRegionInfo regionInfo, final Put put,
        final int retryCount) {
      this.regionInfo = regionInfo;
      this.put = put;
      this.retryCount = retryCount;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }
    public Put getPut() {
      return put;
    }
    public int getRetryCount() {
      return retryCount;
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

  private static class HTableFlushWorker implements Runnable, AsyncProcessCallback<Object> {
    private final HRegionLocation addr;
    private final Configuration conf;
    private final LinkedBlockingQueue<PutStatus> queue;
    private final HTableMultiplexer htableMultiplexer;
    private final AtomicLong totalFailedPutCount = new AtomicLong(0);
    private final AtomicInteger currentProcessingPutCount = new AtomicInteger(0);
    private final AtomicAverageCounter averageLatency = new AtomicAverageCounter();
    private final AtomicLong maxLatency = new AtomicLong(0);
    private final AsyncProcess<Object> ap;
    private final List<Object> results = new ArrayList<Object>();
    
    public HTableFlushWorker(Configuration conf, HConnection conn, HRegionLocation addr,
        HTableMultiplexer htableMultiplexer, LinkedBlockingQueue<PutStatus> queue, ExecutorService pool) {
      this.addr = addr;
      this.conf = conf;
      this.htableMultiplexer = htableMultiplexer;
      this.queue = queue;
      RpcRetryingCallerFactory rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf);
      RpcControllerFactory rpcControllerFactory = RpcControllerFactory.instantiate(conf);
      this.ap = new AsyncProcess<Object>(conn, null, pool, this, conf, rpcCallerFactory,
              rpcControllerFactory);
    }

    public long getTotalFailedCount() {
      return totalFailedPutCount.get();
    }

    public long getTotalBufferedCount() {
      return queue.size() + currentProcessingPutCount.get();
    }

    public AtomicAverageCounter getAverageLatencyCounter() {
      return this.averageLatency;
    }

    public long getMaxLatency() {
      return this.maxLatency.getAndSet(0);
    }

    private boolean resubmitFailedPut(PutStatus failedPutStatus,
        HRegionLocation oldLoc) throws IOException {
      Put failedPut = failedPutStatus.getPut();
      // The currentPut is failed. So get the table name for the currentPut.
      TableName tableName = failedPutStatus.getRegionInfo().getTable();
      // Decrease the retry count
      int retryCount = failedPutStatus.getRetryCount() - 1;
      
      if (retryCount <= 0) {
        // Update the failed counter and no retry any more.
        return false;
      } else {
        // Retry one more time
        return this.htableMultiplexer.put(tableName, failedPut, retryCount);
      }
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings
        (value = "REC_CATCH_EXCEPTION", justification = "na")
    public void run() {
      List<PutStatus> processingList = new ArrayList<PutStatus>();
      /** 
       * The frequency in milliseconds for the current thread to process the corresponding  
       * buffer queue.  
       **/
      long frequency = conf.getLong(TABLE_MULTIPLEXER_FLUSH_FREQ_MS, 100);
      
      // initial delay
      try {
        Thread.sleep(frequency);
      } catch (InterruptedException e) {
      } // Ignore

      long start, elapsed;
      int failedCount = 0;
      while (true) {
        try {
          start = elapsed = EnvironmentEdgeManager.currentTimeMillis();

          // Clear the processingList, putToStatusMap and failedCount
          processingList.clear();
          failedCount = 0;
          
          // drain all the queued puts into the tmp list
          queue.drainTo(processingList);
          currentProcessingPutCount.set(processingList.size());

          if (processingList.size() > 0) {
            this.results.clear();
            List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(processingList.size());
            MultiAction<Row> actions = new MultiAction<Row>();
            for (int i = 0; i < processingList.size(); i++) {
              PutStatus putStatus = processingList.get(i);
              Action<Row> action = new Action<Row>(putStatus.getPut(), i);
              actions.add(putStatus.getRegionInfo().getRegionName(), action);
              retainedActions.add(action);
              this.results.add(null);
            }
            
            // Process this multi-put request
            List<PutStatus> failed = null;
            Map<HRegionLocation, MultiAction<Row>> actionsByServer =
                Collections.singletonMap(addr, actions);
            try {
              HConnectionManager.ServerErrorTracker errorsByServer =
                  new HConnectionManager.ServerErrorTracker(1, 10);
              ap.sendMultiAction(retainedActions, actionsByServer, 10, errorsByServer);
              ap.waitUntilDone();

              if (ap.hasError()) {
                throw ap.getErrors();
              }
            } catch (IOException e) {
              LOG.debug("Caught some exceptions " + e
                  + " when flushing puts to region server " + addr.getHostnamePort());
            } finally {
              // mutate list so that it is empty for complete success, or
              // contains only failed records
              // results are returned in the same order as the requests in list
              // walk the list backwards, so we can remove from list without
              // impacting the indexes of earlier members
              for (int i = 0; i < results.size(); i++) {
                if (results.get(i) == null) {
                  if (failed == null) {
                    failed = new ArrayList<PutStatus>();
                  }
                  failed.add(processingList.get(i));
                }
              }
            }

            if (failed != null) {
              // Resubmit failed puts
              for (PutStatus putStatus : processingList) {
                if (!resubmitFailedPut(putStatus, this.addr)) {
                  failedCount++;
                }
              }
              // Update the totalFailedCount
              this.totalFailedPutCount.addAndGet(failedCount);
            }
            
            elapsed = EnvironmentEdgeManager.currentTimeMillis() - start;
            // Update latency counters
            averageLatency.add(elapsed);
            if (elapsed > maxLatency.get()) {
              maxLatency.set(elapsed);
            }
            
            // Log some basic info
            if (LOG.isDebugEnabled()) {
              LOG.debug("Processed " + currentProcessingPutCount
                  + " put requests for " + addr.getHostnamePort() + " and "
                  + failedCount + " failed" + ", latency for this send: "
                  + elapsed);
            }

            // Reset the current processing put count
            currentProcessingPutCount.set(0);
          }

          // Sleep for a while
          if (elapsed == start) {
            elapsed = EnvironmentEdgeManager.currentTimeMillis() - start;
          }
          if (elapsed < frequency) {
            Thread.sleep(frequency - elapsed);
          }
        } catch (Exception e) {
          // Log all the exceptions and move on
          LOG.debug("Caught some exceptions " + e
              + " when flushing puts to region server "
                + addr.getHostnamePort(), e);
        }
      }
    }

    @Override
    public void success(int originalIndex, byte[] region, Row row, Object result) {
      if (results == null || originalIndex >= results.size()) {
        return;
      }
      results.set(originalIndex, result);
    }

    @Override
    public boolean failure(int originalIndex, byte[] region, Row row, Throwable t) {
      return false;
    }

    @Override
    public boolean retriableFailure(int originalIndex, Row row, byte[] region, Throwable exception) {
      return false;
    }
  }
}
