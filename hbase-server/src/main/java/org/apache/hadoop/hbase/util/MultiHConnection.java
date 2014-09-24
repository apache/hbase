/**
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

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

/**
 * Provides ability to create multiple HConnection instances and allows to process a batch of
 * actions using HConnection.processBatchCallback()
 */
@InterfaceAudience.Private
public class MultiHConnection {
  private static final Log LOG = LogFactory.getLog(MultiHConnection.class);
  private HConnection[] hConnections;
  private int noOfConnections;
  private ExecutorService batchPool;

  /**
   * Create multiple HConnection instances and initialize a thread pool executor
   * @param conf configuration
   * @param noOfConnections total no of HConnections to create
   * @throws IOException
   */
  public MultiHConnection(Configuration conf, int noOfConnections)
      throws IOException {
    this.noOfConnections = noOfConnections;
    hConnections = new HConnection[noOfConnections];
    for (int i = 0; i < noOfConnections; i++) {
      HConnection conn = HConnectionManager.createConnection(conf);
      hConnections[i] = conn;
    }
    createBatchPool(conf);
  }

  /**
   * Close the open connections and shutdown the batchpool
   */
  public void close() {
    if (hConnections != null) {
      synchronized (hConnections) {
        if (hConnections != null) {
          for (HConnection conn : hConnections) {
            if (conn != null) {
              try {
                conn.close();
              } catch (IOException e) {
                LOG.info("Got exception in closing connection", e);
              } finally {
                conn = null;
              }
            }
          }
          hConnections = null;
        }
      }
    }
    if (this.batchPool != null && !this.batchPool.isShutdown()) {
      this.batchPool.shutdown();
      try {
        if (!this.batchPool.awaitTermination(10, TimeUnit.SECONDS)) {
          this.batchPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        this.batchPool.shutdownNow();
      }
    }

  }

  private static ThreadLocal<Random> threadLocalRandom = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  /**
   * Randomly pick a connection and process the batch of actions for a given table
   * @param actions the actions
   * @param tableName table name
   * @param results the results array
   * @param callback 
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("deprecation")
  public <R> void processBatchCallback(List<? extends Row> actions, TableName tableName,
      Object[] results, Batch.Callback<R> callback) throws IOException {
    // Currently used by RegionStateStore
    // A deprecated method is used as multiple threads accessing RegionStateStore do a single put
    // and htable is not thread safe. Alternative would be to create an Htable instance for each 
    // put but that is not very efficient.
    // See HBASE-11610 for more details.
    try {
      hConnections[threadLocalRandom.get().nextInt(noOfConnections)].processBatchCallback(
        actions, tableName, this.batchPool, results, callback);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
  }

  
  // Copied from HConnectionImplementation.getBatchPool()
  // We should get rid of this when HConnection.processBatchCallback is un-deprecated and provides
  // an API to manage a batch pool
  private void createBatchPool(Configuration conf) {
    // Use the same config for keep alive as in HConnectionImplementation.getBatchPool();
    int maxThreads = conf.getInt("hbase.multihconnection.threads.max", 256);
    int coreThreads = conf.getInt("hbase.multihconnection.threads.core", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.multihconnection.threads.keepalivetime", 60);
    LinkedBlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<Runnable>(maxThreads
            * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
              HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue,
            Threads.newDaemonThreadFactory("MultiHConnection" + "-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    this.batchPool = tpe;
  }
  
}
