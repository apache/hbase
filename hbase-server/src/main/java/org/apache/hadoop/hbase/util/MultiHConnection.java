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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides ability to create multiple Connection instances and allows to process a batch of
 * actions using CHTable.doBatchWithCallback()
 */
@InterfaceAudience.Private
public class MultiHConnection {
  private static final Logger LOG = LoggerFactory.getLogger(MultiHConnection.class);
  private Connection[] connections;
  private final Object connectionsLock =  new Object();
  private final int noOfConnections;
  private ExecutorService batchPool;

  /**
   * Create multiple Connection instances and initialize a thread pool executor
   * @param conf configuration
   * @param noOfConnections total no of Connections to create
   * @throws IOException if IO failure occurs
   */
  public MultiHConnection(Configuration conf, int noOfConnections)
      throws IOException {
    this.noOfConnections = noOfConnections;
    synchronized (this.connectionsLock) {
      connections = new Connection[noOfConnections];
      for (int i = 0; i < noOfConnections; i++) {
        Connection conn = ConnectionFactory.createConnection(conf);
        connections[i] = conn;
      }
    }
    createBatchPool(conf);
  }

  /**
   * Close the open connections and shutdown the batchpool
   */
  public void close() {
    synchronized (connectionsLock) {
      if (connections != null) {
        for (Connection conn : connections) {
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
        connections = null;
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

  /**
   * Randomly pick a connection and process the batch of actions for a given table
   * @param actions the actions
   * @param tableName table name
   * @param results the results array
   * @param callback to run when results are in
   * @throws IOException If IO failure occurs
   */
  public <R> void processBatchCallback(List<? extends Row> actions, TableName tableName,
      Object[] results, Batch.Callback<R> callback) throws IOException {
    // Currently used by RegionStateStore
    HTable.doBatchWithCallback(actions, results, callback,
      connections[ThreadLocalRandom.current().nextInt(noOfConnections)], batchPool, tableName);
  }

  // Copied from ConnectionImplementation.getBatchPool()
  // We should get rid of this when Connection.processBatchCallback is un-deprecated and provides
  // an API to manage a batch pool
  private void createBatchPool(Configuration conf) {
    // Use the same config for keep alive as in ConnectionImplementation.getBatchPool();
    int maxThreads = conf.getInt("hbase.multihconnection.threads.max", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.multihconnection.threads.keepalivetime", 60);
    LinkedBlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<>(maxThreads
            * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
              HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue,
            Threads.newDaemonThreadFactory("MultiHConnection" + "-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    this.batchPool = tpe;
  }
  
}
