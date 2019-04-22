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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * Abstract class that provides an interface to the Replication Table. Which is currently
 * being used for WAL offset tracking.
 * The basic schema of this table will store each individual queue as a
 * seperate row. The row key will be a unique identifier of the creating server's name and the
 * queueId. Each queue must have the following two columns:
 *  COL_QUEUE_OWNER: tracks which server is currently responsible for tracking the queue
 *  COL_QUEUE_OWNER_HISTORY: a "|" delimited list of the previous server's that have owned this
 *    queue. The most recent previous owner is the leftmost entry.
 * They will also have columns mapping [WAL filename : offset]
 * The most flexible method of interacting with the Replication Table is by calling
 * getOrBlockOnReplicationTable() which will return a new copy of the Replication Table. It is up
 * to the caller to close the returned table.
 */
@InterfaceAudience.Private
abstract class ReplicationTableBase {

  /** Name of the HBase Table used for tracking replication*/
  public static final TableName REPLICATION_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Column family and column names for Queues in the Replication Table
  public static final byte[] CF_QUEUE = Bytes.toBytes("q");
  public static final byte[] COL_QUEUE_OWNER = Bytes.toBytes("o");
  public static final byte[] COL_QUEUE_OWNER_HISTORY = Bytes.toBytes("h");

  // Column Descriptor for the Replication Table
  private static final HColumnDescriptor REPLICATION_COL_DESCRIPTOR =
    new HColumnDescriptor(CF_QUEUE).setMaxVersions(1)
      .setInMemory(true)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        // TODO: Figure out which bloom filter to use
      .setBloomFilterType(BloomType.NONE);

  // The value used to delimit the queueId and server name inside of a queue's row key. Currently a
  // hyphen, because it is guaranteed that queueId (which is a cluster id) cannot contain hyphens.
  // See HBASE-11394.
  public static final String ROW_KEY_DELIMITER = "-";

  // The value used to delimit server names in the queue history list
  public static final String QUEUE_HISTORY_DELIMITER = "|";

  /*
  * Make sure that HBase table operations for replication have a high number of retries. This is
  * because the server is aborted if any HBase table operation fails. Each RPC will be attempted
  * 3600 times before exiting. This provides each operation with 2 hours of retries
  * before the server is aborted.
  */
  private static final int CLIENT_RETRIES = 3600;
  private static final int RPC_TIMEOUT = 2000;
  private static final int OPERATION_TIMEOUT = CLIENT_RETRIES * RPC_TIMEOUT;

  // We only need a single thread to initialize the Replication Table
  private static final int NUM_INITIALIZE_WORKERS = 1;

  protected final Configuration conf;
  protected final Abortable abortable;
  private final Connection connection;
  private final Executor executor;
  private volatile CountDownLatch replicationTableInitialized;

  public ReplicationTableBase(Configuration conf, Abortable abort) throws IOException {
    this.conf = new Configuration(conf);
    this.abortable = abort;
    decorateConf();
    this.connection = ConnectionFactory.createConnection(this.conf);
    this.executor = setUpExecutor();
    this.replicationTableInitialized = new CountDownLatch(1);
    createReplicationTableInBackground();
  }

  /**
   * Modify the connection's config so that operations run on the Replication Table have longer and
   * a larger number of retries
   */
  private void decorateConf() {
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES);
  }

  /**
   * Sets up the thread pool executor used to build the Replication Table in the background
   * @return the configured executor
   */
  private Executor setUpExecutor() {
    ThreadPoolExecutor tempExecutor = new ThreadPoolExecutor(NUM_INITIALIZE_WORKERS,
        NUM_INITIALIZE_WORKERS, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationTableExecutor-%d");
    tfb.setDaemon(true);
    tempExecutor.setThreadFactory(tfb.build());
    return tempExecutor;
  }

  /**
   * Get whether the Replication Table has been successfully initialized yet
   * @return whether the Replication Table is initialized
   */
  public boolean getInitializationStatus() {
    return replicationTableInitialized.getCount() == 0;
  }

  /**
   * Increases the RPC and operations timeouts for the Replication Table
   */
  private Table setReplicationTableTimeOuts(Table replicationTable) {
    replicationTable.setRpcTimeout(RPC_TIMEOUT);
    replicationTable.setOperationTimeout(OPERATION_TIMEOUT);
    return replicationTable;
  }

  /**
   * Build the row key for the given queueId. This will uniquely identify it from all other queues
   * in the cluster.
   * @param serverName The owner of the queue
   * @param queueId String identifier of the queue
   * @return String representation of the queue's row key
   */
  protected String buildQueueRowKey(String serverName, String queueId) {
    return queueId + ROW_KEY_DELIMITER + serverName;
  }

  /**
   * Parse the original queueId from a row key
   * @param rowKey String representation of a queue's row key
   * @return the original queueId
   */
  protected String getRawQueueIdFromRowKey(String rowKey) {
    return rowKey.split(ROW_KEY_DELIMITER)[0];
  }

  /**
   * Returns a queue's row key given either its raw or reclaimed queueId
   *
   * @param queueId queueId of the queue
   * @return byte representation of the queue's row key
   */
  protected byte[] queueIdToRowKey(String serverName, String queueId) {
    // Cluster id's are guaranteed to have no hyphens, so if the passed in queueId has no hyphen
    // then this is not a reclaimed queue.
    if (!queueId.contains(ROW_KEY_DELIMITER)) {
      return Bytes.toBytes(buildQueueRowKey(serverName, queueId));
      // If the queueId contained some hyphen it was reclaimed. In this case, the queueId is the
      // queue's row key
    } else {
      return Bytes.toBytes(queueId);
    }
  }

  /**
   * Creates a "|" delimited record of the queue's past region server owners.
   *
   * @param originalHistory the queue's original owner history
   * @param oldServer the name of the server that used to own the queue
   * @return the queue's new owner history
   */
  protected String buildClaimedQueueHistory(String originalHistory, String oldServer) {
    return oldServer + QUEUE_HISTORY_DELIMITER + originalHistory;
  }

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   */
  protected List<String> getListOfReplicators() {
    // scan all of the queues and return a list of all unique OWNER values
    Set<String> peerServers = new HashSet<>();
    ResultScanner allQueuesInCluster = null;
    try (Table replicationTable = getOrBlockOnReplicationTable()){
      Scan scan = new Scan();
      scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
      allQueuesInCluster = replicationTable.getScanner(scan);
      for (Result queue : allQueuesInCluster) {
        peerServers.add(Bytes.toString(queue.getValue(CF_QUEUE, COL_QUEUE_OWNER)));
      }
    } catch (IOException e) {
      String errMsg = "Failed getting list of replicators";
      abortable.abort(errMsg, e);
    } finally {
      if (allQueuesInCluster != null) {
        allQueuesInCluster.close();
      }
    }
    return new ArrayList<>(peerServers);
  }

  protected List<String> getAllQueues(String serverName) {
    List<String> allQueues = new ArrayList<>();
    ResultScanner queueScanner = null;
    try {
      queueScanner = getQueuesBelongingToServer(serverName);
      for (Result queue : queueScanner) {
        String rowKey =  Bytes.toString(queue.getRow());
        // If the queue does not have a Owner History, then we must be its original owner. So we
        // want to return its queueId in raw form
        if (Bytes.toString(queue.getValue(CF_QUEUE, COL_QUEUE_OWNER_HISTORY)).length() == 0) {
          allQueues.add(getRawQueueIdFromRowKey(rowKey));
        } else {
          allQueues.add(rowKey);
        }
      }
      return allQueues;
    } catch (IOException e) {
      String errMsg = "Failed getting list of all replication queues for serverName=" + serverName;
      abortable.abort(errMsg, e);
      return null;
    } finally {
      if (queueScanner != null) {
        queueScanner.close();
      }
    }
  }

  protected List<String> getLogsInQueue(String serverName, String queueId) {
    String rowKey = queueId;
    if (!queueId.contains(ROW_KEY_DELIMITER)) {
      rowKey = buildQueueRowKey(serverName, queueId);
    }
    return getLogsInQueue(Bytes.toBytes(rowKey));
  }

  protected List<String> getLogsInQueue(byte[] rowKey) {
    String errMsg = "Failed getting logs in queue queueId=" + Bytes.toString(rowKey);
    try (Table replicationTable = getOrBlockOnReplicationTable()) {
      Get getQueue = new Get(rowKey);
      Result queue = replicationTable.get(getQueue);
      if (queue == null || queue.isEmpty()) {
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return null;
      }
      return readWALsFromResult(queue);
    } catch (IOException e) {
      abortable.abort(errMsg, e);
      return null;
    }
  }

  /**
   * Read all of the WAL's from a queue into a list
   *
   * @param queue HBase query result containing the queue
   * @return a list of all the WAL filenames
   */
  protected List<String> readWALsFromResult(Result queue) {
    List<String> wals = new ArrayList<>();
    Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF_QUEUE);
    for (byte[] cQualifier : familyMap.keySet()) {
      // Ignore the meta data fields of the queue
      if (Arrays.equals(cQualifier, COL_QUEUE_OWNER) || Arrays.equals(cQualifier,
          COL_QUEUE_OWNER_HISTORY)) {
        continue;
      }
      wals.add(Bytes.toString(cQualifier));
    }
    return wals;
  }

  /**
   * Get the queue id's and meta data (Owner and History) for the queues belonging to the named
   * server
   *
   * @param server name of the server
   * @return a ResultScanner over the QueueIds belonging to the server
   * @throws IOException
   */
  protected ResultScanner getQueuesBelongingToServer(String server) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF_QUEUE, COL_QUEUE_OWNER,
    CompareOperator.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER_HISTORY);
    try (Table replicationTable = getOrBlockOnReplicationTable()) {
      ResultScanner results = replicationTable.getScanner(scan);
      return results;
    }
  }

  /**
   * Attempts to acquire the Replication Table. This operation will block until it is assigned by
   * the CreateReplicationWorker thread. It is up to the caller of this method to close the
   * returned Table
   * @return the Replication Table when it is created
   * @throws IOException
   */
  protected Table getOrBlockOnReplicationTable() throws IOException {
    // Sleep until the Replication Table becomes available
    try {
      replicationTableInitialized.await();
    } catch (InterruptedException e) {
      String errMsg = "Unable to acquire the Replication Table due to InterruptedException: " +
          e.getMessage();
      throw new InterruptedIOException(errMsg);
    }
    return getAndSetUpReplicationTable();
  }

  /**
   * Creates a new copy of the Replication Table and sets up the proper Table time outs for it
   *
   * @return the Replication Table
   * @throws IOException
   */
  private Table getAndSetUpReplicationTable() throws IOException {
    Table replicationTable = connection.getTable(REPLICATION_TABLE_NAME);
    setReplicationTableTimeOuts(replicationTable);
    return replicationTable;
  }

  /**
   * Builds the Replication Table in a background thread. Any method accessing the Replication Table
   * should do so through getOrBlockOnReplicationTable()
   *
   * @return the Replication Table
   * @throws IOException if the Replication Table takes too long to build
   */
  private void createReplicationTableInBackground() throws IOException {
    executor.execute(new CreateReplicationTableWorker());
  }

  /**
   * Attempts to build the Replication Table. Will continue blocking until we have a valid
   * Table for the Replication Table.
   */
  private class CreateReplicationTableWorker implements Runnable {

    private Admin admin;

    @Override
    public void run() {
      try {
        admin = connection.getAdmin();
        if (!replicationTableExists()) {
          createReplicationTable();
        }
        int maxRetries = conf.getInt("hbase.replication.queues.createtable.retries.number",
            CLIENT_RETRIES);
        RetryCounterFactory counterFactory = new RetryCounterFactory(maxRetries, RPC_TIMEOUT);
        RetryCounter retryCounter = counterFactory.create();
        while (!replicationTableExists()) {
          retryCounter.sleepUntilNextRetry();
          if (!retryCounter.shouldRetry()) {
            throw new IOException("Unable to acquire the Replication Table");
          }
        }
        replicationTableInitialized.countDown();
      } catch (IOException | InterruptedException e) {
        abortable.abort("Failed building Replication Table", e);
      }
    }

    /**
     * Create the replication table with the provided HColumnDescriptor REPLICATION_COL_DESCRIPTOR
     * in TableBasedReplicationQueuesImpl
     *
     * @throws IOException
     */
    private void createReplicationTable() throws IOException {
      HTableDescriptor replicationTableDescriptor = new HTableDescriptor(REPLICATION_TABLE_NAME);
      replicationTableDescriptor.addFamily(REPLICATION_COL_DESCRIPTOR);
      try {
        admin.createTable(replicationTableDescriptor);
      } catch (TableExistsException e) {
        // In this case we can just continue as normal
      }
    }

    /**
     * Checks whether the Replication Table exists yet
     *
     * @return whether the Replication Table exists
     * @throws IOException
     */
    private boolean replicationTableExists() {
      try {
        return admin.tableExists(REPLICATION_TABLE_NAME);
      } catch (IOException e) {
        return false;
      }
    }
  }
}
