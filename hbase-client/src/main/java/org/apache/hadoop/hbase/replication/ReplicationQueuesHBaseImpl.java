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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides an implementation of the ReplicationQueues interface using an HBase table
 * "Replication Table". The basic schema of this table will store each individual queue as a
 * seperate row. The row key will be a unique identifier of the creating server's name and the
 * queueId. Each queue must have the following two columns:
 *  COL_OWNER: tracks which server is currently responsible for tracking the queue
 *  COL_QUEUE_ID: tracks the queue's id as stored in ReplicationSource
 * They will also have columns mapping [WAL filename : offset]
 * One key difference from the ReplicationQueuesZkImpl is that when queues are reclaimed we
 * simply return its HBase row key as its new "queueId"
 */

@InterfaceAudience.Private
public class ReplicationQueuesHBaseImpl extends ReplicationStateZKBase
    implements ReplicationQueues {

  private static final Log LOG = LogFactory.getLog(ReplicationQueuesHBaseImpl.class);

  /** Name of the HBase Table used for tracking replication*/
  public static final TableName REPLICATION_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Column family and column names for the Replication Table
  private static final byte[] CF = Bytes.toBytes("r");
  private static final byte[] COL_OWNER = Bytes.toBytes("o");
  private static final byte[] COL_OWNER_HISTORY = Bytes.toBytes("h");

  // The value used to delimit the queueId and server name inside of a queue's row key. Currently a
  // hyphen, because it is guaranteed that queueId (which is a cluster id) cannot contain hyphens.
  // See HBASE-11394.
  private static String ROW_KEY_DELIMITER = "-";

  // Column Descriptor for the Replication Table
  private static final HColumnDescriptor REPLICATION_COL_DESCRIPTOR =
    new HColumnDescriptor(CF).setMaxVersions(1)
      .setInMemory(true)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        // TODO: Figure out which bloom filter to use
      .setBloomFilterType(BloomType.NONE)
      .setCacheDataInL1(true);

  // Common byte values used in replication offset tracking
  private static final byte[] INITIAL_OFFSET_BYTES = Bytes.toBytes(0L);
  private static final byte[] EMPTY_STRING_BYTES = Bytes.toBytes("");

  /*
   * Make sure that HBase table operations for replication have a high number of retries. This is
   * because the server is aborted if any HBase table operation fails. Each RPC will be attempted
   * 3600 times before exiting. This provides each operation with 2 hours of retries
   * before the server is aborted.
   */
  private static final int CLIENT_RETRIES = 3600;
  private static final int RPC_TIMEOUT = 2000;
  private static final int OPERATION_TIMEOUT = CLIENT_RETRIES * RPC_TIMEOUT;

  private Configuration modifiedConf;
  private Admin admin;
  private Connection connection;
  private Table replicationTable;
  private String serverName = null;
  private byte[] serverNameBytes = null;

  public ReplicationQueuesHBaseImpl(ReplicationQueuesArguments args) {
    this(args.getConf(), args.getAbortable(), args.getZk());
  }

  public ReplicationQueuesHBaseImpl(Configuration conf, Abortable abort, ZooKeeperWatcher zkw) {
    super(zkw, conf, abort);
    modifiedConf = new Configuration(conf);
    modifiedConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES);
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    try {
      this.serverName = serverName;
      this.serverNameBytes = Bytes.toBytes(serverName);
      // Modify the connection's config so that the Replication Table it returns has a much higher
      // number of client retries
      this.connection = ConnectionFactory.createConnection(modifiedConf);
      this.admin = connection.getAdmin();
      replicationTable = createAndGetReplicationTable();
      replicationTable.setRpcTimeout(RPC_TIMEOUT);
      replicationTable.setOperationTimeout(OPERATION_TIMEOUT);
    } catch (IOException e) {
      throw new ReplicationException(e);
    }
  }

  @Override
  public void removeQueue(String queueId) {

    try {
      byte[] rowKey = queueIdToRowKey(queueId);
      Delete deleteQueue = new Delete(rowKey);
      safeQueueUpdate(deleteQueue);
    } catch (IOException | ReplicationException e) {
      String errMsg = "Failed removing queue queueId=" + queueId;
      abortable.abort(errMsg, e);
    }
  }

  @Override
  public void addLog(String queueId, String filename) throws ReplicationException {
    try {
      if (!checkQueueExists(queueId)) {
        // Each queue will have an Owner, OwnerHistory, and a collection of [WAL:offset] key values
        Put putNewQueue = new Put(Bytes.toBytes(buildQueueRowKey(queueId)));
        putNewQueue.addColumn(CF, COL_OWNER, serverNameBytes);
        putNewQueue.addColumn(CF, COL_OWNER_HISTORY, EMPTY_STRING_BYTES);
        putNewQueue.addColumn(CF, Bytes.toBytes(filename), INITIAL_OFFSET_BYTES);
        replicationTable.put(putNewQueue);
      } else {
        // Otherwise simply add the new log and offset as a new column
        Put putNewLog = new Put(queueIdToRowKey(queueId));
        putNewLog.addColumn(CF, Bytes.toBytes(filename), INITIAL_OFFSET_BYTES);
        safeQueueUpdate(putNewLog);
      }
    } catch (IOException | ReplicationException e) {
      String errMsg = "Failed adding log queueId=" + queueId + " filename=" + filename;
      abortable.abort(errMsg, e);
    }
  }

  @Override
  public void removeLog(String queueId, String filename) {
    try {
      byte[] rowKey = queueIdToRowKey(queueId);
      Delete delete = new Delete(rowKey);
      delete.addColumns(CF, Bytes.toBytes(filename));
      safeQueueUpdate(delete);
    } catch (IOException | ReplicationException e) {
      String errMsg = "Failed removing log queueId=" + queueId + " filename=" + filename;
      abortable.abort(errMsg, e);
    }
  }

  @Override
  public void setLogPosition(String queueId, String filename, long position) {
    try {
      byte[] rowKey = queueIdToRowKey(queueId);
      // Check that the log exists. addLog() must have been called before setLogPosition().
      Get checkLogExists = new Get(rowKey);
      checkLogExists.addColumn(CF, Bytes.toBytes(filename));
      if (!replicationTable.exists(checkLogExists)) {
        String errMsg = "Could not set position of non-existent log from queueId=" + queueId +
          ", filename=" + filename;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      // Update the log offset if it exists
      Put walAndOffset = new Put(rowKey);
      walAndOffset.addColumn(CF, Bytes.toBytes(filename), Bytes.toBytes(position));
      safeQueueUpdate(walAndOffset);
    } catch (IOException | ReplicationException e) {
      String errMsg = "Failed writing log position queueId=" + queueId + "filename=" +
        filename + " position=" + position;
      abortable.abort(errMsg, e);
    }
  }

  @Override
  public long getLogPosition(String queueId, String filename) throws ReplicationException {
    try {
      byte[] rowKey = queueIdToRowKey(queueId);
      Get getOffset = new Get(rowKey);
      getOffset.addColumn(CF, Bytes.toBytes(filename));
      Result result = getResultIfOwner(getOffset);
      if (result == null || !result.containsColumn(CF, Bytes.toBytes(filename))) {
        throw new ReplicationException("Could not read empty result while getting log position " +
            "queueId=" + queueId + ", filename=" + filename);
      }
      return Bytes.toLong(result.getValue(CF, Bytes.toBytes(filename)));
    } catch (IOException e) {
      throw new ReplicationException("Could not get position in log for queueId=" + queueId +
          ", filename=" + filename);
    }
  }

  @Override
  public void removeAllQueues() {
    List<String> myQueueIds = getAllQueues();
    for (String queueId : myQueueIds) {
      removeQueue(queueId);
    }
  }

  @Override
  public List<String> getLogsInQueue(String queueId) {
    byte[] rowKey = queueIdToRowKey(queueId);
    return getLogsInQueue(rowKey);
  }

  private List<String> getLogsInQueue(byte[] rowKey) {
    String errMsg = "Could not get logs in queue queueId=" + Bytes.toString(rowKey);
    try {
      Get getQueue = new Get(rowKey);
      Result queue = getResultIfOwner(getQueue);
      // The returned queue could be null if we have lost ownership of it
      if (queue == null) {
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return null;
      }
      return readWALsFromResult(queue);
    } catch (IOException e) {
      abortable.abort(errMsg, e);
      return null;
    }
  }

  @Override
  public List<String> getAllQueues() {
    List<String> allQueues = new ArrayList<String>();
    ResultScanner queueScanner = null;
    try {
      queueScanner = this.getQueuesBelongingToServer(serverName);
      for (Result queue : queueScanner) {
        String rowKey =  Bytes.toString(queue.getRow());
        // If the queue does not have a Owner History, then we must be its original owner. So we
        // want to return its queueId in raw form
        if (Bytes.toString(queue.getValue(CF, COL_OWNER_HISTORY)).length() == 0) {
          allQueues.add(getRawQueueIdFromRowKey(rowKey));
        } else {
          allQueues.add(rowKey);
        }
      }
      return allQueues;
    } catch (IOException e) {
      String errMsg = "Failed getting list of all replication queues";
      abortable.abort(errMsg, e);
      return null;
    } finally {
      if (queueScanner != null) {
        queueScanner.close();
      }
    }
  }

  @Override
  public Map<String, Set<String>> claimQueues(String regionserver) {
    Map<String, Set<String>> queues = new HashMap<>();
    if (isThisOurRegionServer(regionserver)) {
      return queues;
    }
    ResultScanner queuesToClaim = null;
    try {
      queuesToClaim = this.getQueuesBelongingToServer(regionserver);
      for (Result queue : queuesToClaim) {
        if (attemptToClaimQueue(queue, regionserver)) {
          String rowKey = Bytes.toString(queue.getRow());
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(rowKey);
          if (peerExists(replicationQueueInfo.getPeerId())) {
            Set<String> sortedLogs = new HashSet<String>();
            List<String> logs = getLogsInQueue(queue.getRow());
            for (String log : logs) {
              sortedLogs.add(log);
            }
            queues.put(rowKey, sortedLogs);
            LOG.info(serverName + " has claimed queue " + rowKey + " from " + regionserver);
          } else {
            // Delete orphaned queues
            removeQueue(Bytes.toString(queue.getRow()));
            LOG.info(serverName + " has deleted abandoned queue " + rowKey + " from " +
                regionserver);
          }
        }
      }
    } catch (IOException | KeeperException e) {
      String errMsg = "Failed claiming queues for regionserver=" + regionserver;
      abortable.abort(errMsg, e);
      queues.clear();
    } finally {
      if (queuesToClaim != null) {
        queuesToClaim.close();
      }
    }
    return queues;
  }

  @Override
  public List<String> getListOfReplicators() {
    // scan all of the queues and return a list of all unique OWNER values
    Set<String> peerServers = new HashSet<String>();
    ResultScanner allQueuesInCluster = null;
    try {
      Scan scan = new Scan();
      scan.addColumn(CF, COL_OWNER);
      allQueuesInCluster = replicationTable.getScanner(scan);
      for (Result queue : allQueuesInCluster) {
        peerServers.add(Bytes.toString(queue.getValue(CF, COL_OWNER)));
      }
    } catch (IOException e) {
      String errMsg = "Failed getting list of replicators";
      abortable.abort(errMsg, e);
    } finally {
      if (allQueuesInCluster != null) {
        allQueuesInCluster.close();
      }
    }
    return new ArrayList<String>(peerServers);
  }

  @Override
  public boolean isThisOurRegionServer(String regionserver) {
    return this.serverName.equals(regionserver);
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void addHFileRefs(String peerId, List<String> files) throws ReplicationException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) {
    // TODO
    throw new NotImplementedException();
  }

  /**
   * Gets the Replication Table. Builds and blocks until the table is available if the Replication
   * Table does not exist.
   *
   * @return the Replication Table
   * @throws IOException if the Replication Table takes too long to build
   */
  private Table createAndGetReplicationTable() throws IOException {
    if (!replicationTableExists()) {
      createReplicationTable();
    }
    int maxRetries = conf.getInt("replication.queues.createtable.retries.number", 100);
    RetryCounterFactory counterFactory = new RetryCounterFactory(maxRetries, 100);
    RetryCounter retryCounter = counterFactory.create();
    while (!replicationTableExists()) {
      try {
        retryCounter.sleepUntilNextRetry();
        if (!retryCounter.shouldRetry()) {
          throw new IOException("Unable to acquire the Replication Table");
        }
      } catch (InterruptedException e) {
        return null;
      }
    }
    return connection.getTable(REPLICATION_TABLE_NAME);
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

  /**
   * Create the replication table with the provided HColumnDescriptor REPLICATION_COL_DESCRIPTOR
   * in ReplicationQueuesHBaseImpl
   *
   * @throws IOException
   */
  private void createReplicationTable() throws IOException {
    HTableDescriptor replicationTableDescriptor = new HTableDescriptor(REPLICATION_TABLE_NAME);
    replicationTableDescriptor.addFamily(REPLICATION_COL_DESCRIPTOR);
    admin.createTable(replicationTableDescriptor);
  }

  /**
   * Build the row key for the given queueId. This will uniquely identify it from all other queues
   * in the cluster.
   * @param serverName The owner of the queue
   * @param queueId String identifier of the queue
   * @return String representation of the queue's row key
   */
  private String buildQueueRowKey(String serverName, String queueId) {
    return queueId + ROW_KEY_DELIMITER + serverName;
  }

  private String buildQueueRowKey(String queueId) {
    return buildQueueRowKey(serverName, queueId);
  }

  /**
   * Parse the original queueId from a row key
   * @param rowKey String representation of a queue's row key
   * @return the original queueId
   */
  private String getRawQueueIdFromRowKey(String rowKey) {
    return rowKey.split(ROW_KEY_DELIMITER)[0];
  }

  /**
   * See safeQueueUpdate(RowMutations mutate)
   *
   * @param put Row mutation to perform on the queue
   */
  private void safeQueueUpdate(Put put) throws ReplicationException, IOException {
    RowMutations mutations = new RowMutations(put.getRow());
    mutations.add(put);
    safeQueueUpdate(mutations);
  }

  /**
   * See safeQueueUpdate(RowMutations mutate)
   *
   * @param delete Row mutation to perform on the queue
   */
  private void safeQueueUpdate(Delete delete) throws ReplicationException,
      IOException{
    RowMutations mutations = new RowMutations(delete.getRow());
    mutations.add(delete);
    safeQueueUpdate(mutations);
  }

  /**
   * Attempt to mutate a given queue in the Replication Table with a checkAndPut on the OWNER column
   * of the queue. Abort the server if this checkAndPut fails: which means we have somehow lost
   * ownership of the column or an IO Exception has occurred during the transaction.
   *
   * @param mutate Mutation to perform on a given queue
   */
  private void safeQueueUpdate(RowMutations mutate) throws ReplicationException, IOException{
    boolean updateSuccess = replicationTable.checkAndMutate(mutate.getRow(), CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, serverNameBytes, mutate);
    if (!updateSuccess) {
      throw new ReplicationException("Failed to update Replication Table because we lost queue " +
        " ownership");
    }
  }

  /**
   * Returns a queue's row key given either its raw or reclaimed queueId
   *
   * @param queueId queueId of the queue
   * @return byte representation of the queue's row key
   */
  private byte[] queueIdToRowKey(String queueId) {
    // Cluster id's are guaranteed to have no hyphens, so if the passed in queueId has no hyphen
    // then this is not a reclaimed queue.
    if (!queueId.contains(ROW_KEY_DELIMITER)) {
      return Bytes.toBytes(buildQueueRowKey(queueId));
      // If the queueId contained some hyphen it was reclaimed. In this case, the queueId is the
      // queue's row key
    } else {
      return Bytes.toBytes(queueId);
    }
  }

  /**
   * Get the QueueIds belonging to the named server from the ReplicationTable
   *
   * @param server name of the server
   * @return a ResultScanner over the QueueIds belonging to the server
   * @throws IOException
   */
  private ResultScanner getQueuesBelongingToServer(String server) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF, COL_OWNER);
    scan.addColumn(CF, COL_OWNER_HISTORY);
    ResultScanner results = replicationTable.getScanner(scan);
    return results;
  }

  /**
   * Check if the queue specified by queueId is stored in HBase
   *
   * @param queueId Either raw or reclaimed format of the queueId
   * @return Whether the queue is stored in HBase
   * @throws IOException
   */
  private boolean checkQueueExists(String queueId) throws IOException {
    byte[] rowKey = queueIdToRowKey(queueId);
    return replicationTable.exists(new Get(rowKey));
  }

  /**
   * Read all of the WAL's from a queue into a list
   *
   * @param queue HBase query result containing the queue
   * @return a list of all the WAL filenames
   */
  private List<String> readWALsFromResult(Result queue) {
    List<String> wals = new ArrayList<>();
    Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF);
    for(byte[] cQualifier : familyMap.keySet()) {
      // Ignore the meta data fields of the queue
      if (Arrays.equals(cQualifier, COL_OWNER) || Arrays.equals(cQualifier, COL_OWNER_HISTORY)) {
        continue;
      }
      wals.add(Bytes.toString(cQualifier));
    }
    return wals;
  }

  /**
   * Attempt to claim the given queue with a checkAndPut on the OWNER column. We check that the
   * recently killed server is still the OWNER before we claim it.
   *
   * @param queue The queue that we are trying to claim
   * @param originalServer The server that originally owned the queue
   * @return Whether we successfully claimed the queue
   * @throws IOException
   */
  private boolean attemptToClaimQueue (Result queue, String originalServer) throws IOException{
    Put putQueueNameAndHistory = new Put(queue.getRow());
    putQueueNameAndHistory.addColumn(CF, COL_OWNER, Bytes.toBytes(serverName));
    String newOwnerHistory = buildClaimedQueueHistory(Bytes.toString(queue.getValue(CF,
      COL_OWNER_HISTORY)), originalServer);
    putQueueNameAndHistory.addColumn(CF, COL_OWNER_HISTORY, Bytes.toBytes(newOwnerHistory));
    RowMutations claimAndRenameQueue = new RowMutations(queue.getRow());
    claimAndRenameQueue.add(putQueueNameAndHistory);
    // Attempt to claim ownership for this queue by checking if the current OWNER is the original
    // server. If it is not then another RS has already claimed it. If it is we set ourselves as the
    // new owner and update the queue's history
    boolean success = replicationTable.checkAndMutate(queue.getRow(), CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(originalServer), claimAndRenameQueue);
    return success;
  }

  /**
   * Creates a "|" delimited record of the queue's past region server owners.
   *
   * @param originalHistory the queue's original owner history
   * @param oldServer the name of the server that used to own the queue
   * @return the queue's new owner history
   */
  private String buildClaimedQueueHistory(String originalHistory, String oldServer) {
    return originalHistory + "|" + oldServer;
  }

  /**
   * Attempts to run a Get on some queue. Will only return a non-null result if we currently own
   * the queue.
   *
   * @param get The get that we want to query
   * @return The result of the get if this server is the owner of the queue. Else it returns null
   * @throws IOException
   */
  private Result getResultIfOwner(Get get) throws IOException {
    Scan scan = new Scan(get);
    // Check if the Get currently contains all columns or only specific columns
    if (scan.getFamilyMap().size() > 0) {
      // Add the OWNER column if the scan is already only over specific columns
      scan.addColumn(CF, COL_OWNER);
    }
    scan.setMaxResultSize(1);
    SingleColumnValueFilter checkOwner = new SingleColumnValueFilter(CF, COL_OWNER,
      CompareFilter.CompareOp.EQUAL, serverNameBytes);
    scan.setFilter(checkOwner);
    ResultScanner scanner = null;
    try {
      scanner = replicationTable.getScanner(scan);
      Result result = scanner.next();
      return (result == null || result.isEmpty()) ? null : result;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }
}
