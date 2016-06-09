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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

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
 * "Replication Table". It utilizes the ReplicationTableBase to access the Replication Table.
 */
@InterfaceAudience.Private
public class TableBasedReplicationQueuesImpl extends ReplicationTableBase
  implements ReplicationQueues {

  private static final Log LOG = LogFactory.getLog(TableBasedReplicationQueuesImpl.class);

  // Common byte values used in replication offset tracking
  private static final byte[] INITIAL_OFFSET_BYTES = Bytes.toBytes(0L);
  private static final byte[] EMPTY_STRING_BYTES = Bytes.toBytes("");

  private String serverName = null;
  private byte[] serverNameBytes = null;

  // TODO: Only use this variable temporarily. Eventually we want to use HBase to store all
  // TODO: replication information
  private ReplicationStateZKBase replicationState;

  public TableBasedReplicationQueuesImpl(ReplicationQueuesArguments args) throws IOException {
    this(args.getConf(), args.getAbortable(), args.getZk());
  }

  public TableBasedReplicationQueuesImpl(Configuration conf, Abortable abort, ZooKeeperWatcher zkw)
    throws IOException {
    super(conf, abort);
    replicationState = new ReplicationStateZKBase(zkw, conf, abort) {};
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    this.serverName = serverName;
    this.serverNameBytes = Bytes.toBytes(serverName);
  }

  @Override
  public List<String> getListOfReplicators() {
    return super.getListOfReplicators();
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
        putNewQueue.addColumn(CF_QUEUE, COL_QUEUE_OWNER, serverNameBytes);
        putNewQueue.addColumn(CF_QUEUE, COL_QUEUE_OWNER_HISTORY, EMPTY_STRING_BYTES);
        putNewQueue.addColumn(CF_QUEUE, Bytes.toBytes(filename), INITIAL_OFFSET_BYTES);
        replicationTable.put(putNewQueue);
      } else {
        // Otherwise simply add the new log and offset as a new column
        Put putNewLog = new Put(queueIdToRowKey(queueId));
        putNewLog.addColumn(CF_QUEUE, Bytes.toBytes(filename), INITIAL_OFFSET_BYTES);
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
      delete.addColumns(CF_QUEUE, Bytes.toBytes(filename));
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
      checkLogExists.addColumn(CF_QUEUE, Bytes.toBytes(filename));
      if (!replicationTable.exists(checkLogExists)) {
        String errMsg = "Could not set position of non-existent log from queueId=" + queueId +
          ", filename=" + filename;
        abortable.abort(errMsg, new ReplicationException(errMsg));
        return;
      }
      // Update the log offset if it exists
      Put walAndOffset = new Put(rowKey);
      walAndOffset.addColumn(CF_QUEUE, Bytes.toBytes(filename), Bytes.toBytes(position));
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
      getOffset.addColumn(CF_QUEUE, Bytes.toBytes(filename));
      Result result = getResultIfOwner(getOffset);
      if (result == null || !result.containsColumn(CF_QUEUE, Bytes.toBytes(filename))) {
        throw new ReplicationException("Could not read empty result while getting log position " +
          "queueId=" + queueId + ", filename=" + filename);
      }
      return Bytes.toLong(result.getValue(CF_QUEUE, Bytes.toBytes(filename)));
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
    return getLogsInQueueAndCheckOwnership(rowKey);
  }

  @Override
  public List<String> getAllQueues() {
    return getAllQueues(serverName);
  }

  @Override
  public Map<String, Set<String>> claimQueues(String regionserver) {
    Map<String, Set<String>> queues = new HashMap<>();
    if (isThisOurRegionServer(regionserver)) {
      return queues;
    }
    ResultScanner queuesToClaim = null;
    try {
      queuesToClaim = getAllQueuesScanner(regionserver);
      for (Result queue : queuesToClaim) {
        if (attemptToClaimQueue(queue, regionserver)) {
          String rowKey = Bytes.toString(queue.getRow());
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(rowKey);
          if (replicationState.peerExists(replicationQueueInfo.getPeerId())) {
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

  /**
   * Get the QueueIds belonging to the named server from the ReplicationTableBase
   *
   * @param server name of the server
   * @return a ResultScanner over the QueueIds belonging to the server
   * @throws IOException
   */
  private ResultScanner getAllQueuesScanner(String server) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filterMyQueues = new SingleColumnValueFilter(CF_QUEUE, COL_QUEUE_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(server));
    scan.setFilter(filterMyQueues);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
    scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER_HISTORY);
    ResultScanner results = replicationTable.getScanner(scan);
    return results;
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

  private List<String> getLogsInQueueAndCheckOwnership(byte[] rowKey) {
    String errMsg = "Failed getting logs in queue queueId=" + Bytes.toString(rowKey);
    List<String> logs = new ArrayList<String>();
    try {
      Get getQueue = new Get(rowKey);
      Result queue = getResultIfOwner(getQueue);
      if (queue == null || queue.isEmpty()) {
        String errMsgLostOwnership = "Failed getting logs for queue queueId=" +
          Bytes.toString(rowKey) + " because the queue was missing or we lost ownership";
        abortable.abort(errMsg, new ReplicationException(errMsgLostOwnership));
        return null;
      }
      Map<byte[], byte[]> familyMap = queue.getFamilyMap(CF_QUEUE);
      for(byte[] cQualifier : familyMap.keySet()) {
        if (Arrays.equals(cQualifier, COL_QUEUE_OWNER) || Arrays.equals(cQualifier,
            COL_QUEUE_OWNER_HISTORY)) {
          continue;
        }
        logs.add(Bytes.toString(cQualifier));
      }
    } catch (IOException e) {
      abortable.abort(errMsg, e);
      return null;
    }
    return logs;
  }

  private String buildQueueRowKey(String queueId) {
    return buildQueueRowKey(serverName, queueId);
  }

  /**
   * Convenience method that gets the row key of the queue specified by queueId
   * @param queueId queueId of a queue in this server
   * @return the row key of the queue in the Replication Table
   */
  private byte[] queueIdToRowKey(String queueId) {
    return queueIdToRowKey(serverName, queueId);
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
    boolean updateSuccess = replicationTable.checkAndMutate(mutate.getRow(), CF_QUEUE,
        COL_QUEUE_OWNER, CompareFilter.CompareOp.EQUAL, serverNameBytes, mutate);
    if (!updateSuccess) {
      throw new ReplicationException("Failed to update Replication Table because we lost queue " +
        " ownership");
    }
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
    putQueueNameAndHistory.addColumn(CF_QUEUE, COL_QUEUE_OWNER, Bytes.toBytes(serverName));
    String newOwnerHistory = buildClaimedQueueHistory(Bytes.toString(queue.getValue(CF_QUEUE,
      COL_QUEUE_OWNER_HISTORY)), originalServer);
    putQueueNameAndHistory.addColumn(CF_QUEUE, COL_QUEUE_OWNER_HISTORY,
        Bytes.toBytes(newOwnerHistory));
    RowMutations claimAndRenameQueue = new RowMutations(queue.getRow());
    claimAndRenameQueue.add(putQueueNameAndHistory);
    // Attempt to claim ownership for this queue by checking if the current OWNER is the original
    // server. If it is not then another RS has already claimed it. If it is we set ourselves as the
    // new owner and update the queue's history
    boolean success = replicationTable.checkAndMutate(queue.getRow(), CF_QUEUE, COL_QUEUE_OWNER,
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes(originalServer), claimAndRenameQueue);
    return success;
  }

  /**
   * Attempts to run a Get on some queue. Will only return a non-null result if we currently own
   * the queue.
   *
   * @param get The Get that we want to query
   * @return The result of the Get if this server is the owner of the queue. Else it returns null.
   * @throws IOException
   */
  private Result getResultIfOwner(Get get) throws IOException {
    Scan scan = new Scan(get);
    // Check if the Get currently contains all columns or only specific columns
    if (scan.getFamilyMap().size() > 0) {
      // Add the OWNER column if the scan is already only over specific columns
      scan.addColumn(CF_QUEUE, COL_QUEUE_OWNER);
    }
    scan.setMaxResultSize(1);
    SingleColumnValueFilter checkOwner = new SingleColumnValueFilter(CF_QUEUE, COL_QUEUE_OWNER,
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
