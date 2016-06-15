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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements the ReplicationQueuesClient interface on top of the Replication Table. It utilizes
 * the ReplicationTableBase to access the Replication Table.
 */
@InterfaceAudience.Private
public class TableBasedReplicationQueuesClientImpl extends ReplicationTableBase
  implements ReplicationQueuesClient {

  public TableBasedReplicationQueuesClientImpl(ReplicationQueuesClientArguments args)
    throws IOException {
    super(args.getConf(), args.getAbortable());
  }
  public TableBasedReplicationQueuesClientImpl(Configuration conf,
                                               Abortable abortable) throws IOException {
    super(conf, abortable);
  }

  @Override
  public void init() throws ReplicationException{
    // no-op
  }

  @Override
  public List<String> getListOfReplicators() {
    return super.getListOfReplicators();
  }

  @Override
  public List<String> getLogsInQueue(String serverName, String queueId) {
    return super.getLogsInQueue(serverName, queueId);
  }

  @Override
  public List<String> getAllQueues(String serverName) {
    return super.getAllQueues(serverName);
  }

  @Override
  public Set<String> getAllWALs() {
    Set<String> allWals = new HashSet<String>();
    ResultScanner allQueues = null;
    try (Table replicationTable = getOrBlockOnReplicationTable()) {
      allQueues = replicationTable.getScanner(new Scan());
      for (Result queue : allQueues) {
        for (String wal : readWALsFromResult(queue)) {
          allWals.add(wal);
        }
      }
    } catch (IOException e) {
      String errMsg = "Failed getting all WAL's in Replication Table";
      abortable.abort(errMsg, e);
    } finally {
      if (allQueues != null) {
        allQueues.close();
      }
    }
    return allWals;
  }

  @Override
  public int getHFileRefsNodeChangeVersion() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws KeeperException {
    // TODO
    throw new NotImplementedException();
  }
}
