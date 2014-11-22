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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;

/**
 * Interface that defines a replication source
 */
@InterfaceAudience.Private
public interface ReplicationSourceInterface {

  /**
   * Initializer for the source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param replicationQueues
   * @param replicationPeers
   * @param stopper the stopper object for this region server
   * @param peerClusterZnode
   * @param clusterId
   * @throws IOException
   */
  public void init(final Configuration conf, final FileSystem fs,
      final ReplicationSourceManager manager, final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final Stoppable stopper,
      final String peerClusterZnode, final UUID clusterId, ReplicationEndpoint replicationEndpoint,
      final MetricsSource metrics) throws IOException;

  /**
   * Add a log to the list of logs to replicate
   * @param log path to the log to replicate
   */
  void enqueueLog(Path log);

  /**
   * Get the current log that's replicated
   * @return the current log
   */
  Path getCurrentPath();

  /**
   * Start the replication
   */
  void startup();

  /**
   * End the replication
   * @param reason why it's terminating
   */
  void terminate(String reason);

  /**
   * End the replication
   * @param reason why it's terminating
   * @param cause the error that's causing it
   */
  void terminate(String reason, Exception cause);

  /**
   * Get the id that the source is replicating to
   *
   * @return peer cluster id
   */
  String getPeerClusterZnode();

  /**
   * Get the id that the source is replicating to.
   *
   * @return peer cluster id
   */
  String getPeerClusterId();

  /**
   * Get a string representation of the current statistics
   * for this source
   * @return printable stats
   */
  String getStats();

}
