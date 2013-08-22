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
package org.apache.hadoop.hbase.replication.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClientZKImpl;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * replication before deleting it when its TTL is over.
 */
@InterfaceAudience.Private
public class ReplicationLogCleaner extends BaseLogCleanerDelegate implements Abortable {
  private static final Log LOG = LogFactory.getLog(ReplicationLogCleaner.class);
  private ZooKeeperWatcher zkw;
  private ReplicationQueuesClient replicationQueues;
  private final Set<String> hlogs = new HashSet<String>();
  private boolean stopped = false;
  private boolean aborted;


  @Override
  public boolean isLogDeletable(FileStatus fStat) {

    // all members of this class are null if replication is disabled, and we
    // return true since false would render the LogsCleaner useless
    if (this.getConf() == null) {
      return true;
    }
    String log = fStat.getPath().getName();
    // If we saw the hlog previously, let's consider it's still used
    // At some point in the future we will refresh the list and it will be gone
    if (this.hlogs.contains(log)) {
      return false;
    }

    // Let's see it's still there
    // This solution makes every miss very expensive to process since we
    // almost completely refresh the cache each time
    return !refreshHLogsAndSearch(log);
  }

  /**
   * Search through all the hlogs we have in ZK to refresh the cache
   * If a log is specified and found, then we early out and return true
   * @param searchedLog log we are searching for, pass null to cache everything
   *                    that's in zookeeper.
   * @return false until a specified log is found.
   */
  private boolean refreshHLogsAndSearch(String searchedLog) {
    this.hlogs.clear();
    final boolean lookForLog = searchedLog != null;
    List<String> rss = replicationQueues.getListOfReplicators();
    if (rss == null) {
      LOG.debug("Didn't find any region server that replicates, deleting: " +
          searchedLog);
      return false;
    }
    for (String rs: rss) {
      List<String> listOfPeers = replicationQueues.getAllQueues(rs);
      // if rs just died, this will be null
      if (listOfPeers == null) {
        continue;
      }
      for (String id : listOfPeers) {
        List<String> peersHlogs = replicationQueues.getLogsInQueue(rs, id);
        if (peersHlogs != null) {
          this.hlogs.addAll(peersHlogs);
        }
        // early exit if we found the log
        if(lookForLog && this.hlogs.contains(searchedLog)) {
          LOG.debug("Found log in ZK, keeping: " + searchedLog);
          return true;
        }
      }
    }
    LOG.debug("Didn't find this log in ZK, deleting: " + searchedLog);
    return false;
  }

  @Override
  public void setConf(Configuration config) {
    // If replication is disabled, keep all members null
    if (!config.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      return;
    }
    // Make my own Configuration.  Then I'll have my own connection to zk that
    // I can close myself when comes time.
    Configuration conf = new Configuration(config);
    super.setConf(conf);
    try {
      this.zkw = new ZooKeeperWatcher(conf, "replicationLogCleaner", null);
      this.replicationQueues = ReplicationFactory.getReplicationQueuesClient(zkw, conf, this);
      this.replicationQueues.init();
    } catch (ReplicationException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
    refreshHLogsAndSearch(null);
  }


  @Override
  public void stop(String why) {
    if (this.stopped) return;
    this.stopped = true;
    if (this.zkw != null) {
      LOG.info("Stopping " + this.zkw);
      this.zkw.close();
    }
    // Not sure why we're deleting a connection that we never acquired or used
    HConnectionManager.deleteConnection(this.getConf());
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.warn("Aborting ReplicationLogCleaner because " + why, e);
    this.aborted = true;
    stop(why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}
