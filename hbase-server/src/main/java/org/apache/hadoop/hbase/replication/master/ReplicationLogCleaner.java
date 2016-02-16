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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.zookeeper.KeeperException;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * replication before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationLogCleaner extends BaseLogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(ReplicationLogCleaner.class);
  private ZooKeeperWatcher zkw;
  private ReplicationQueuesClient replicationQueues;
  private boolean stopped = false;


  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
   // all members of this class are null if replication is disabled,
   // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    final Set<String> wals;
    try {
      // The concurrently created new WALs may not be included in the return list,
      // but they won't be deleted because they're not in the checking set.
      wals = loadWALsFromQueues();
    } catch (KeeperException e) {
      LOG.warn("Failed to read zookeeper, skipping checking deletable files");
      return Collections.emptyList();
    }
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        String hlog = file.getPath().getName();
        boolean logInReplicationQueue = wals.contains(hlog);
        if (LOG.isDebugEnabled()) {
          if (logInReplicationQueue) {
            LOG.debug("Found log in ZK, keeping: " + hlog);
          } else {
            LOG.debug("Didn't find this log in ZK, deleting: " + hlog);
          }
        }
       return !logInReplicationQueue;
      }});
  }

  /**
   * Load all wals in all replication queues from ZK. This method guarantees to return a
   * snapshot which contains all WALs in the zookeeper at the start of this call even there
   * is concurrent queue failover. However, some newly created WALs during the call may
   * not be included.
   */
  private Set<String> loadWALsFromQueues() throws KeeperException {
    for (int retry = 0; ; retry++) {
      int v0 = replicationQueues.getQueuesZNodeCversion();
      List<String> rss = replicationQueues.getListOfReplicators();
      if (rss == null) {
        LOG.debug("Didn't find any region server that replicates, won't prevent any deletions.");
        return ImmutableSet.of();
      }
      Set<String> wals = Sets.newHashSet();
      for (String rs : rss) {
        List<String> listOfPeers = replicationQueues.getAllQueues(rs);
        // if rs just died, this will be null
        if (listOfPeers == null) {
          continue;
        }
        for (String id : listOfPeers) {
          List<String> peersWals = replicationQueues.getLogsInQueue(rs, id);
          if (peersWals != null) {
            wals.addAll(peersWals);
          }
        }
      }
      int v1 = replicationQueues.getQueuesZNodeCversion();
      if (v0 == v1) {
        return wals;
      }
      LOG.info(String.format("Replication queue node cversion changed from %d to %d, retry = %d",
          v0, v1, retry));
    }
  }

  @Override
  public void setConf(Configuration config) {
    // If replication is disabled, keep all members null
    if (!config.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT)) {
      LOG.warn("Not configured - allowing all hlogs to be deleted");
      return;
    }
    // Make my own Configuration.  Then I'll have my own connection to zk that
    // I can close myself when comes time.
    Configuration conf = new Configuration(config);
    try {
      setConf(conf, new ZooKeeperWatcher(conf, "replicationLogCleaner", null));
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @VisibleForTesting
  public void setConf(Configuration conf, ZooKeeperWatcher zk) {
    super.setConf(conf);
    try {
      this.zkw = zk;
      this.replicationQueues = ReplicationFactory.getReplicationQueuesClient(zkw, conf,
          new WarnOnlyAbortable());
      this.replicationQueues.init();
    } catch (ReplicationException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @Override
  public void stop(String why) {
    if (this.stopped) return;
    this.stopped = true;
    if (this.zkw != null) {
      LOG.info("Stopping " + this.zkw);
      this.zkw.close();
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  private static class WarnOnlyAbortable implements Abortable {

    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("ReplicationLogCleaner received abort, ignoring.  Reason: " + why);
      if (LOG.isDebugEnabled()) {
        LOG.debug(e);
      }
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
}
