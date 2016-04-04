/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication.master;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Implementation of a file cleaner that checks if a hfile is still scheduled for replication before
 * deleting it from hfile archive directory.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationHFileCleaner extends BaseHFileCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(ReplicationHFileCleaner.class);
  private ZooKeeperWatcher zkw;
  private ReplicationQueuesClient rqc;
  private boolean stopped = false;

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // all members of this class are null if replication is disabled,
    // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    final Set<String> hfileRefs;
    try {
      // The concurrently created new hfile entries in ZK may not be included in the return list,
      // but they won't be deleted because they're not in the checking set.
      hfileRefs = loadHFileRefsFromPeers();
    } catch (KeeperException e) {
      LOG.warn("Failed to read hfile references from zookeeper, skipping checking deletable files");
      return Collections.emptyList();
    }
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        String hfile = file.getPath().getName();
        boolean foundHFileRefInQueue = hfileRefs.contains(hfile);
        if (LOG.isDebugEnabled()) {
          if (foundHFileRefInQueue) {
            LOG.debug("Found hfile reference in ZK, keeping: " + hfile);
          } else {
            LOG.debug("Did not find hfile reference in ZK, deleting: " + hfile);
          }
        }
        return !foundHFileRefInQueue;
      }
    });
  }

  /**
   * Load all hfile references in all replication queues from ZK. This method guarantees to return a
   * snapshot which contains all hfile references in the zookeeper at the start of this call.
   * However, some newly created hfile references during the call may not be included.
   */
  private Set<String> loadHFileRefsFromPeers() throws KeeperException {
    Set<String> hfileRefs = Sets.newHashSet();
    List<String> listOfPeers;
    for (int retry = 0;; retry++) {
      int v0 = rqc.getHFileRefsNodeChangeVersion();
      hfileRefs.clear();
      listOfPeers = rqc.getAllPeersFromHFileRefsQueue();
      if (listOfPeers == null) {
        LOG.debug("Didn't find any peers with hfile references, won't prevent any deletions.");
        return ImmutableSet.of();
      }
      for (String id : listOfPeers) {
        List<String> peerHFileRefs = rqc.getReplicableHFiles(id);
        if (peerHFileRefs != null) {
          hfileRefs.addAll(peerHFileRefs);
        }
      }
      int v1 = rqc.getHFileRefsNodeChangeVersion();
      if (v0 == v1) {
        return hfileRefs;
      }
      LOG.debug(String.format("Replication hfile references node cversion changed from "
          + "%d to %d, retry = %d", v0, v1, retry));
    }
  }

  @Override
  public void setConf(Configuration config) {
    // If either replication or replication of bulk load hfiles is disabled, keep all members null
    if (!(config.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
      HConstants.REPLICATION_ENABLE_DEFAULT) && config.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT))) {
      LOG.warn(HConstants.REPLICATION_ENABLE_KEY
          + " is not enabled so allowing all hfile references to be deleted. Better to remove "
          + ReplicationHFileCleaner.class + " from " + HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS
          + " configuration.");
      return;
    }
    // Make my own Configuration. Then I'll have my own connection to zk that
    // I can close myself when time comes.
    Configuration conf = new Configuration(config);
    try {
      setConf(conf, new ZooKeeperWatcher(conf, "replicationHFileCleaner", null));
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @VisibleForTesting
  public void setConf(Configuration conf, ZooKeeperWatcher zk) {
    super.setConf(conf);
    try {
      initReplicationQueuesClient(conf, zk);
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  private void initReplicationQueuesClient(Configuration conf, ZooKeeperWatcher zk)
      throws ZooKeeperConnectionException, IOException {
    this.zkw = zk;
    this.rqc = ReplicationFactory.getReplicationQueuesClient(zkw, conf, new WarnOnlyAbortable());
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
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

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    Set<String> hfileRefsFromQueue;
    // all members of this class are null if replication is disabled,
    // so do not stop from deleting the file
    if (getConf() == null) {
      return true;
    }

    try {
      hfileRefsFromQueue = loadHFileRefsFromPeers();
    } catch (KeeperException e) {
      LOG.warn("Failed to read hfile references from zookeeper, skipping checking deletable "
          + "file for " + fStat.getPath());
      return false;
    }
    return !hfileRefsFromQueue.contains(fStat.getPath().getName());
  }

  private static class WarnOnlyAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("ReplicationHFileCleaner received abort, ignoring.  Reason: " + why);
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
