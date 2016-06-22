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
import org.apache.hadoop.hbase.replication.ReplicationQueuesClientArguments;
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
      wals = replicationQueues.getAllWALs();
    } catch (KeeperException e) {
      LOG.warn("Failed to read zookeeper, skipping checking deletable files");
      return Collections.emptyList();
    }
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        String wal = file.getPath().getName();
        boolean logInReplicationQueue = wals.contains(wal);
        if (LOG.isDebugEnabled()) {
          if (logInReplicationQueue) {
            LOG.debug("Found log in ZK, keeping: " + wal);
          } else {
            LOG.debug("Didn't find this log in ZK, deleting: " + wal);
          }
        }
       return !logInReplicationQueue;
      }});
  }

  @Override
  public void setConf(Configuration config) {
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
      this.replicationQueues = ReplicationFactory.getReplicationQueuesClient(
          new ReplicationQueuesClientArguments(conf, new WarnOnlyAbortable(), zkw));
      this.replicationQueues.init();
    } catch (Exception e) {
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
