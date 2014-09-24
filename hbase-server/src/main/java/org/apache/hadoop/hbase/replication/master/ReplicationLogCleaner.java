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
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * replication before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationLogCleaner extends BaseLogCleanerDelegate implements Abortable {
  private static final Log LOG = LogFactory.getLog(ReplicationLogCleaner.class);
  private ZooKeeperWatcher zkw;
  private ReplicationQueuesClient replicationQueues;
  private boolean stopped = false;
  private boolean aborted;


  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
   // all members of this class are null if replication is disabled,
   // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    final Set<String> hlogs = loadHLogsFromQueues();
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        String hlog = file.getPath().getName();
        boolean logInReplicationQueue = hlogs.contains(hlog);
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
   * Load all hlogs in all replication queues from ZK
   */
  private Set<String> loadHLogsFromQueues() {
    List<String> rss = replicationQueues.getListOfReplicators();
    if (rss == null) {
      LOG.debug("Didn't find any region server that replicates, won't prevent any deletions.");
      return ImmutableSet.of();
    }
    Set<String> hlogs = Sets.newHashSet();
    for (String rs: rss) {
      List<String> listOfPeers = replicationQueues.getAllQueues(rs);
      // if rs just died, this will be null
      if (listOfPeers == null) {
        continue;
      }
      for (String id : listOfPeers) {
        List<String> peersHlogs = replicationQueues.getLogsInQueue(rs, id);
        if (peersHlogs != null) {
          hlogs.addAll(peersHlogs);
        }
      }
    }
    return hlogs;
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
