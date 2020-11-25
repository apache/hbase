/**
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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * replication before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationLogCleaner extends BaseLogCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogCleaner.class);
  private ZKWatcher zkw;
  private ReplicationQueueStorage queueStorage;
  private boolean stopped = false;
  private Set<String> wals;
  private long readZKTimestamp = 0;

  @Override
  public void preClean() {
    readZKTimestamp = EnvironmentEdgeManager.currentTime();
    try {
      // The concurrently created new WALs may not be included in the return list,
      // but they won't be deleted because they're not in the checking set.
      wals = queueStorage.getAllWALs();
    } catch (ReplicationException e) {
      LOG.warn("Failed to read zookeeper, skipping checking deletable files");
      wals = null;
    }
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // all members of this class are null if replication is disabled,
    // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    if (wals == null) {
      return Collections.emptyList();
    }
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        // just for overriding the findbugs NP warnings, as the parameter is marked as Nullable in
        // the guava Predicate.
        if (file == null) {
          return false;
        }
        String wal = file.getPath().getName();
        boolean logInReplicationQueue = wals.contains(wal);
        if (logInReplicationQueue) {
          LOG.debug("Found up in ZooKeeper, NOT deleting={}", wal);
        }
        return !logInReplicationQueue && (file.getModificationTime() < readZKTimestamp);
      }
    });
  }

  @Override
  public void setConf(Configuration config) {
    // Make my own Configuration.  Then I'll have my own connection to zk that
    // I can close myself when comes time.
    Configuration conf = new Configuration(config);
    try {
      setConf(conf, new ZKWatcher(conf, "replicationLogCleaner", null));
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @InterfaceAudience.Private
  public void setConf(Configuration conf, ZKWatcher zk) {
    super.setConf(conf);
    try {
      this.zkw = zk;
      this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(zk, conf);
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
}
