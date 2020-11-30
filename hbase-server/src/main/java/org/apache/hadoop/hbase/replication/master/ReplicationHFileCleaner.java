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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Implementation of a file cleaner that checks if a hfile is still scheduled for replication before
 * deleting it from hfile archive directory.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationHFileCleaner extends BaseHFileCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationHFileCleaner.class);
  private ZKWatcher zkw;
  private ReplicationQueueStorage rqs;
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
      hfileRefs = rqs.getAllHFileRefs();
    } catch (ReplicationException e) {
      LOG.warn("Failed to read hfile references from zookeeper, skipping checking deletable files");
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

  @Override
  public void setConf(Configuration config) {
    // If either replication or replication of bulk load hfiles is disabled, keep all members null
    if (!(config.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT))) {
      LOG.warn(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY
          + " is not enabled. Better to remove "
          + ReplicationHFileCleaner.class + " from " + HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS
          + " configuration.");
      return;
    }
    // Make my own Configuration. Then I'll have my own connection to zk that
    // I can close myself when time comes.
    Configuration conf = new Configuration(config);
    try {
      setConf(conf, new ZKWatcher(conf, "replicationHFileCleaner", null));
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @InterfaceAudience.Private
  public void setConf(Configuration conf, ZKWatcher zk) {
    super.setConf(conf);
    try {
      initReplicationQueueStorage(conf, zk);
    } catch (Exception e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  private void initReplicationQueueStorage(Configuration conf, ZKWatcher zk) {
    this.zkw = zk;
    this.rqs = ReplicationStorageFactory.getReplicationQueueStorage(zk, conf);
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
      hfileRefsFromQueue = rqs.getAllHFileRefs();
    } catch (ReplicationException e) {
      LOG.warn("Failed to read hfile references from zookeeper, skipping checking deletable "
          + "file for " + fStat.getPath());
      return false;
    }
    return !hfileRefsFromQueue.contains(fStat.getPath().getName());
  }
}
