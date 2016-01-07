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
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_ENABLE_KEY;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;

/**
 * Gateway to Replication.  Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}.
 */
public class Replication implements WALActionsListener, 
  ReplicationSourceService, ReplicationSinkService {
  private boolean replication;
  private ReplicationSourceManager replicationManager;
  private final AtomicBoolean replicating = new AtomicBoolean(true);
  private ReplicationZookeeper zkHelper;
  private Configuration conf;
  private ReplicationSink replicationSink;
  // Hosting server
  private Server server;

  /**
   * Instantiate the replication management (if rep is enabled).
   * @param server Hosting server
   * @param fs handle to the filesystem
   * @param logDir
   * @param oldLogDir directory where logs are archived
   * @throws IOException
   */
  public Replication(final Server server, final FileSystem fs,
      final Path logDir, final Path oldLogDir) throws IOException{
    initialize(server, fs, logDir, oldLogDir);
  }

  /**
   * Empty constructor
   */
  public Replication() {
  }

  public void initialize(final Server server, final FileSystem fs,
      final Path logDir, final Path oldLogDir) throws IOException {
    this.server = server;
    this.conf = this.server.getConfiguration();
    this.replication = isReplication(this.conf);
    if (replication) {
      try {
        this.zkHelper = new ReplicationZookeeper(server, this.replicating);
      } catch (KeeperException ke) {
        throw new IOException("Failed replication handler create " +
           "(replicating=" + this.replicating, ke);
      }
      this.replicationManager = new ReplicationSourceManager(zkHelper, conf,
          this.server, fs, this.replicating, logDir, oldLogDir) ;
    } else {
      this.replicationManager = null;
      this.zkHelper = null;
    }
  }

   /**
    * @param c Configuration to look at
    * @return True if replication is enabled.
    */
  public static boolean isReplication(final Configuration c) {
    return c.getBoolean(REPLICATION_ENABLE_KEY, false);
  }

   /*
    * Returns an object to listen to new hlog changes
    **/
  public WALActionsListener getWALActionsListener() {
    return this;
  }
  /**
   * Stops replication service.
   */
  public void stopReplicationService() {
    join();
  }

  /**
   * Join with the replication threads
   */
  public void join() {
    if (this.replication) {
      this.replicationManager.join();
      if (this.replicationSink != null) {
        this.replicationSink.stopReplicationSinkServices();
      }
    }
  }

  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of entries to replicate
   * @throws IOException
   */
  public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    if (this.replication) {
      this.replicationSink.replicateEntries(entries);
    }
  }

  /**
   * If replication is enabled and this cluster is a master,
   * it starts
   * @throws IOException
   */
  public void startReplicationService() throws IOException {
    if (this.replication) {
      this.replicationManager.init();
      this.replicationSink = new ReplicationSink(this.conf, this.server);
    }
  }

  /**
   * Get the replication sources manager
   * @return the manager if replication is enabled, else returns false
   */
  public ReplicationSourceManager getReplicationManager() {
    return this.replicationManager;
  }

  @Override
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
      WALEdit logEdit) {
    // Not interested
  }

  @Override
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey,
                                       WALEdit logEdit) {
    byte[] family;
    for (KeyValue kv : logEdit.getKeyValues()) {
      family = kv.getFamily();
      int scope = htd.getFamily(family).getScope();
      if (scope != REPLICATION_SCOPE_LOCAL &&
          !logEdit.hasKeyInScope(family)) {
        logEdit.putIntoScope(family, scope);
      }
    }
  }

  @Override
  public void preLogRoll(Path oldPath, Path newPath) throws IOException {
    getReplicationManager().preLogRoll(newPath);
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
    getReplicationManager().postLogRoll(newPath);
  }

  @Override
  public void preLogArchive(Path oldPath, Path newPath) throws IOException {
    // Not interested
  }

  @Override
  public void postLogArchive(Path oldPath, Path newPath) throws IOException {
    // Not interested
  }

  /**
   * This method modifies the master's configuration in order to inject
   * replication-related features
   * @param conf
   */
  public static void decorateMasterConfiguration(Configuration conf) {
    if (!isReplication(conf)) {
      return;
    }
    String plugins = conf.get(HBASE_MASTER_LOGCLEANER_PLUGINS);
    String cleanerClass = ReplicationLogCleaner.class.getCanonicalName();
    if (!plugins.contains(cleanerClass)) {
      conf.set(HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
    }
  }

  @Override
  public void logRollRequested() {
    // Not interested
  }

  @Override
  public void logCloseRequested() {
    // not interested
  }
}
