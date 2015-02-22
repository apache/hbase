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

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_ENABLE_KEY;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Gateway to Replication.  Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}.
 */
@InterfaceAudience.Private
public class Replication extends WALActionsListener.Base implements 
  ReplicationSourceService, ReplicationSinkService {
  private static final Log LOG =
      LogFactory.getLog(Replication.class);
  private boolean replication;
  private ReplicationSourceManager replicationManager;
  private ReplicationQueues replicationQueues;
  private ReplicationPeers replicationPeers;
  private ReplicationTracker replicationTracker;
  private Configuration conf;
  private ReplicationSink replicationSink;
  // Hosting server
  private Server server;
  /** Statistics thread schedule pool */
  private ScheduledExecutorService scheduleThreadPool;
  private int statsThreadPeriod;

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
    this.scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setNameFormat(server.getServerName().toShortString() + "Replication Statistics #%d")
        .setDaemon(true)
        .build());
    if (replication) {
      try {
        this.replicationQueues =
            ReplicationFactory.getReplicationQueues(server.getZooKeeper(), this.conf, this.server);
        this.replicationQueues.init(this.server.getServerName().toString());
        this.replicationPeers =
            ReplicationFactory.getReplicationPeers(server.getZooKeeper(), this.conf, this.server);
        this.replicationPeers.init();
        this.replicationTracker =
            ReplicationFactory.getReplicationTracker(server.getZooKeeper(), this.replicationPeers,
              this.conf, this.server, this.server);
      } catch (ReplicationException e) {
        throw new IOException("Failed replication handler create", e);
      }
      UUID clusterId = null;
      try {
        clusterId = ZKClusterId.getUUIDForCluster(this.server.getZooKeeper());
      } catch (KeeperException ke) {
        throw new IOException("Could not read cluster id", ke);
      }
      this.replicationManager =
          new ReplicationSourceManager(replicationQueues, replicationPeers, replicationTracker,
              conf, this.server, fs, logDir, oldLogDir, clusterId);
      this.statsThreadPeriod =
          this.conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
      LOG.debug("ReplicationStatisticsThread " + this.statsThreadPeriod);
    } else {
      this.replicationManager = null;
      this.replicationQueues = null;
      this.replicationPeers = null;
      this.replicationTracker = null;
    }
  }

   /**
    * @param c Configuration to look at
    * @return True if replication is enabled.
    */
  public static boolean isReplication(final Configuration c) {
    return c.getBoolean(REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
  }

   /*
    * Returns an object to listen to new wal changes
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
    scheduleThreadPool.shutdown();
  }

  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of entries to replicate
   * @param cells The data -- the cells -- that <code>entries</code> describes (the entries
   * do not contain the Cells we are replicating; they are passed here on the side in this
   * CellScanner).
   * @throws IOException
   */
  public void replicateLogEntries(List<WALEntry> entries, CellScanner cells) throws IOException {
    if (this.replication) {
      this.replicationSink.replicateEntries(entries, cells);
    }
  }

  /**
   * If replication is enabled and this cluster is a master,
   * it starts
   * @throws IOException
   */
  public void startReplicationService() throws IOException {
    if (this.replication) {
      try {
        this.replicationManager.init();
      } catch (ReplicationException e) {
        throw new IOException(e);
      }
      this.replicationSink = new ReplicationSink(this.conf, this.server);
      this.scheduleThreadPool.scheduleAtFixedRate(
        new ReplicationStatisticsThread(this.replicationSink, this.replicationManager),
        statsThreadPeriod, statsThreadPeriod, TimeUnit.SECONDS);
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
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey,
                                       WALEdit logEdit) {
    scopeWALEdits(htd, logKey, logEdit);
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys
   * from compaction WAL edits and if the scope is local.
   * @param htd Descriptor used to find the scope to use
   * @param logKey Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   */
  public static void scopeWALEdits(HTableDescriptor htd, WALKey logKey,
                                   WALEdit logEdit) {
    NavigableMap<byte[], Integer> scopes =
        new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    byte[] family;
    for (Cell cell : logEdit.getCells()) {
      family = cell.getFamily();
      // This is expected and the KV should not be replicated
      if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) continue;
      // Unexpected, has a tendency to happen in unit tests
      assert htd.getFamily(family) != null;

      int scope = htd.getFamily(family).getScope();
      if (scope != REPLICATION_SCOPE_LOCAL &&
          !scopes.containsKey(family)) {
        scopes.put(family, scope);
      }
    }
    if (!scopes.isEmpty()) {
      logKey.setScopes(scopes);
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

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  static class ReplicationStatisticsThread extends Thread {

    private final ReplicationSink replicationSink;
    private final ReplicationSourceManager replicationManager;

    public ReplicationStatisticsThread(final ReplicationSink replicationSink,
                            final ReplicationSourceManager replicationManager) {
      super("ReplicationStatisticsThread");
      this.replicationManager = replicationManager;
      this.replicationSink = replicationSink;
    }

    @Override
    public void run() {
      printStats(this.replicationManager.getStats());
      printStats(this.replicationSink.getStats());
    }

    private void printStats(String stats) {
      if (!stats.isEmpty()) {
        LOG.info(stats);
      }
    }
  }
}
