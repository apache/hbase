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
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
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
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
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
  private boolean replicationForBulkLoadData;
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
  // ReplicationLoad to access replication metrics
  private ReplicationLoad replicationLoad;
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
    this.replicationForBulkLoadData = isReplicationForBulkLoadDataEnabled(this.conf);
    this.scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setNameFormat(server.getServerName().toShortString() + "Replication Statistics #%d")
        .setDaemon(true)
        .build());
    if (this.replicationForBulkLoadData) {
      if (conf.get(HConstants.REPLICATION_CLUSTER_ID) == null
          || conf.get(HConstants.REPLICATION_CLUSTER_ID).isEmpty()) {
        throw new IllegalArgumentException(HConstants.REPLICATION_CLUSTER_ID
            + " cannot be null/empty when " + HConstants.REPLICATION_BULKLOAD_ENABLE_KEY
            + " is set to true.");
      }
    }
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
      this.replicationLoad = new ReplicationLoad();
    } else {
      this.replicationManager = null;
      this.replicationQueues = null;
      this.replicationPeers = null;
      this.replicationTracker = null;
      this.replicationLoad = null;
    }
  }

   /**
    * @param c Configuration to look at
    * @return True if replication is enabled.
    */
  public static boolean isReplication(final Configuration c) {
    return c.getBoolean(REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
  }

  /**
   * @param c Configuration to look at
   * @return True if replication for bulk load data is enabled.
   */
  public static boolean isReplicationForBulkLoadDataEnabled(final Configuration c) {
    return c.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
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
   * @param cells The data -- the cells -- that <code>entries</code> describes (the entries do not
   *          contain the Cells we are replicating; they are passed here on the side in this
   *          CellScanner).
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *          directory required for replicating hfiles
   * @param sourceHFileArchiveDirPath Path that point to the source cluster hfile archive directory
   * @throws IOException
   */
  public void replicateLogEntries(List<WALEntry> entries, CellScanner cells,
      String replicationClusterId, String sourceBaseNamespaceDirPath,
      String sourceHFileArchiveDirPath) throws IOException {
    if (this.replication) {
      this.replicationSink.replicateEntries(entries, cells, replicationClusterId,
        sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath);
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
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey, WALEdit logEdit)
      throws IOException {
    scopeWALEdits(htd, logKey, logEdit, this.conf, this.getReplicationManager());
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys from
   * compaction WAL edits and if the scope is local.
   * @param htd Descriptor used to find the scope to use
   * @param logKey Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   * @param replicationManager Manager used to add bulk load events hfile references
   * @throws IOException If failed to parse the WALEdit
   */
  public static void scopeWALEdits(HTableDescriptor htd, WALKey logKey, WALEdit logEdit,
      Configuration conf, ReplicationSourceManager replicationManager) throws IOException {
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    byte[] family;
    boolean replicationForBulkLoadEnabled = isReplicationForBulkLoadDataEnabled(conf);
    for (Cell cell : logEdit.getCells()) {
      if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
        if (replicationForBulkLoadEnabled && CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
          scopeBulkLoadEdits(htd, replicationManager, scopes, logKey.getTablename(), cell);
        } else {
          // Skip the flush/compaction/region events
          continue;
        }
      } else {
        family = CellUtil.cloneFamily(cell);
        // Unexpected, has a tendency to happen in unit tests
        assert htd.getFamily(family) != null;

        if (!scopes.containsKey(family)) {
          int scope = htd.getFamily(family).getScope();
          if (scope != REPLICATION_SCOPE_LOCAL) {
            scopes.put(family, scope);
          }
        }
      }
    }
    if (!scopes.isEmpty()) {
      logKey.setScopes(scopes);
    }
  }

  private static void scopeBulkLoadEdits(HTableDescriptor htd,
      ReplicationSourceManager replicationManager, NavigableMap<byte[], Integer> scopes,
      TableName tableName, Cell cell) throws IOException {
    byte[] family;
    try {
      BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
      for (StoreDescriptor s : bld.getStoresList()) {
        family = s.getFamilyName().toByteArray();
        if (!scopes.containsKey(family)) {
          int scope = htd.getFamily(family).getScope();
          if (scope != REPLICATION_SCOPE_LOCAL) {
            scopes.put(family, scope);
            addHFileRefsToQueue(replicationManager, tableName, family, s);
          }
        } else {
          addHFileRefsToQueue(replicationManager, tableName, family, s);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get bulk load events information from the wal file.", e);
      throw e;
    }
  }

  private static void addHFileRefsToQueue(ReplicationSourceManager replicationManager,
      TableName tableName, byte[] family, StoreDescriptor s) throws IOException {
    try {
      replicationManager.addHFileRefs(tableName, family, s.getStoreFileList());
    } catch (ReplicationException e) {
      LOG.error("Failed to create hfile references in ZK.", e);
      throw new IOException(e);
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
   * This method modifies the master's configuration in order to inject replication-related features
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
    if (isReplicationForBulkLoadDataEnabled(conf)) {
      plugins = conf.get(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
      cleanerClass = ReplicationHFileCleaner.class.getCanonicalName();
      if (!plugins.contains(cleanerClass)) {
        conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, plugins + "," + cleanerClass);
      }
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

  @Override
  public ReplicationLoad refreshAndGetReplicationLoad() {
    if (this.replicationLoad == null) {
      return null;
    }
    // always build for latest data
    buildReplicationLoad();
    return this.replicationLoad;
  }

  private void buildReplicationLoad() {
    // get source
    List<ReplicationSourceInterface> sources = this.replicationManager.getSources();
    List<MetricsSource> sourceMetricsList = new ArrayList<MetricsSource>();

    for (ReplicationSourceInterface source : sources) {
      if (source instanceof ReplicationSource) {
        sourceMetricsList.add(((ReplicationSource) source).getSourceMetrics());
      }
    }
    // get sink
    MetricsSink sinkMetrics = this.replicationSink.getSinkMetrics();
    this.replicationLoad.buildReplicationLoad(sourceMetricsList, sinkMetrics);
  }
}
