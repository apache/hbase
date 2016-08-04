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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
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
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesArguments;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.wal.WALKey;
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

    try {
      this.replicationQueues =
          ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, this.server,
            server.getZooKeeper()));
      this.replicationQueues.init(this.server.getServerName().toString());
      this.replicationPeers =
          ReplicationFactory.getReplicationPeers(server.getZooKeeper(), this.conf, this.server);
      this.replicationPeers.init();
      this.replicationTracker =
          ReplicationFactory.getReplicationTracker(server.getZooKeeper(), this.replicationPeers,
            this.conf, this.server, this.server);
    } catch (Exception e) {
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
    this.replicationManager.join();
    if (this.replicationSink != null) {
      this.replicationSink.stopReplicationSinkServices();
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
    this.replicationSink.replicateEntries(entries, cells, replicationClusterId,
      sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath);
  }

  /**
   * If replication is enabled and this cluster is a master,
   * it starts
   * @throws IOException
   */
  public void startReplicationService() throws IOException {
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

  /**
   * Get the replication sources manager
   * @return the manager if replication is enabled, else returns false
   */
  public ReplicationSourceManager getReplicationManager() {
    return this.replicationManager;
  }

  @Override
  public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) throws IOException {
    scopeWALEdits(logKey, logEdit, this.conf, this.getReplicationManager());
  }

  @Override
  public void postAppend(long entryLen, long elapsedTimeMillis, final WALKey logKey,
      final WALEdit edit) throws IOException {
    NavigableMap<byte[], Integer> scopes = logKey.getReplicationScopes();
    if (this.replicationForBulkLoadData && scopes != null && !scopes.isEmpty()) {
      TableName tableName = logKey.getTablename();
      for (Cell c : edit.getCells()) {
        // Only check for bulk load events
        if (CellUtil.matchingQualifier(c, WALEdit.BULK_LOAD)) {
          BulkLoadDescriptor bld = null;
          try {
            bld = WALEdit.getBulkLoadDescriptor(c);
          } catch (IOException e) {
            LOG.error("Failed to get bulk load events information from the wal file.", e);
            throw e;
          }

          for (StoreDescriptor s : bld.getStoresList()) {
            byte[] fam = s.getFamilyName().toByteArray();
            if (scopes.containsKey(fam)) {
              addHFileRefsToQueue(this.getReplicationManager(), tableName, fam, s);
            }
          }
        }
      }
    }
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys from
   * compaction WAL edits and if the scope is local.
   * @param logKey Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   * @param replicationManager Manager used to add bulk load events hfile references
   * @throws IOException If failed to parse the WALEdit
   */
  public static void scopeWALEdits(WALKey logKey,
      WALEdit logEdit, Configuration conf, ReplicationSourceManager replicationManager)
          throws IOException {
    boolean replicationForBulkLoadEnabled = isReplicationForBulkLoadDataEnabled(conf);
    boolean foundOtherEdits = false;
    for (Cell cell : logEdit.getCells()) {
      if (!CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
        foundOtherEdits = true;
        break;
      }
    }

    if (!foundOtherEdits && logEdit.getCells().size() > 0) {
      WALProtos.RegionEventDescriptor maybeEvent =
          WALEdit.getRegionEventDescriptor(logEdit.getCells().get(0));
      if (maybeEvent != null && (maybeEvent.getEventType() ==
          WALProtos.RegionEventDescriptor.EventType.REGION_CLOSE)) {
        // In serially replication, we use scopes when reading close marker.
        foundOtherEdits = true;
      }
    }
    if ((!replicationForBulkLoadEnabled && !foundOtherEdits) || logEdit.isReplay()) {
      logKey.serializeReplicationScope(false);
    }
  }

  private static void addHFileRefsToQueue(ReplicationSourceManager replicationManager,
      TableName tableName, byte[] family, StoreDescriptor s) throws IOException {
    try {
      replicationManager.addHFileRefs(tableName, family, s.getStoreFileList());
    } catch (ReplicationException e) {
      LOG.error("Failed to add hfile references in the replication queue.", e);
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
