/*
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

import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.OFFSET_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_INFO_FAMILY;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_TABLE_NAME;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.RS_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.TIMESTAMP_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.WAL_NAME_COLUMN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * <p>
 * This class is responsible for replicating the edits coming from another cluster.
 * </p>
 * <p>
 * This replication process is currently waiting for the edits to be applied before the method can
 * return. This means that the replication of edits is synchronized (after reading from WALs in
 * ReplicationSource) and that a single region server cannot receive edits from two sources at the
 * same time
 * </p>
 * <p>
 * This class uses the native HBase client in order to replicate entries.
 * </p>
 * TODO make this class more like ReplicationSource wrt log handling
 */
@InterfaceAudience.Private
public class ReplicationSink {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSink.class);
  private final Configuration conf;
  // Volatile because of note in here -- look for double-checked locking:
  // http://www.oracle.com/technetwork/articles/javase/bloch-effective-08-qa-140880.html
  private volatile AsyncClusterConnection sharedConn;
  private final MetricsSink metrics;
  private final AtomicLong totalReplicatedEdits = new AtomicLong();
  private final Object sharedConnLock = new Object();
  // Number of hfiles that we successfully replicated
  private long hfilesReplicated = 0;
  private SourceFSConfigurationProvider provider;
  private WALEntrySinkFilter walEntrySinkFilter;

  /**
   * Row size threshold for multi requests above which a warning is logged
   */
  private final int rowSizeWarnThreshold;
  private boolean replicationSinkTrackerEnabled;

  private final RegionServerCoprocessorHost rsServerHost;

  /**
   * Create a sink for replication
   * @param conf conf object
   * @throws IOException thrown when HDFS goes bad or bad file name
   */
  public ReplicationSink(Configuration conf, RegionServerCoprocessorHost rsServerHost)
    throws IOException {
    this.conf = HBaseConfiguration.create(conf);
    this.rsServerHost = rsServerHost;
    rowSizeWarnThreshold =
      conf.getInt(HConstants.BATCH_ROWS_THRESHOLD_NAME, HConstants.BATCH_ROWS_THRESHOLD_DEFAULT);
    replicationSinkTrackerEnabled = conf.getBoolean(REPLICATION_SINK_TRACKER_ENABLED_KEY,
      REPLICATION_SINK_TRACKER_ENABLED_DEFAULT);
    decorateConf();
    this.metrics = new MetricsSink();
    this.walEntrySinkFilter = setupWALEntrySinkFilter();
    String className = conf.get("hbase.replication.source.fs.conf.provider",
      DefaultSourceFSConfigurationProvider.class.getCanonicalName());
    try {
      Class<? extends SourceFSConfigurationProvider> c =
        Class.forName(className).asSubclass(SourceFSConfigurationProvider.class);
      this.provider = c.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "Configured source fs configuration provider class " + className + " throws error.", e);
    }
  }

  private WALEntrySinkFilter setupWALEntrySinkFilter() throws IOException {
    Class<?> walEntryFilterClass =
      this.conf.getClass(WALEntrySinkFilter.WAL_ENTRY_FILTER_KEY, null);
    WALEntrySinkFilter filter = null;
    try {
      filter = walEntryFilterClass == null
        ? null
        : (WALEntrySinkFilter) walEntryFilterClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Failed to instantiate " + walEntryFilterClass);
    }
    if (filter != null) {
      filter.init(getConnection());
    }
    return filter;
  }

  /**
   * decorate the Configuration object to make replication more receptive to delays: lessen the
   * timeout and numTries.
   */
  private void decorateConf() {
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      this.conf.getInt("replication.sink.client.retries.number", 4));
    this.conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      this.conf.getInt("replication.sink.client.ops.timeout", 10000));
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
    // use server ZK cluster for replication, so we unset the client ZK related properties if any
    if (this.conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM) != null) {
      this.conf.unset(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    }
  }

  /**
   * Replicate this array of entries directly into the local cluster using the native client. Only
   * operates against raw protobuf type saving on a conversion from pb to pojo.
   * @param entries                    WAL entries to be replicated.
   * @param cells                      cell scanner for iteration.
   * @param replicationClusterId       Id which will uniquely identify source cluster FS client
   *                                   configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *                                   directory
   * @param sourceHFileArchiveDirPath  Path that point to the source cluster hfile archive directory
   * @throws IOException If failed to replicate the data
   */
  public void replicateEntries(List<WALEntry> entries, final ExtendedCellScanner cells,
    String replicationClusterId, String sourceBaseNamespaceDirPath,
    String sourceHFileArchiveDirPath) throws IOException {
    if (entries.isEmpty()) {
      return;
    }
    // Very simple optimization where we batch sequences of rows going
    // to the same table.
    try {
      long totalReplicated = 0;
      // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
      // invocation of this method per table and cluster id.
      Map<TableName, Map<List<UUID>, List<Row>>> rowMap = new TreeMap<>();

      Map<List<String>, Map<String, List<Pair<byte[], List<String>>>>> bulkLoadsPerClusters = null;
      Pair<List<Mutation>, List<WALEntry>> mutationsToWalEntriesPairs =
        new Pair<>(new ArrayList<>(), new ArrayList<>());
      for (WALEntry entry : entries) {
        TableName table = TableName.valueOf(entry.getKey().getTableName().toByteArray());
        if (this.walEntrySinkFilter != null) {
          if (this.walEntrySinkFilter.filter(table, entry.getKey().getWriteTime())) {
            // Skip Cells in CellScanner associated with this entry.
            int count = entry.getAssociatedCellCount();
            for (int i = 0; i < count; i++) {
              // Throw index out of bounds if our cell count is off
              if (!cells.advance()) {
                this.metrics.incrementFailedBatches();
                throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
              }
            }
            continue;
          }
        }
        ExtendedCell previousCell = null;
        Mutation mutation = null;
        int count = entry.getAssociatedCellCount();
        for (int i = 0; i < count; i++) {
          // Throw index out of bounds if our cell count is off
          if (!cells.advance()) {
            this.metrics.incrementFailedBatches();
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }
          ExtendedCell cell = cells.current();
          // Handle bulk load hfiles replication
          if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
            BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
            if (bld.getReplicate()) {
              if (bulkLoadsPerClusters == null) {
                bulkLoadsPerClusters = new HashMap<>();
              }
              // Map of table name Vs list of pair of family and list of
              // hfile paths from its namespace

              List<String> clusterIds = entry.getKey().getClusterIdsList().stream()
                .map(k -> toUUID(k).toString()).collect(Collectors.toList());
              Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap =
                bulkLoadsPerClusters.computeIfAbsent(clusterIds, k -> new HashMap<>());
              buildBulkLoadHFileMap(bulkLoadHFileMap, table, bld);
            }
          } else if (CellUtil.matchingQualifier(cell, WALEdit.REPLICATION_MARKER)) {
            Mutation put = processReplicationMarkerEntry(cell);
            if (put == null) {
              continue;
            }
            table = REPLICATION_SINK_TRACKER_TABLE_NAME;
            List<UUID> clusterIds = new ArrayList<>();
            for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
              clusterIds.add(toUUID(clusterId));
            }
            put.setClusterIds(clusterIds);
            addToHashMultiMap(rowMap, table, clusterIds, put);
          } else {
            // Handle wal replication
            if (isNewRowOrType(previousCell, cell)) {
              // Create new mutation
              mutation = CellUtil.isDelete(cell)
                ? new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                : new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
              List<UUID> clusterIds = new ArrayList<>(entry.getKey().getClusterIdsList().size());
              for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
                clusterIds.add(toUUID(clusterId));
              }
              mutation.setClusterIds(clusterIds);
              mutation.setAttribute(ReplicationUtils.REPLICATION_ATTR_NAME,
                HConstants.EMPTY_BYTE_ARRAY);
              if (rsServerHost != null) {
                rsServerHost.preReplicationSinkBatchMutate(entry, mutation);
                mutationsToWalEntriesPairs.getFirst().add(mutation);
                mutationsToWalEntriesPairs.getSecond().add(entry);
              }
              addToHashMultiMap(rowMap, table, clusterIds, mutation);
            }
            if (CellUtil.isDelete(cell)) {
              ((Delete) mutation).add(cell);
            } else {
              ((Put) mutation).add(cell);
            }
            previousCell = cell;
          }
        }
        totalReplicated++;
      }

      // TODO Replicating mutations and bulk loaded data can be made parallel
      if (!rowMap.isEmpty()) {
        LOG.debug("Started replicating mutations.");
        for (Entry<TableName, Map<List<UUID>, List<Row>>> entry : rowMap.entrySet()) {
          batch(entry.getKey(), entry.getValue().values(), rowSizeWarnThreshold);
        }
        LOG.debug("Finished replicating mutations.");
      }

      if (rsServerHost != null) {
        List<Mutation> mutations = mutationsToWalEntriesPairs.getFirst();
        List<WALEntry> walEntries = mutationsToWalEntriesPairs.getSecond();
        for (int i = 0; i < mutations.size(); i++) {
          rsServerHost.postReplicationSinkBatchMutate(walEntries.get(i), mutations.get(i));
        }
      }

      if (bulkLoadsPerClusters != null) {
        for (Entry<List<String>,
          Map<String, List<Pair<byte[], List<String>>>>> entry : bulkLoadsPerClusters.entrySet()) {
          Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap = entry.getValue();
          if (bulkLoadHFileMap != null && !bulkLoadHFileMap.isEmpty()) {
            LOG.debug("Replicating {} bulk loaded data", entry.getKey().toString());
            Configuration providerConf = this.provider.getConf(this.conf, replicationClusterId);
            try (HFileReplicator hFileReplicator = new HFileReplicator(providerConf,
              sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath, bulkLoadHFileMap, conf,
              getConnection(), entry.getKey())) {
              hFileReplicator.replicate();
              LOG.debug("Finished replicating {} bulk loaded data", entry.getKey().toString());
            }
          }
        }
      }

      int size = entries.size();
      this.metrics.setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
      this.metrics.applyBatch(size + hfilesReplicated, hfilesReplicated);
      this.totalReplicatedEdits.addAndGet(totalReplicated);
    } catch (IOException ex) {
      LOG.error("Unable to accept edit because:", ex);
      this.metrics.incrementFailedBatches();
      throw ex;
    }
  }

  /*
   * First check if config key hbase.regionserver.replication.sink.tracker.enabled is true or not.
   * If false, then ignore this cell. If set to true, de-serialize value into
   * ReplicationTrackerDescriptor. Create a Put mutation with regionserver name, walname, offset and
   * timestamp from ReplicationMarkerDescriptor.
   */
  private Put processReplicationMarkerEntry(Cell cell) throws IOException {
    // If source is emitting replication marker rows but sink is not accepting them,
    // ignore the edits.
    if (!replicationSinkTrackerEnabled) {
      return null;
    }
    WALProtos.ReplicationMarkerDescriptor descriptor =
      WALProtos.ReplicationMarkerDescriptor.parseFrom(new ByteArrayInputStream(cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    Put put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, RS_COLUMN, cell.getTimestamp(),
      (Bytes.toBytes(descriptor.getRegionServerName())));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, WAL_NAME_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(descriptor.getWalName()));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, TIMESTAMP_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(cell.getTimestamp()));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, OFFSET_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(descriptor.getOffset()));
    return put;
  }

  private void buildBulkLoadHFileMap(
    final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, TableName table,
    BulkLoadDescriptor bld) throws IOException {
    List<StoreDescriptor> storesList = bld.getStoresList();
    int storesSize = storesList.size();
    for (int j = 0; j < storesSize; j++) {
      StoreDescriptor storeDescriptor = storesList.get(j);
      List<String> storeFileList = storeDescriptor.getStoreFileList();
      int storeFilesSize = storeFileList.size();
      hfilesReplicated += storeFilesSize;
      for (int k = 0; k < storeFilesSize; k++) {
        byte[] family = storeDescriptor.getFamilyName().toByteArray();

        // Build hfile relative path from its namespace
        String pathToHfileFromNS = getHFilePath(table, bld, storeFileList.get(k), family);
        String tableName = table.getNameWithNamespaceInclAsString();
        List<Pair<byte[], List<String>>> familyHFilePathsList = bulkLoadHFileMap.get(tableName);
        if (familyHFilePathsList != null) {
          boolean foundFamily = false;
          for (Pair<byte[], List<String>> familyHFilePathsPair : familyHFilePathsList) {
            if (Bytes.equals(familyHFilePathsPair.getFirst(), family)) {
              // Found family already present, just add the path to the existing list
              familyHFilePathsPair.getSecond().add(pathToHfileFromNS);
              foundFamily = true;
              break;
            }
          }
          if (!foundFamily) {
            // Family not found, add this family and its hfile paths pair to the list
            addFamilyAndItsHFilePathToTableInMap(family, pathToHfileFromNS, familyHFilePathsList);
          }
        } else {
          // Add this table entry into the map
          addNewTableEntryInMap(bulkLoadHFileMap, family, pathToHfileFromNS, tableName);
        }
      }
    }
  }

  private void addFamilyAndItsHFilePathToTableInMap(byte[] family, String pathToHfileFromNS,
    List<Pair<byte[], List<String>>> familyHFilePathsList) {
    List<String> hfilePaths = new ArrayList<>(1);
    hfilePaths.add(pathToHfileFromNS);
    familyHFilePathsList.add(new Pair<>(family, hfilePaths));
  }

  private void addNewTableEntryInMap(
    final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, byte[] family,
    String pathToHfileFromNS, String tableName) {
    List<String> hfilePaths = new ArrayList<>(1);
    hfilePaths.add(pathToHfileFromNS);
    Pair<byte[], List<String>> newFamilyHFilePathsPair = new Pair<>(family, hfilePaths);
    List<Pair<byte[], List<String>>> newFamilyHFilePathsList = new ArrayList<>();
    newFamilyHFilePathsList.add(newFamilyHFilePathsPair);
    bulkLoadHFileMap.put(tableName, newFamilyHFilePathsList);
  }

  private String getHFilePath(TableName table, BulkLoadDescriptor bld, String storeFile,
    byte[] family) {
    return new StringBuilder(100).append(table.getNamespaceAsString()).append(Path.SEPARATOR)
      .append(table.getQualifierAsString()).append(Path.SEPARATOR)
      .append(Bytes.toString(bld.getEncodedRegionName().toByteArray())).append(Path.SEPARATOR)
      .append(Bytes.toString(family)).append(Path.SEPARATOR).append(storeFile).toString();
  }

  /** Returns True if we have crossed over onto a new row or type */
  private boolean isNewRowOrType(final ExtendedCell previousCell, final ExtendedCell cell) {
    return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
      || !CellUtil.matchingRows(previousCell, cell);
  }

  private java.util.UUID toUUID(final HBaseProtos.UUID uuid) {
    return new java.util.UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
  }

  /**
   * Simple helper to a map from key to (a list of) values TODO: Make a general utility method
   * @return the list of values corresponding to key1 and key2
   */
  private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2, List<V>>> map, K1 key1, K2 key2,
    V value) {
    Map<K2, List<V>> innerMap = map.computeIfAbsent(key1, k -> new HashMap<>());
    List<V> values = innerMap.computeIfAbsent(key2, k -> new ArrayList<>());
    values.add(value);
    return values;
  }

  /**
   * stop the thread pool executor. It is called when the regionserver is stopped.
   */
  public void stopReplicationSinkServices() {
    try {
      if (this.sharedConn != null) {
        synchronized (sharedConnLock) {
          if (this.sharedConn != null) {
            this.sharedConn.close();
            this.sharedConn = null;
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("IOException while closing the connection", e); // ignoring as we are closing.
    }
  }

  /**
   * Do the changes and handle the pool
   * @param tableName             table to insert into
   * @param allRows               list of actions
   * @param batchRowSizeThreshold rowSize threshold for batch mutation
   */
  private void batch(TableName tableName, Collection<List<Row>> allRows, int batchRowSizeThreshold)
    throws IOException {
    if (allRows.isEmpty()) {
      return;
    }
    AsyncTable<?> table = getConnection().getTable(tableName);
    List<Future<?>> futures = new ArrayList<>();
    for (List<Row> rows : allRows) {
      List<List<Row>> batchRows;
      if (rows.size() > batchRowSizeThreshold) {
        batchRows = Lists.partition(rows, batchRowSizeThreshold);
      } else {
        batchRows = Collections.singletonList(rows);
      }
      futures.addAll(batchRows.stream().map(table::batchAll).collect(Collectors.toList()));
    }
    // Here we will always wait until all futures are finished, even if there are failures when
    // getting from a future in the middle. This is because this method may be called in a rpc call,
    // so the batch operations may reference some off heap cells(through CellScanner). If we return
    // earlier here, the rpc call may be finished and they will release the off heap cells before
    // some of the batch operations finish, and then cause corrupt data or even crash the region
    // server. See HBASE-28584 and HBASE-28850 for more details.
    IOException error = null;
    for (Future<?> future : futures) {
      try {
        FutureUtils.get(future);
      } catch (RetriesExhaustedException e) {
        IOException ioe;
        if (e.getCause() instanceof TableNotFoundException) {
          ioe = new TableNotFoundException("'" + tableName + "'");
        } else {
          ioe = e;
        }
        if (error == null) {
          error = ioe;
        } else {
          error.addSuppressed(ioe);
        }
      }
    }
    if (error != null) {
      throw error;
    }
  }

  private AsyncClusterConnection getConnection() throws IOException {
    // See https://en.wikipedia.org/wiki/Double-checked_locking
    AsyncClusterConnection connection = sharedConn;
    if (connection == null) {
      synchronized (sharedConnLock) {
        connection = sharedConn;
        if (connection == null) {
          connection = ClusterConnectionFactory.createAsyncClusterConnection(conf, null,
            UserProvider.instantiate(conf).getCurrent());
          sharedConn = connection;
        }
      }
    }
    return connection;
  }

  /**
   * Get a string representation of this sink's metrics
   * @return string with the total replicated edits count and the date of the last edit that was
   *         applied
   */
  public String getStats() {
    long total = this.totalReplicatedEdits.get();
    return total == 0
      ? ""
      : "Sink: " + "age in ms of last applied edit: " + this.metrics.refreshAgeOfLastAppliedOp()
        + ", total replicated edits: " + total;
  }

  /**
   * Get replication Sink Metrics
   */
  public MetricsSink getSinkMetrics() {
    return this.metrics;
  }
}
