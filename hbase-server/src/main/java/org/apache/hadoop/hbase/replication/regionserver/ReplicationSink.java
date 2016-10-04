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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * <p>
 * This class is responsible for replicating the edits coming
 * from another cluster.
 * </p><p>
 * This replication process is currently waiting for the edits to be applied
 * before the method can return. This means that the replication of edits
 * is synchronized (after reading from WALs in ReplicationSource) and that a
 * single region server cannot receive edits from two sources at the same time
 * </p><p>
 * This class uses the native HBase client in order to replicate entries.
 * </p>
 *
 * TODO make this class more like ReplicationSource wrt log handling
 */
@InterfaceAudience.Private
public class ReplicationSink {

  private static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  private final Configuration conf;
  // Volatile because of note in here -- look for double-checked locking:
  // http://www.oracle.com/technetwork/articles/javase/bloch-effective-08-qa-140880.html
  private volatile Connection sharedHtableCon;
  private final MetricsSink metrics;
  private final AtomicLong totalReplicatedEdits = new AtomicLong();
  private final Object sharedHtableConLock = new Object();
  // Number of hfiles that we successfully replicated
  private long hfilesReplicated = 0;
  private SourceFSConfigurationProvider provider;

  /**
   * Create a sink for replication
   *
   * @param conf                conf object
   * @param stopper             boolean to tell this thread to stop
   * @throws IOException thrown when HDFS goes bad or bad file name
   */
  public ReplicationSink(Configuration conf, Stoppable stopper)
      throws IOException {
    this.conf = HBaseConfiguration.create(conf);
    decorateConf();
    this.metrics = new MetricsSink();

    String className =
        conf.get("hbase.replication.source.fs.conf.provider",
          DefaultSourceFSConfigurationProvider.class.getCanonicalName());
    try {
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(className);
      this.provider = (SourceFSConfigurationProvider) c.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Configured source fs configuration provider class "
          + className + " throws error.", e);
    }
  }

  /**
   * decorate the Configuration object to make replication more receptive to delays:
   * lessen the timeout and numTries.
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
   }

  /**
   * Replicate this array of entries directly into the local cluster using the native client. Only
   * operates against raw protobuf type saving on a conversion from pb to pojo.
   * @param entries
   * @param cells
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *          directory
   * @param sourceHFileArchiveDirPath Path that point to the source cluster hfile archive directory
   * @throws IOException If failed to replicate the data
   */
  public void replicateEntries(List<WALEntry> entries, final CellScanner cells,
      String replicationClusterId, String sourceBaseNamespaceDirPath,
      String sourceHFileArchiveDirPath) throws IOException {
    if (entries.isEmpty()) return;
    if (cells == null) throw new NullPointerException("TODO: Add handling of null CellScanner");
    // Very simple optimization where we batch sequences of rows going
    // to the same table.
    try {
      long totalReplicated = 0;
      // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
      // invocation of this method per table and cluster id.
      Map<TableName, Map<List<UUID>, List<Row>>> rowMap =
          new TreeMap<TableName, Map<List<UUID>, List<Row>>>();

      // Map of table name Vs list of pair of family and list of hfile paths from its namespace
      Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap = null;

      for (WALEntry entry : entries) {
        TableName table =
            TableName.valueOf(entry.getKey().getTableName().toByteArray());
        Cell previousCell = null;
        Mutation m = null;
        int count = entry.getAssociatedCellCount();
        for (int i = 0; i < count; i++) {
          // Throw index out of bounds if our cell count is off
          if (!cells.advance()) {
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }
          Cell cell = cells.current();
          // Handle bulk load hfiles replication
          if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
            if (bulkLoadHFileMap == null) {
              bulkLoadHFileMap = new HashMap<String, List<Pair<byte[], List<String>>>>();
            }
            buildBulkLoadHFileMap(bulkLoadHFileMap, table, cell);
          } else {
            // Handle wal replication
            if (isNewRowOrType(previousCell, cell)) {
              // Create new mutation
              m =
                  CellUtil.isDelete(cell) ? new Delete(cell.getRowArray(), cell.getRowOffset(),
                      cell.getRowLength()) : new Put(cell.getRowArray(), cell.getRowOffset(),
                      cell.getRowLength());
              List<UUID> clusterIds = new ArrayList<UUID>();
              for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
                clusterIds.add(toUUID(clusterId));
              }
              m.setClusterIds(clusterIds);
              addToHashMultiMap(rowMap, table, clusterIds, m);
            }
            if (CellUtil.isDelete(cell)) {
              ((Delete) m).addDeleteMarker(cell);
            } else {
              ((Put) m).add(cell);
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
          batch(entry.getKey(), entry.getValue().values());
        }
        LOG.debug("Finished replicating mutations.");
      }

      if (bulkLoadHFileMap != null && !bulkLoadHFileMap.isEmpty()) {
        LOG.debug("Started replicating bulk loaded data.");
        HFileReplicator hFileReplicator =
            new HFileReplicator(this.provider.getConf(this.conf, replicationClusterId),
                sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath, bulkLoadHFileMap, conf,
                getConnection());
        hFileReplicator.replicate();
        LOG.debug("Finished replicating bulk loaded data.");
      }

      int size = entries.size();
      this.metrics.setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
      this.metrics.applyBatch(size + hfilesReplicated, hfilesReplicated);
      this.totalReplicatedEdits.addAndGet(totalReplicated);
    } catch (IOException ex) {
      LOG.error("Unable to accept edit because:", ex);
      throw ex;
    }
  }

  private void buildBulkLoadHFileMap(
      final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, TableName table,
      Cell cell) throws IOException {
    BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
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
        if (bulkLoadHFileMap.containsKey(tableName)) {
          List<Pair<byte[], List<String>>> familyHFilePathsList = bulkLoadHFileMap.get(tableName);
          boolean foundFamily = false;
          for (int i = 0; i < familyHFilePathsList.size(); i++) {
            Pair<byte[], List<String>> familyHFilePathsPair = familyHFilePathsList.get(i);
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
    List<String> hfilePaths = new ArrayList<String>();
    hfilePaths.add(pathToHfileFromNS);
    familyHFilePathsList.add(new Pair<byte[], List<String>>(family, hfilePaths));
  }

  private void addNewTableEntryInMap(
      final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, byte[] family,
      String pathToHfileFromNS, String tableName) {
    List<String> hfilePaths = new ArrayList<String>();
    hfilePaths.add(pathToHfileFromNS);
    Pair<byte[], List<String>> newFamilyHFilePathsPair =
        new Pair<byte[], List<String>>(family, hfilePaths);
    List<Pair<byte[], List<String>>> newFamilyHFilePathsList =
        new ArrayList<Pair<byte[], List<String>>>();
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

  /**
   * @param previousCell
   * @param cell
   * @return True if we have crossed over onto a new row or type
   */
  private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
    return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte() ||
        !CellUtil.matchingRow(previousCell, cell);
  }

  private java.util.UUID toUUID(final HBaseProtos.UUID uuid) {
    return new java.util.UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
  }

  /**
   * Simple helper to a map from key to (a list of) values
   * TODO: Make a general utility method
   * @param map
   * @param key1
   * @param key2
   * @param value
   * @return the list of values corresponding to key1 and key2
   */
  private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2,List<V>>> map, K1 key1, K2 key2, V value) {
    Map<K2,List<V>> innerMap = map.get(key1);
    if (innerMap == null) {
      innerMap = new HashMap<K2, List<V>>();
      map.put(key1, innerMap);
    }
    List<V> values = innerMap.get(key2);
    if (values == null) {
      values = new ArrayList<V>();
      innerMap.put(key2, values);
    }
    values.add(value);
    return values;
  }

  /**
   * stop the thread pool executor. It is called when the regionserver is stopped.
   */
  public void stopReplicationSinkServices() {
    try {
      if (this.sharedHtableCon != null) {
        synchronized (sharedHtableConLock) {
          if (this.sharedHtableCon != null) {
            this.sharedHtableCon.close();
            this.sharedHtableCon = null;
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("IOException while closing the connection", e); // ignoring as we are closing.
    }
  }


  /**
   * Do the changes and handle the pool
   * @param tableName table to insert into
   * @param allRows list of actions
   * @throws IOException
   */
  protected void batch(TableName tableName, Collection<List<Row>> allRows) throws IOException {
    if (allRows.isEmpty()) {
      return;
    }
    Table table = null;
    try {
      Connection connection = getConnection();
      table = connection.getTable(tableName);
      for (List<Row> rows : allRows) {
        table.batch(rows, null);
      }
    } catch (InterruptedException ix) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(ix);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private Connection getConnection() throws IOException {
    // See https://en.wikipedia.org/wiki/Double-checked_locking
    Connection connection = sharedHtableCon;
    if (connection == null) {
      synchronized (sharedHtableConLock) {
        connection = sharedHtableCon;
        if (connection == null) {
          connection = sharedHtableCon = ConnectionFactory.createConnection(conf);
        }
      }
    }
    return connection;
  }

  /**
   * Get a string representation of this sink's metrics
   * @return string with the total replicated edits count and the date
   * of the last edit that was applied
   */
  public String getStats() {
    return this.totalReplicatedEdits.get() == 0 ? "" : "Sink: " +
      "age in ms of last applied edit: " + this.metrics.refreshAgeOfLastAppliedOp() +
      ", total replicated edits: " + this.totalReplicatedEdits;
  }

  /**
   * Get replication Sink Metrics
   * @return MetricsSink
   */
  public MetricsSink getSinkMetrics() {
    return this.metrics;
  }
}
