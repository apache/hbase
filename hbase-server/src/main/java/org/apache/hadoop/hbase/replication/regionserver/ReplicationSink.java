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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

/**
 * This class is responsible for replicating the edits coming
 * from another cluster.
 * <p/>
 * This replication process is currently waiting for the edits to be applied
 * before the method can return. This means that the replication of edits
 * is synchronized (after reading from WALs in ReplicationSource) and that a
 * single region server cannot receive edits from two sources at the same time
 * <p/>
 * This class uses the native HBase client in order to replicate entries.
 * <p/>
 *
 * TODO make this class more like ReplicationSource wrt log handling
 */
@InterfaceAudience.Private
public class ReplicationSink {

  private static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  private final Configuration conf;
  private Connection sharedHtableCon;
  private final MetricsSink metrics;
  private final AtomicLong totalReplicatedEdits = new AtomicLong();

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
   * @throws IOException
   */
  public void replicateEntries(List<WALEntry> entries, final CellScanner cells) throws IOException {
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
          if (isNewRowOrType(previousCell, cell)) {
            // Create new mutation
            m = CellUtil.isDelete(cell)?
              new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()):
              new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            List<UUID> clusterIds = new ArrayList<UUID>();
            for(HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()){
              clusterIds.add(toUUID(clusterId));
            }
            m.setClusterIds(clusterIds);
            addToHashMultiMap(rowMap, table, clusterIds, m);
          }
          if (CellUtil.isDelete(cell)) {
            ((Delete)m).addDeleteMarker(cell);
          } else {
            ((Put)m).add(cell);
          }
          previousCell = cell;
        }
        totalReplicated++;
      }
      for (Entry<TableName, Map<List<UUID>,List<Row>>> entry : rowMap.entrySet()) {
        batch(entry.getKey(), entry.getValue().values());
      }
      int size = entries.size();
      this.metrics.setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
      this.metrics.applyBatch(size);
      this.totalReplicatedEdits.addAndGet(totalReplicated);
    } catch (IOException ex) {
      LOG.error("Unable to accept edit because:", ex);
      throw ex;
    }
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
        this.sharedHtableCon.close();
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
      if (this.sharedHtableCon == null) {
        this.sharedHtableCon = ConnectionFactory.createConnection(this.conf);
      }
      table = this.sharedHtableCon.getTable(tableName);
      for (List<Row> rows : allRows) {
        table.batch(rows);
      }
    } catch (InterruptedException ix) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(ix);
    } finally {
      if (table != null) {
        table.close();
      }
    }
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
