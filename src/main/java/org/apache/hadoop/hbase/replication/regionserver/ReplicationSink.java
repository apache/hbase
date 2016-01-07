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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Stoppable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * This class is responsible for replicating the edits coming
 * from another cluster.
 * <p/>
 * This replication process is currently waiting for the edits to be applied
 * before the method can return. This means that the replication of edits
 * is synchronized (after reading from HLogs in ReplicationSource) and that a
 * single region server cannot receive edits from two sources at the same time
 * <p/>
 * This class uses the native HBase client in order to replicate entries.
 * <p/>
 *
 * TODO make this class more like ReplicationSource wrt log handling
 */
public class ReplicationSink {

  private static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  // Name of the HDFS directory that contains the temporary rep logs
  public static final String REPLICATION_LOG_DIR = ".replogs";
  private final Configuration conf;
  private final HConnection sharedHtableCon;
  private final ReplicationSinkMetrics metrics;

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
    this.sharedHtableCon = HConnectionManager.createConnection(this.conf);
    this.metrics = new ReplicationSinkMetrics();
  }

  /**
   * decorate the Configuration object to make replication more receptive to
   * delays: lessen the timeout and numTries.
   */
  private void decorateConf() {
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        this.conf.getInt("replication.sink.client.retries.number", 4));
    this.conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        this.conf.getInt("replication.sink.client.ops.timeout", 10000));
  }

  /**
   * Replicate this array of entries directly into the local cluster
   * using the native client.
   *
   * @param entries
   * @throws IOException
   */
  public void replicateEntries(HLog.Entry[] entries)
      throws IOException {
    if (entries.length == 0) {
      return;
    }
    // Very simple optimization where we batch sequences of rows going
    // to the same table.
    try {
      long totalReplicated = 0;
      // Map of table => list of Rows, grouped by clusters that consumed the change, we only want to
      // flushCommits once per
      // invocation of this method per table and clusters that have consumed the change.
      Map<byte[], Map<List<UUID>, List<Row>>> rowMap =
          new TreeMap<byte[], Map<List<UUID>, List<Row>>>(Bytes.BYTES_COMPARATOR);
      for (HLog.Entry entry : entries) {
        WALEdit edit = entry.getEdit();
        byte[] table = entry.getKey().getTablename();
        Put put = null;
        Delete del = null;
        KeyValue lastKV = null;
        List<KeyValue> kvs = edit.getKeyValues();
        for (KeyValue kv : kvs) {
          if (lastKV == null || lastKV.getType() != kv.getType() || !lastKV.matchingRow(kv)) {
            UUID clusterId = entry.getKey().getClusterId();
            List<UUID> clusterIds = edit.getClusterIds();
            if (kv.isDelete()) {
              del = new Delete(kv.getRow());
              del.setClusterId(clusterId);
              del.setClusterIds(clusterIds);
              clusterIds.add(clusterId);
              addToHashMultiMap(rowMap, table, clusterIds, del);
            } else {
              put = new Put(kv.getRow());
              put.setClusterId(clusterId);
              put.setClusterIds(clusterIds);
              clusterIds.add(clusterId);
              addToHashMultiMap(rowMap, table, clusterIds, put);
            }
          }
          if (kv.isDelete()) {
            del.addDeleteMarker(kv);
          } else {
            put.add(kv);
          }
          lastKV = kv;
        }
        totalReplicated++;
      }
      for(Map.Entry<byte[], Map<List<UUID>, List<Row>>> entry : rowMap.entrySet()) {
        batch(entry.getKey(), entry.getValue().values());
      }
      this.metrics.setAgeOfLastAppliedOp(
          entries[entries.length-1].getKey().getWriteTime());
      this.metrics.appliedBatchesRate.inc(1);
      LOG.info("Total replicated: " + totalReplicated);
    } catch (IOException ex) {
      LOG.error("Unable to accept edit because:", ex);
      throw ex;
    }
  }

  /**
   * Simple helper to a map from key to (a list of) values
   * TODO: Make a general utility method
   * @param map
   * @param key1
   * @param key2
   * @param value
   * @return the list of values for the combination of key1 and key2
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
      this.sharedHtableCon.close();
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
  protected void batch(byte[] tableName, Collection<List<Row>> allRows) throws IOException {
    if (allRows.isEmpty()) {
      return;
    }
    HTableInterface table = null;
    try {
      table = this.sharedHtableCon.getTable(tableName);
      for (List<Row> rows : allRows) {
        table.batch(rows);
        this.metrics.appliedOpsRate.inc(rows.size());
      }
    } catch (InterruptedException ix) {
      throw new IOException(ix);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
}
