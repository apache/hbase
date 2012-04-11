/*
 * Copyright 2010 The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

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
@InterfaceAudience.Private
public class ReplicationSink {

  private static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  // Name of the HDFS directory that contains the temporary rep logs
  public static final String REPLICATION_LOG_DIR = ".replogs";
  private final Configuration conf;
  // Pool used to replicated
  private final HTablePool pool;
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
    this.conf = conf;
    this.pool = new HTablePool(this.conf,
        conf.getInt("replication.sink.htablepool.capacity", 10));
    this.metrics = new ReplicationSinkMetrics();
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
      // Map of table => list of Rows, we only want to flushCommits once per
      // invocation of this method per table.
      Map<byte[], List<Row>> rows = new TreeMap<byte[], List<Row>>(Bytes.BYTES_COMPARATOR);
      for (HLog.Entry entry : entries) {
        WALEdit edit = entry.getEdit();
        byte[] table = entry.getKey().getTablename();
        Put put = null;
        Delete del = null;
        KeyValue lastKV = null;
        List<KeyValue> kvs = edit.getKeyValues();
        for (KeyValue kv : kvs) {
          if (lastKV == null || lastKV.getType() != kv.getType() || !lastKV.matchingRow(kv)) {
            if (kv.isDelete()) {
              del = new Delete(kv.getRow());
              del.setClusterId(entry.getKey().getClusterId());
              addToMultiMap(rows, table, del);
            } else {
              put = new Put(kv.getRow());
              put.setClusterId(entry.getKey().getClusterId());
              addToMultiMap(rows, table, put);
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
      for (Entry<byte[], List<Row>> entry : rows.entrySet()) {
        batch(entry.getKey(), entry.getValue());
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
   * @param key
   * @param value
   * @return
   */
  private <K, V> List<V> addToMultiMap(Map<K, List<V>> map, K key, V value) {
    List<V> values = map.get(key);
    if (values == null) {
      values = new ArrayList<V>();
      map.put(key, values);
    }
    values.add(value);
    return values;
  }

  /**
   * Do the changes and handle the pool
   * @param tableName table to insert into
   * @param rows list of actions
   * @throws IOException
   */
  private void batch(byte[] tableName, List<Row> rows) throws IOException {
    if (rows.isEmpty()) {
      return;
    }
    HTableInterface table = null;
    try {
      table = this.pool.getTable(tableName);
      table.batch(rows);
      this.metrics.appliedOpsRate.inc(rows.size());
    } catch (InterruptedException ix) {
      throw new IOException(ix);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
}
