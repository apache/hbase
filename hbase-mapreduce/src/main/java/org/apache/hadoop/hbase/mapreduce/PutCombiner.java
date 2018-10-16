/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combine Puts. Merges Put instances grouped by <code>K</code> into a single
 * instance.
 * @see TableMapReduceUtil
 */
@InterfaceAudience.Public
public class PutCombiner<K> extends Reducer<K, Put, K, Put> {
  private static final Logger LOG = LoggerFactory.getLogger(PutCombiner.class);

  @Override
  protected void reduce(K row, Iterable<Put> vals, Context context)
      throws IOException, InterruptedException {
    // Using HeapSize to create an upper bound on the memory size of
    // the puts and flush some portion of the content while looping. This
    // flush could result in multiple Puts for a single rowkey. That is
    // acceptable because Combiner is run as an optimization and it's not
    // critical that all Puts are grouped perfectly.
    long threshold = context.getConfiguration().getLong(
        "putcombiner.row.threshold", 1L * (1<<30));
    int cnt = 0;
    long curSize = 0;
    Put put = null;
    Map<byte[], List<Cell>> familyMap = null;
    for (Put p : vals) {
      cnt++;
      if (put == null) {
        put = p;
        familyMap = put.getFamilyCellMap();
      } else {
        for (Entry<byte[], List<Cell>> entry : p.getFamilyCellMap()
            .entrySet()) {
          List<Cell> cells = familyMap.get(entry.getKey());
          List<Cell> kvs = (cells != null) ? (List<Cell>) cells : null;
          for (Cell cell : entry.getValue()) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            curSize += kv.heapSize();
            if (kvs != null) {
              kvs.add(kv);
            }
          }
          if (cells == null) {
            familyMap.put(entry.getKey(), entry.getValue());
          }
        }
        if (cnt % 10 == 0) context.setStatus("Combine " + cnt);
        if (curSize > threshold) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Combined %d Put(s) into %d.", cnt, 1));
          }
          context.write(row, put);
          put = null;
          curSize = 0;
          cnt = 0;
        }
      }
    }
    if (put != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Combined %d Put(s) into %d.", cnt, 1));
      }
      context.write(row, put);
    }
  }
}
