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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.client.ClientInternalHelper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combine Puts. Merges Put instances grouped by <code>K</code> into a single instance.
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
    long threshold =
      context.getConfiguration().getLong("putcombiner.row.threshold", 1L * (1 << 30));
    int cnt = 0;
    long curSize = 0;
    Put combinedPut = null;
    Map<byte[], List<ExtendedCell>> combinedFamilyMap = null;
    for (Put p : vals) {
      cnt++;
      if (combinedPut == null) {
        combinedPut = p;
        combinedFamilyMap = ClientInternalHelper.getExtendedFamilyCellMap(combinedPut);
      } else {
        for (Entry<byte[], List<ExtendedCell>> entry : ClientInternalHelper
          .getExtendedFamilyCellMap(p).entrySet()) {
          List<ExtendedCell> existCells = combinedFamilyMap.get(entry.getKey());
          if (existCells == null) {
            // no cells for this family yet, just put it
            combinedFamilyMap.put(entry.getKey(), entry.getValue());
            // do not forget to calculate the size
            for (ExtendedCell cell : entry.getValue()) {
              curSize += cell.heapSize();
            }
          } else {
            // otherwise just add the cells to the existent list for this family
            for (ExtendedCell cell : entry.getValue()) {
              existCells.add(cell);
              curSize += cell.heapSize();
            }
          }
        }
        if (cnt % 10 == 0) {
          context.setStatus("Combine " + cnt);
        }
        if (curSize > threshold) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Combined %d Put(s) into %d.", cnt, 1));
          }
          context.write(row, combinedPut);
          combinedPut = null;
          curSize = 0;
          cnt = 0;
        }
      }
    }
    if (combinedPut != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Combined %d Put(s) into %d.", cnt, 1));
      }
      context.write(row, combinedPut);
    }
  }
}
