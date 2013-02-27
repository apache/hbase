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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combine Puts. Merges Put instances grouped by <code>K</code> into a single
 * instance.
 * @see TableMapReduceUtil
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PutCombiner<K> extends Reducer<K, Put, K, Put> {
  private static final Log LOG = LogFactory.getLog(PutCombiner.class);

  @Override
  protected void reduce(K row, Iterable<Put> vals, Context context)
      throws IOException, InterruptedException {

    int cnt = 0;
    // There's nothing to say <code>K row</code> is the same as the rowkey
    // used to construct Puts (value) instances. Thus the map of put.getRow()
    // to combined Put is necessary.
    // TODO: would be better if we knew <code>K row</code> and Put rowkey were
    // identical. Then this whole Put buffering business goes away.
    // TODO: Could use HeapSize to create an upper bound on the memory size of
    // the puts map and flush some portion of the content while looping. This
    // flush could result in multiple Puts for a single rowkey. That is
    // acceptable because Combiner is run as an optimization and it's not
    // critical that all Puts are grouped perfectly.
    Map<byte[], Put> puts = new HashMap<byte[], Put>();
    for (Put p : vals) {
      cnt++;
      if (!puts.containsKey(p.getRow())) {
        puts.put(p.getRow(), p);
      } else {
        puts.get(p.getRow()).getFamilyMap().putAll(p.getFamilyMap());
      }
    }

    for (Put p : puts.values()) {
      context.write(row, p);
    }
    LOG.info(String.format("Combined %d Put(s) into %d.", cnt, puts.size()));
  }
}
