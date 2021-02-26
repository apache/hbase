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

import java.util.TreeSet;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Emits sorted KeyValues.
 * Reads in all KeyValues from passed Iterator, sorts them, then emits
 * KeyValues in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat2
 * @deprecated Use {@link CellSortReducer}. Will be removed from
 * 3.0 onwards
 */
@Deprecated
@InterfaceAudience.Public
public class KeyValueSortReducer
    extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
  protected void reduce(ImmutableBytesWritable row, Iterable<KeyValue> kvs,
      Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue>.Context context)
  throws java.io.IOException, InterruptedException {
    TreeSet<KeyValue> map = new TreeSet<>(CellComparatorImpl.COMPARATOR);
    for (KeyValue kv: kvs) {
      try {
        map.add(kv.clone());
      } catch (CloneNotSupportedException e) {
        throw new java.io.IOException(e);
      }
    }
    context.setStatus("Read " + map.getClass());
    int index = 0;
    for (KeyValue kv: map) {
      context.write(row, kv);
      if (++index % 100 == 0) context.setStatus("Wrote " + index);
    }
  }
}
