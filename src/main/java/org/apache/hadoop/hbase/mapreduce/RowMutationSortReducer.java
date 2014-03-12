/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

/**
 * Emits sorted Puts and Deletes.
 * Reads in all Puts and Deletes from passed Iterator (over RowMutation elements), sorts them, then emits
 * KeyValue items in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat
 * @see KeyValueSortReducer
 */
public class RowMutationSortReducer extends
    Reducer<ImmutableBytesWritable, RowMutation, ImmutableBytesWritable, KeyValue> {
  
  @Override
  protected void reduce(
      ImmutableBytesWritable row,
      java.lang.Iterable<RowMutation> requests,
      Reducer<ImmutableBytesWritable, RowMutation,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    // although reduce() is called per-row, handle pathological case
    long threshold = context.getConfiguration().getLong(
        "rowmutationsortreducer.row.threshold", 2L * (1<<30));
    Iterator<RowMutation> iter = requests.iterator();
    while (iter.hasNext()) {
      TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      long curSize = 0;
      // stop at the end or the RAM threshold
      while (iter.hasNext() && curSize < threshold) {
        RowMutation rm = iter.next();
        Row r = rm.getInstance();
        Map< byte[], List<KeyValue> > familyMap;
        if (r instanceof Put) {
          familyMap = ((Put) r).getFamilyMap();
        } else if (r instanceof Delete) {
          familyMap = ((Delete) r).getFamilyMap();
        } else {
          familyMap = null;
        }

        if (null != familyMap) {
          for (List<KeyValue> kvs : familyMap.values()) {
            for (KeyValue kv : kvs) {
              kv.setMemstoreTS(rm.getOrderNumber());
              map.add(kv);
              curSize += kv.getValueLength();
            }
          }
        }
      }
      context.setStatus("Read " + map.size() + " entries of " + map.getClass()
          + "(" + StringUtils.humanReadableInt(curSize) + ")");
      int index = 0;
      for (KeyValue kv : map) {
        // Set memstore timestamp to zero so that the data is immediately visible to all clients
        // regardless of the target regionserver's readpoint.
        KeyValue kvCopy = kv.shallowCopy();
        kvCopy.setMemstoreTS(0);

        context.write(row, kvCopy);
        if (index > 0 && index % 100 == 0)
          context.setStatus("Wrote " + index);
      }

      // if we have more entries to process
      if (iter.hasNext()) {
        // force flush because we cannot guarantee intra-row sorted order
        context.write(null, null);
      }
    }
  }
}
