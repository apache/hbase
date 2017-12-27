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
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Emits sorted Cells.
 * Reads in all Cells from passed Iterator, sorts them, then emits
 * Cells in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat2
 */
@InterfaceAudience.Public
public class CellSortReducer
    extends Reducer<ImmutableBytesWritable, Cell, ImmutableBytesWritable, Cell> {
  protected void reduce(ImmutableBytesWritable row, Iterable<Cell> kvs,
      Reducer<ImmutableBytesWritable, Cell, ImmutableBytesWritable, Cell>.Context context)
  throws java.io.IOException, InterruptedException {
    TreeSet<Cell> map = new TreeSet<>(CellComparator.getInstance());
    for (Cell kv : kvs) {
      try {
        map.add(PrivateCellUtil.deepClone(kv));
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }
    }
    context.setStatus("Read " + map.getClass());
    int index = 0;
    for (Cell kv: map) {
      context.write(row, new MapReduceExtendedCell(kv));
      if (++index % 100 == 0) context.setStatus("Wrote " + index);
    }
  }
}
