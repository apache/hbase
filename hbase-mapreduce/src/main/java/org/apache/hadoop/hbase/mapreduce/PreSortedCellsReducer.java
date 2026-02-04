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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PreSortedCellsReducer
  extends Reducer<KeyOnlyCellComparable, Cell, ImmutableBytesWritable, Cell> {

  @Override
  protected void reduce(KeyOnlyCellComparable keyComparable, Iterable<Cell> values, Context context)
    throws IOException, InterruptedException {

    int index = 0;
    ImmutableBytesWritable key =
      new ImmutableBytesWritable(CellUtil.cloneRow(keyComparable.getCell()));
    for (Cell cell : values) {
      context.write(key, new MapReduceExtendedCell(PrivateCellUtil.ensureExtendedCell(cell)));
      if (++index % 100 == 0) {
        context.setStatus("Wrote " + index + " cells");
      }
    }
  }
}
