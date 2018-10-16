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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

/**
 * Emits sorted Puts.
 * Reads in all Puts from passed Iterator, sorts them, then emits
 * Puts in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat2
 * @see CellSortReducer
 */
@InterfaceAudience.Public
public class PutSortReducer extends
    Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue> {
  // the cell creator
  private CellCreator kvCreator;

  @Override
  protected void
      setup(Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue>.Context context)
          throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    this.kvCreator = new CellCreator(conf);
  }

  @Override
  protected void reduce(
      ImmutableBytesWritable row,
      java.lang.Iterable<Put> puts,
      Reducer<ImmutableBytesWritable, Put,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    // although reduce() is called per-row, handle pathological case
    long threshold = context.getConfiguration().getLong(
        "putsortreducer.row.threshold", 1L * (1<<30));
    Iterator<Put> iter = puts.iterator();
    while (iter.hasNext()) {
      TreeSet<KeyValue> map = new TreeSet<>(CellComparator.getInstance());
      long curSize = 0;
      // stop at the end or the RAM threshold
      List<Tag> tags = new ArrayList<>();
      while (iter.hasNext() && curSize < threshold) {
        // clear the tags
        tags.clear();
        Put p = iter.next();
        long t = p.getTTL();
        if (t != Long.MAX_VALUE) {
          // add TTL tag if found
          tags.add(new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(t)));
        }
        byte[] acl = p.getACL();
        if (acl != null) {
          // add ACL tag if found
          tags.add(new ArrayBackedTag(TagType.ACL_TAG_TYPE, acl));
        }
        try {
          CellVisibility cellVisibility = p.getCellVisibility();
          if (cellVisibility != null) {
            // add the visibility labels if any
            tags.addAll(kvCreator.getVisibilityExpressionResolver()
                .createVisibilityExpTags(cellVisibility.getExpression()));
          }
        } catch (DeserializationException e) {
          // We just throw exception here. Should we allow other mutations to proceed by
          // just ignoring the bad one?
          throw new IOException("Invalid visibility expression found in mutation " + p, e);
        }
        for (List<Cell> cells: p.getFamilyCellMap().values()) {
          for (Cell cell: cells) {
            // Creating the KV which needs to be directly written to HFiles. Using the Facade
            // KVCreator for creation of kvs.
            KeyValue kv = null;
            TagUtil.carryForwardTags(tags, cell);
            if (!tags.isEmpty()) {
              kv = (KeyValue) kvCreator.create(cell.getRowArray(), cell.getRowOffset(),
                cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(),
                cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength(), cell.getTimestamp(), cell.getValueArray(),
                cell.getValueOffset(), cell.getValueLength(), tags);
            } else {
              kv = KeyValueUtil.ensureKeyValue(cell);
            }
            if (map.add(kv)) {// don't count duplicated kv into size
              curSize += kv.heapSize();
            }
          }
        }
      }
      context.setStatus("Read " + map.size() + " entries of " + map.getClass()
          + "(" + StringUtils.humanReadableInt(curSize) + ")");
      int index = 0;
      for (KeyValue kv : map) {
        context.write(row, kv);
        if (++index % 100 == 0)
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
