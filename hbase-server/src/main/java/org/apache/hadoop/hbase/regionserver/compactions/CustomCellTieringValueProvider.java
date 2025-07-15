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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An extension of DateTieredCompactor, overriding the decorateCells method to allow for custom
 * values to be used for the different file tiers during compaction.
 */
@InterfaceAudience.Private
public class CustomCellTieringValueProvider implements CustomTieredCompactor.TieringValueProvider {
  public static final String TIERING_CELL_QUALIFIER = "TIERING_CELL_QUALIFIER";
  private byte[] tieringQualifier;

  @Override
  public void init(Configuration conf) throws Exception {
    tieringQualifier = Bytes.toBytes(conf.get(TIERING_CELL_QUALIFIER));
  }

  @Override
  public List<Cell> decorateCells(List<Cell> cells) {
    // if no tiering qualifier properly set, skips the whole flow
    if (tieringQualifier != null) {
      byte[] tieringValue = null;
      // first iterates through the cells within a row, to find the tiering value for the row
      for (Cell cell : cells) {
        if (CellUtil.matchingQualifier(cell, tieringQualifier)) {
          tieringValue = CellUtil.cloneValue(cell);
          break;
        }
      }
      if (tieringValue == null) {
        tieringValue = Bytes.toBytes(Long.MAX_VALUE);
      }
      // now apply the tiering value as a tag to all cells within the row
      Tag tieringValueTag = new ArrayBackedTag(TagType.CELL_VALUE_TIERING_TAG_TYPE, tieringValue);
      List<Cell> newCells = new ArrayList<>(cells.size());
      for (Cell cell : cells) {
        List<Tag> tags = PrivateCellUtil.getTags(cell);
        tags.add(tieringValueTag);
        newCells.add(PrivateCellUtil.createCell(cell, tags));
      }
      return newCells;
    } else {
      return cells;
    }
  }

  @Override
  public long getTieringValue(Cell cell) {
    Optional<Tag> tagOptional = PrivateCellUtil.getTag(cell, TagType.CELL_VALUE_TIERING_TAG_TYPE);
    if (tagOptional.isPresent()) {
      Tag tag = tagOptional.get();
      return Bytes.toLong(tag.getValueByteBuffer().array(), tag.getValueOffset(),
        tag.getValueLength());
    }
    return Long.MAX_VALUE;
  }
}
