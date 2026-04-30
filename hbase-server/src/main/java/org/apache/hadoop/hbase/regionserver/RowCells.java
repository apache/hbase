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
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;

@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCells implements HeapSize {
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(RowCells.class, false);

  private final List<Cell> cells = new ArrayList<>();
  /**
   * Earliest expiration time among contained cells, derived from cell-level TTL tags. Set to
   * {@link Long#MAX_VALUE} when no cell carries a TTL tag, which lets the row cache short-circuit
   * the expiration check on every hit.
   */
  private final long earliestExpirationMs;

  public RowCells(List<Cell> cells) throws CloneNotSupportedException {
    long earliest = Long.MAX_VALUE;
    for (Cell cell : cells) {
      if (!(cell instanceof ExtendedCell extCell)) {
        throw new CloneNotSupportedException("Cell is not an ExtendedCell");
      }
      try {
        // To garbage collect the objects referenced by the cells
        ExtendedCell cloned = extCell.deepClone();
        this.cells.add(cloned);
        long exp = expirationTimeOf(cloned);
        if (exp < earliest) {
          earliest = exp;
        }
      } catch (RuntimeException e) {
        throw new CloneNotSupportedException("Deep clone failed");
      }
    }
    this.earliestExpirationMs = earliest;
  }

  private static long expirationTimeOf(ExtendedCell cell) {
    Iterator<Tag> i = PrivateCellUtil.tagsIterator(cell);
    while (i.hasNext()) {
      Tag t = i.next();
      if (TagType.TTL_TAG_TYPE == t.getType()) {
        return cell.getTimestamp() + Tag.getValueAsLong(t);
      }
    }
    return Long.MAX_VALUE;
  }

  public boolean isExpired(long now) {
    return earliestExpirationMs < now;
  }

  @Override
  public long heapSize() {
    long cellsSize = cells.stream().mapToLong(Cell::heapSize).sum();
    return FIXED_OVERHEAD + cellsSize;
  }

  public List<Cell> getCells() {
    return cells;
  }
}
