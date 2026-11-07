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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles ROWPREFIX bloom related context. It works with both ByteBufferedCell and byte[] backed
 * cells
 */
@InterfaceAudience.Private
public class RowPrefixFixedLengthBloomContext extends RowBloomContext {
  private final int prefixLength;
  private final int startOffset;

  public RowPrefixFixedLengthBloomContext(BloomFilterWriter bloomFilterWriter,
    CellComparator comparator, int prefixLength, int startOffset) {
    super(bloomFilterWriter, comparator);
    this.prefixLength = prefixLength;
    this.startOffset = startOffset;
  }

  @Override
  public void writeBloom(ExtendedCell cell) throws IOException {
    // Skip rows shorter than startOffset. They have no bytes at the prefix window, and the read
    // path bypasses the bloom entirely when the query row is this short (returns true to scan),
    // so any entry we would write here is unreachable and only inflates the false positive rate.
    if (cell.getRowLength() <= startOffset) {
      return;
    }
    super.writeBloom(getRowPrefixCell(cell));
  }

  /**
   * @param cell the cell
   * @return the new cell created by row prefix
   */
  private ExtendedCell getRowPrefixCell(ExtendedCell cell) {
    byte[] row = CellUtil.copyRow(cell);
    int length = Math.min(prefixLength, row.length - startOffset);
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(row, startOffset, length).setType(Cell.Type.Put).build();
  }
}
