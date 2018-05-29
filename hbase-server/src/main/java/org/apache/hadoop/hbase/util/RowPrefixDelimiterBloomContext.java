/**
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
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles ROWPREFIX_DELIMITED bloom related context.
 * It works with both ByteBufferedCell and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowPrefixDelimiterBloomContext extends RowBloomContext {
  private final byte[] delimiter;

  public RowPrefixDelimiterBloomContext(BloomFilterWriter bloomFilterWriter,
      CellComparator comparator, byte[] delimiter) {
    super(bloomFilterWriter, comparator);
    this.delimiter = delimiter;
  }

  public void writeBloom(Cell cell) throws IOException {
    super.writeBloom(getDelimitedRowPrefixCell(cell));
  }

  /**
   * @param cell the new cell
   * @return the new cell created by delimited row prefix
   */
  private Cell getDelimitedRowPrefixCell(Cell cell) {
    byte[] row = CellUtil.copyRow(cell);
    int prefixLength = Bytes.indexOf(row, delimiter);
    if (prefixLength <= 0) {
      return cell;
    }
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(row, 0, Math.min(prefixLength, row.length))
        .setType(Cell.Type.Put)
        .build();
  }
}
