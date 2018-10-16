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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.LAST_BLOOM_KEY;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles ROWCOL bloom related context. It can work with both BytebufferdCells
 * and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowColBloomContext extends BloomContext {

  public RowColBloomContext(BloomFilterWriter generalBloomFilterWriter, CellComparator comparator) {
    super(generalBloomFilterWriter, comparator);
  }

  @Override
  public void addLastBloomKey(Writer writer) throws IOException {
    if (this.getLastCell() != null) {
      Cell firstOnRow = PrivateCellUtil.createFirstOnRowCol(this.getLastCell());
      // This copy happens only once when the writer is closed
      byte[] key = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(firstOnRow);
      writer.appendFileInfo(LAST_BLOOM_KEY, key);
    }
  }

  @Override
  protected boolean isNewKey(Cell cell) {
    if (this.getLastCell() != null) {
      return !CellUtil.matchingRowColumn(cell, this.getLastCell());
    }
    return true;
  }
}
