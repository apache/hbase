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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Handles ROW bloom related context. It works with both ByteBufferedCell and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowBloomContext extends BloomContext {

  public RowBloomContext(BloomFilterWriter generalBloomFilterWriter) {
    super(generalBloomFilterWriter);
  }

  public void addLastBloomKey(Writer writer) throws IOException {
    if (lastCell != null) {
      byte[] key = CellUtil.copyRow(this.lastCell);
      writer.appendFileInfo(StoreFile.LAST_BLOOM_KEY, key);
    }
  }

  @Override
  protected boolean isNewKey(Cell cell) {
    if (this.lastCell != null) {
      return !CellUtil.matchingRows(cell, this.lastCell);
    }
    return true;
  }
}
