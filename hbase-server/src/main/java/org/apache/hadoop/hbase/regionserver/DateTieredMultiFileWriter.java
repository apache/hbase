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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * class for cell sink that separates the provided cells into multiple files for date tiered
 * compaction.
 */
@InterfaceAudience.Private
public class DateTieredMultiFileWriter extends AbstractMultiFileWriter {

  private final NavigableMap<Long, StoreFileWriter> lowerBoundary2Writer
    = new TreeMap<Long, StoreFileWriter>();

  private final boolean needEmptyFile;

  /**
   * @param needEmptyFile whether need to create an empty store file if we haven't written out
   *          anything.
   */
  public DateTieredMultiFileWriter(List<Long> lowerBoundaries, boolean needEmptyFile) {
    for (Long lowerBoundary : lowerBoundaries) {
      lowerBoundary2Writer.put(lowerBoundary, null);
    }
    this.needEmptyFile = needEmptyFile;
  }

  @Override
  public void append(Cell cell) throws IOException {
    Map.Entry<Long, StoreFileWriter> entry = lowerBoundary2Writer.floorEntry(cell.getTimestamp());
    StoreFileWriter writer = entry.getValue();
    if (writer == null) {
      writer = writerFactory.createWriter();
      lowerBoundary2Writer.put(entry.getKey(), writer);
    }
    writer.append(cell);
  }

  @Override
  protected Collection<StoreFileWriter> writers() {
    return lowerBoundary2Writer.values();
  }

  @Override
  protected void preCommitWriters() throws IOException {
    if (!needEmptyFile) {
      return;
    }
    for (StoreFileWriter writer : lowerBoundary2Writer.values()) {
      if (writer != null) {
        return;
      }
    }
    // we haven't written out any data, create an empty file to retain metadata
    lowerBoundary2Writer.put(lowerBoundary2Writer.firstKey(), writerFactory.createWriter());
  }
}
