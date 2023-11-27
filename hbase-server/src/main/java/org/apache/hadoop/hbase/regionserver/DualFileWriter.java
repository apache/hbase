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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.HAS_LATEST_VERSION_KEY;

/**
 * Separates the provided cells into two files, one file for the latest put cells and
 * the other for the rest of the cells.
 */
@InterfaceAudience.Private
public class DualFileWriter extends AbstractMultiFileWriter {

  private final CellComparator comparator;
  private StoreFileWriter latestVersionWriter;
  private StoreFileWriter multiVersionWriter;

  private final List<StoreFileWriter> writers;
  private Cell lastCell = null;
  private boolean deleteFamily = false;
  public DualFileWriter(CellComparator comparator) {
    this.comparator = comparator;
    writers = new ArrayList<>(2);
  }

  @Override
  public void append(Cell cell) throws IOException {
    if (lastCell != null && comparator.compareRows(lastCell, cell) != 0) {
      // It is a new row and thus time to reset deleteFamily and lastCell
      deleteFamily = false;
      lastCell = null;
    }

    if (!deleteFamily && cell.getType() == Cell.Type.Put
      && (lastCell == null || !CellUtil.matchingColumn(lastCell, cell))) {
      // No delete family marker has been seen for the current row and this is a put cell and
      // the first cell (i.e., the latest version) of a column. We can store it in the latest
      // version writer
      if (latestVersionWriter == null) {
        latestVersionWriter = writerFactory.createWriter();
        writers.add(latestVersionWriter);
      }
      latestVersionWriter.append(cell);
    } else {
      if (cell.getType() == Cell.Type.DeleteFamily
        || cell.getType() == Cell.Type.DeleteFamilyVersion) {
        deleteFamily = true;
      }
      if (multiVersionWriter == null) {
        multiVersionWriter = writerFactory.createWriter();
        writers.add(multiVersionWriter);
      }
      multiVersionWriter.append(cell);
    }
    lastCell = cell;
  }

  @Override
  protected Collection<StoreFileWriter> writers() {
    return writers;
  }

  @Override
  protected void preCommitWriters() throws IOException {
    if (writers.isEmpty()) {
      latestVersionWriter = writerFactory.createWriter();
      writers.add(latestVersionWriter);
    }
    if (latestVersionWriter != null) {
      latestVersionWriter.appendFileInfo(HAS_LATEST_VERSION_KEY, Bytes.toBytes(true));
    }
    if (multiVersionWriter != null) {
      multiVersionWriter.appendFileInfo(HAS_LATEST_VERSION_KEY, Bytes.toBytes(false));
    }
  }
}
