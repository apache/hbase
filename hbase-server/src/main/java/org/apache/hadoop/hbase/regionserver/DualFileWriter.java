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
 * Separates the provided cells into two files, one file for the latest cells and
 * the other for the rest of the cells. The latest cells includes the latest put cells that are
 * not deleted by a delete marker, the delete markers that delete latest put cells, and the
 * version delete markers (that is, DeleteFamilyVersion and Delete) that are not deleted by other
 * delete markers (that is DeleteFamily and DeleteColumn).
 */
@InterfaceAudience.Private
public class DualFileWriter extends AbstractMultiFileWriter {

  private final CellComparator comparator;
  private StoreFileWriter latestVersionWriter;
  private StoreFileWriter multiVersionWriter;

  private final List<StoreFileWriter> writers;
  private Cell lastCell;
  private Cell deleteFamily;
  private List<Cell> deleteFamilyVersionList = new ArrayList<>();
  private Cell deleteColumn;
  private List<Cell> deleteColumnVersionList = new ArrayList<>();
  private Cell firstAndPutCellOfAColumn;
  public DualFileWriter(CellComparator comparator) {
    this.comparator = comparator;
    writers = new ArrayList<>(2);
    initRowState();
  }

  private void initRowState() {
    deleteFamily = null;
    deleteFamilyVersionList.clear();
    lastCell = null;
  }

  private void initColumnState() {
    deleteColumn = null;
    deleteColumnVersionList.clear();
    firstAndPutCellOfAColumn = null;
  }

  private void addLatestVersion(Cell cell) throws  IOException {
    if (latestVersionWriter == null) {
      latestVersionWriter = writerFactory.createWriter();
      writers.add(latestVersionWriter);
    }
    latestVersionWriter.append(cell);
  }

  private void addOlderVersion(Cell cell) throws  IOException {
    if (multiVersionWriter == null) {
      multiVersionWriter = writerFactory.createWriter();
      writers.add(multiVersionWriter);
    }
    multiVersionWriter.append(cell);
  }

  private boolean isDeletedByDeleteFamily(Cell cell) {
    return deleteFamily != null && deleteFamily.getTimestamp() >= cell.getTimestamp();
  }

  private boolean isDeletedByDeleteFamilyVersion(Cell cell) {
    for (Cell deleteFamilyVersion : deleteFamilyVersionList) {
      if (deleteFamilyVersion.getTimestamp() == cell.getTimestamp()) return true;
    }
    return false;
  }

  private boolean isDeletedByDeleteColumnVersion(Cell cell) {
    for (Cell deleteColumnVersion : deleteColumnVersionList) {
      if (deleteColumnVersion.getTimestamp() == cell.getTimestamp()) return true;
    }
    return false;
  }

  private boolean isDeleted(Cell cell) {
    return isDeletedByDeleteFamily(cell) || deleteColumn != null
      || isDeletedByDeleteFamilyVersion(cell) || isDeletedByDeleteColumnVersion(cell);
  }
  @Override
  public void append(Cell cell) throws IOException {
    if (lastCell != null && comparator.compareRows(lastCell, cell) != 0) {
      // It is a new row and thus time to reset the state
      initRowState();
    }
    if ((lastCell == null || !CellUtil.matchingColumn(lastCell, cell))) {
      initColumnState();
    }
    if (cell.getType() == Cell.Type.DeleteFamily) {
      if (deleteFamily == null) {
        if (cell.getType() == Cell.Type.DeleteFamily) {
          deleteFamily = cell;
          addLatestVersion(cell);
        } else {
          addOlderVersion(cell);
        }
      }
    } else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
      if (deleteFamily == null) {
        deleteFamilyVersionList.add(cell);
        addLatestVersion(cell);
      } else {
        addOlderVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.DeleteColumn) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumn = cell;
        addLatestVersion(cell);
      } else {
        addOlderVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.Delete) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumnVersionList.add(cell);
        addLatestVersion(cell);
      } else {
        addOlderVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.Put) {
      if (firstAndPutCellOfAColumn == null) {
        // This is the first put cell (i.e., the latest version) of a column. Is it deleted?
        if (!isDeleted(cell)) {
          addLatestVersion(cell);
          firstAndPutCellOfAColumn = cell;
        } else {
          // It is deleted
          addOlderVersion(cell);
        }
      } else {
        // It is an older put cell
        addOlderVersion(cell);
      }
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
