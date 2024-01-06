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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.HAS_LIVE_VERSIONS_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Separates the provided cells into two files, one file for the live cells and the other for the
 * rest of the cells (historical cells). The live cells includes the live put cells, delete all and
 * version delete markers that are not masked by other delete all markers.
 */
@InterfaceAudience.Private
public class DualFileWriter extends AbstractMultiFileWriter {

  private final CellComparator comparator;
  private StoreFileWriter liveVersionWriter;
  private StoreFileWriter historicalVersionWriter;

  private final List<StoreFileWriter> writers;
  // The last cell of the current row
  private Cell lastCell;
  // The first (latest) delete family marker of the current row
  private Cell deleteFamily;
  // The list of delete family version markers of the current row
  private List<Cell> deleteFamilyVersionList = new ArrayList<>();
  // The first (latest) delete column marker of the current column
  private Cell deleteColumn;
  // The list of delete column version markers of the current column
  private List<Cell> deleteColumnVersionList = new ArrayList<>();
  // The live put cell count for the current column
  private int livePutCellCount;
  private final boolean dualWriterEnabled;
  private final int maxVersions;
  private final boolean newVersionBehavior;

  public DualFileWriter(CellComparator comparator, int maxVersions, boolean dualWriterEnabled,
    boolean newVersionBehavior) {
    this.comparator = comparator;
    this.maxVersions = maxVersions;
    this.dualWriterEnabled = dualWriterEnabled;
    this.newVersionBehavior = newVersionBehavior;
    writers = new ArrayList<>(2);
    initRowState();
  }

  private void initRowState() {
    deleteFamily = null;
    deleteFamilyVersionList.clear();
    lastCell = null;
  }

  private void initColumnState() {
    livePutCellCount = 0;
    deleteColumn = null;
    deleteColumnVersionList.clear();

  }

  private void addLiveVersion(Cell cell) throws IOException {
    if (liveVersionWriter == null) {
      liveVersionWriter = writerFactory.createWriter();
      writers.add(liveVersionWriter);
    }
    liveVersionWriter.append(cell);
  }

  private void addHistoricalVersion(Cell cell) throws IOException {
    if (historicalVersionWriter == null) {
      historicalVersionWriter = writerFactory.createWriter();
      writers.add(historicalVersionWriter);
    }
    historicalVersionWriter.append(cell);
  }

  private boolean isDeletedByDeleteFamily(Cell cell) {
    return deleteFamily != null && (deleteFamily.getTimestamp() > cell.getTimestamp()
      || (deleteFamily.getTimestamp() == cell.getTimestamp()
        && (!newVersionBehavior || cell.getSequenceId() < deleteFamily.getSequenceId())));
  }

  private boolean isDeletedByDeleteFamilyVersion(Cell cell) {
    for (Cell deleteFamilyVersion : deleteFamilyVersionList) {
      if (
        deleteFamilyVersion.getTimestamp() == cell.getTimestamp()
          && (!newVersionBehavior || cell.getSequenceId() < deleteFamilyVersion.getSequenceId())
      ) return true;
    }
    return false;
  }

  private boolean isDeletedByDeleteColumn(Cell cell) {
    return deleteColumn != null && (deleteColumn.getTimestamp() > cell.getTimestamp()
      || (deleteColumn.getTimestamp() == cell.getTimestamp()
        && (!newVersionBehavior || cell.getSequenceId() < deleteColumn.getSequenceId())));
  }

  private boolean isDeletedByDeleteColumnVersion(Cell cell) {
    for (Cell deleteColumnVersion : deleteColumnVersionList) {
      if (
        deleteColumnVersion.getTimestamp() == cell.getTimestamp()
          && (!newVersionBehavior || cell.getSequenceId() < deleteColumnVersion.getSequenceId())
      ) return true;
    }
    return false;
  }

  private boolean isDeleted(Cell cell) {
    return isDeletedByDeleteFamily(cell) || isDeletedByDeleteColumn(cell)
      || isDeletedByDeleteFamilyVersion(cell) || isDeletedByDeleteColumnVersion(cell);
  }

  private void appendCell(Cell cell) throws IOException {
    if ((lastCell == null || !CellUtil.matchingColumn(lastCell, cell))) {
      initColumnState();
    }
    if (cell.getType() == Cell.Type.DeleteFamily) {
      if (deleteFamily == null) {
        deleteFamily = cell;
        addLiveVersion(cell);
      } else {
        addHistoricalVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
      if (!isDeletedByDeleteFamily(cell)) {
        deleteFamilyVersionList.add(cell);
        if (deleteFamily != null && deleteFamily.getTimestamp() == cell.getTimestamp()) {
          // This means both the delete-family and delete-family-version markers have the same
          // timestamp but the sequence id of delete-family-version marker is higher than that of
          // the delete-family marker. In this case, there is no need to add the
          // delete-family-version marker to the live version file. This case happens only with
          // the new version behavior.
          addHistoricalVersion(cell);
        } else {
          addLiveVersion(cell);
        }
      } else {
        addHistoricalVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.DeleteColumn) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumn = cell;
        addLiveVersion(cell);
      } else {
        addHistoricalVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.Delete) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumnVersionList.add(cell);
        if (deleteFamily != null && deleteFamily.getTimestamp() == cell.getTimestamp()) {
          // This means both the delete-family and delete-column-version markers have the same
          // timestamp but the sequence id of delete-column-version marker is higher than that of
          // the delete-family marker. In this case, there is no need to add the
          // delete-column-version marker to the live version file. This case happens only with
          // the new version behavior.
          addHistoricalVersion(cell);
        } else {
          addLiveVersion(cell);
        }
      } else {
        addHistoricalVersion(cell);
      }
    } else if (cell.getType() == Cell.Type.Put) {
      if (livePutCellCount < maxVersions) {
        // This is a live put cell (i.e., the latest version) of a column. Is it deleted?
        if (!isDeleted(cell)) {
          addLiveVersion(cell);
          livePutCellCount++;
        } else {
          // It is deleted
          addHistoricalVersion(cell);
        }
      } else {
        // It is an older put cell
        addHistoricalVersion(cell);
      }
    }
    lastCell = cell;
  }

  @Override
  public void appendAll(List<Cell> cellList) throws IOException {
    if (!dualWriterEnabled) {
      // If the dual writer is not enabled then all cells are written to one file. We use
      // the live version file in this case
      for (Cell cell : cellList) {
        addLiveVersion(cell);
      }
      return;
    }
    if (cellList.isEmpty()) {
      return;
    }
    if (lastCell != null && comparator.compareRows(lastCell, cellList.get(0)) != 0) {
      // It is a new row and thus time to reset the state
      initRowState();
    }
    for (Cell cell : cellList) {
      appendCell(cell);
    }
  }

  @Override
  public void append(Cell cell) throws IOException {
    if (!dualWriterEnabled) {
      // If the dual writer is not enabled then all cells are written to one file. We use
      // the live version file in this case
      addLiveVersion(cell);
      return;
    }
    if (lastCell != null && comparator.compareRows(lastCell, cell) != 0) {
      // It is a new row and thus time to reset the state
      initRowState();
    }
    appendCell(cell);
  }

  @Override
  protected Collection<StoreFileWriter> writers() {
    return writers;
  }

  @Override
  protected void preCommitWriters() throws IOException {
    if (writers.isEmpty()) {
      liveVersionWriter = writerFactory.createWriter();
      writers.add(liveVersionWriter);
    }
    if (!dualWriterEnabled) {
      return;
    }
    if (liveVersionWriter != null) {
      liveVersionWriter.appendFileInfo(HAS_LIVE_VERSIONS_KEY, Bytes.toBytes(true));
    }
    if (historicalVersionWriter != null) {
      historicalVersionWriter.appendFileInfo(HAS_LIVE_VERSIONS_KEY, Bytes.toBytes(false));
    }
  }

  public HFile.Writer getLiveVersionHFileWriter() {
    if (writers.isEmpty()) {
      return null;
    }
    return writers.get(0).getHFileWriter();
  }
}
