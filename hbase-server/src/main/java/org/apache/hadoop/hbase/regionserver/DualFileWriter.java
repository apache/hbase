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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.HAS_LIVE_VERSIONS_KEY;

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
  private final boolean keepDeletedCells;
  private final int maxVersions;
  public DualFileWriter(CellComparator comparator, int maxVersions,
    boolean keepDeletedCells, boolean dualWriterEnabled) {
    this.comparator = comparator;
    this.maxVersions = maxVersions;
    this.keepDeletedCells = keepDeletedCells;
    this.dualWriterEnabled = dualWriterEnabled;
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

  private void addLiveVersion(Cell cell) throws  IOException {
    if (liveVersionWriter == null) {
      liveVersionWriter = writerFactory.createWriter();
      writers.add(liveVersionWriter);
    }
    liveVersionWriter.append(cell);
  }

  private void addHistoricalVersion(Cell cell) throws  IOException {
    if (!keepDeletedCells) {
      return;
    }
    if (historicalVersionWriter == null) {
      historicalVersionWriter = writerFactory.createWriter();
      writers.add(historicalVersionWriter);
    }
    historicalVersionWriter.append(cell);
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
    if ((lastCell == null || !CellUtil.matchingColumn(lastCell, cell))) {
      initColumnState();
    }
    if (cell.getType() == Cell.Type.DeleteFamily) {
      if (deleteFamily == null) {
        if (cell.getType() == Cell.Type.DeleteFamily) {
          deleteFamily = cell;
          addLiveVersion(cell);
        } else {
          addHistoricalVersion(cell);
        }
      }
    } else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
      if (deleteFamily == null) {
        deleteFamilyVersionList.add(cell);
        addLiveVersion(cell);
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
        addLiveVersion(cell);
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
  public HFile.Writer getHFileWriter() {
    if (writers.isEmpty()) {
      return null;
    }
    return writers.get(0).getHFileWriter();
  }
}
