/**
 *
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;

/**
 * CellArrayImmutableSegment extends the API supported by a {@link Segment},
 * and {@link ImmutableSegment}. This immutable segment is working with CellSet with
 * CellArrayMap delegatee.
 */
@InterfaceAudience.Private
public class CellArrayImmutableSegment extends ImmutableSegment {

  public static final long DEEP_OVERHEAD_CAM = DEEP_OVERHEAD + ClassSize.CELL_ARRAY_MAP;

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * C-tor to be used when new CellArrayImmutableSegment is a result of compaction of a
   * list of older ImmutableSegments.
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellArrayImmutableSegment(CellComparator comparator, MemStoreSegmentsIterator iterator,
      MemStoreLAB memStoreLAB, int numOfCells, MemStoreCompactionStrategy.Action action) {
    super(null, comparator, memStoreLAB); // initiailize the CellSet with NULL
    incMemStoreSize(0, DEEP_OVERHEAD_CAM, 0, 0); // CAM is always on-heap
    // build the new CellSet based on CellArrayMap and update the CellSet of the new Segment
    initializeCellSet(numOfCells, iterator, action);
  }

  /**------------------------------------------------------------------------
   * C-tor to be used when new CellChunkImmutableSegment is built as a result of flattening
   * of CSLMImmutableSegment
   * The given iterator returns the Cells that "survived" the compaction.
   */
  protected CellArrayImmutableSegment(CSLMImmutableSegment segment, MemStoreSizing mss,
      MemStoreCompactionStrategy.Action action) {
    super(segment); // initiailize the upper class
    long indexOverhead = DEEP_OVERHEAD_CAM - CSLMImmutableSegment.DEEP_OVERHEAD_CSLM;
    incMemStoreSize(0, indexOverhead, 0, 0); // CAM is always on-heap
    mss.incMemStoreSize(0, indexOverhead, 0, 0);
    int numOfCells = segment.getCellsCount();
    // build the new CellSet based on CellChunkMap and update the CellSet of this Segment
    reinitializeCellSet(numOfCells, segment.getScanner(Long.MAX_VALUE), segment.getCellSet(),
      action);
    // arrange the meta-data size, decrease all meta-data sizes related to SkipList;
    // add sizes of CellArrayMap entry (reinitializeCellSet doesn't take the care for the sizes)
    long newSegmentSizeDelta =
        numOfCells * (indexEntrySize() - ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    incMemStoreSize(0, newSegmentSizeDelta, 0, 0);
    mss.incMemStoreSize(0, newSegmentSizeDelta, 0, 0);
  }

  @Override
  protected long indexEntrySize() {
    return ClassSize.CELL_ARRAY_MAP_ENTRY;
  }

  @Override
  protected boolean canBeFlattened() {
    return false;
  }

  /////////////////////  PRIVATE METHODS  /////////////////////
  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellArrayMap from compacting iterator
  private void initializeCellSet(int numOfCells, MemStoreSegmentsIterator iterator,
      MemStoreCompactionStrategy.Action action) {

    boolean merge = (action == MemStoreCompactionStrategy.Action.MERGE ||
        action == MemStoreCompactionStrategy.Action.MERGE_COUNT_UNIQUE_KEYS);
    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    int i = 0;
    int numUniqueKeys=0;
    Cell prev = null;
    while (iterator.hasNext()) {
      Cell c = iterator.next();
      // The scanner behind the iterator is doing all the elimination logic
      if (merge) {
        // if this is merge we just move the Cell object without copying MSLAB
        // the sizes still need to be updated in the new segment
        cells[i] = c;
      } else {
        // now we just copy it to the new segment (also MSLAB copy)
        cells[i] = maybeCloneWithAllocator(c, false);
      }
      // second parameter true, because in compaction/merge the addition of the cell to new segment
      // is always successful
      updateMetaInfo(cells[i], true, null); // updates the size per cell
      if(action == MemStoreCompactionStrategy.Action.MERGE_COUNT_UNIQUE_KEYS) {
        //counting number of unique keys
        if (prev != null) {
          if (!CellUtil.matchingRowColumnBytes(prev, c)) {
            numUniqueKeys++;
          }
        } else {
          numUniqueKeys++;
        }
      }
      prev = c;
      i++;
    }
    if(action == MemStoreCompactionStrategy.Action.COMPACT) {
      numUniqueKeys = numOfCells;
    } else if(action != MemStoreCompactionStrategy.Action.MERGE_COUNT_UNIQUE_KEYS) {
      numUniqueKeys = CellSet.UNKNOWN_NUM_UNIQUES;
    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(), cells, 0, i, false);
    this.setCellSet(null, new CellSet(cam, numUniqueKeys));   // update the CellSet of this Segment
  }

  /*------------------------------------------------------------------------*/
  // Create CellSet based on CellChunkMap from current ConcurrentSkipListMap based CellSet
  // (without compacting iterator)
  // We do not consider cells bigger than chunks!
  private void reinitializeCellSet(
      int numOfCells, KeyValueScanner segmentScanner, CellSet oldCellSet,
      MemStoreCompactionStrategy.Action action) {
    Cell[] cells = new Cell[numOfCells];   // build the Cell Array
    Cell curCell;
    int idx = 0;
    int numUniqueKeys=0;
    Cell prev = null;
    try {
      while ((curCell = segmentScanner.next()) != null) {
        cells[idx++] = curCell;
        if(action == MemStoreCompactionStrategy.Action.FLATTEN_COUNT_UNIQUE_KEYS) {
          //counting number of unique keys
          if (prev != null) {
            if (!CellUtil.matchingRowColumn(prev, curCell)) {
              numUniqueKeys++;
            }
          } else {
            numUniqueKeys++;
          }
        }
        prev = curCell;
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    } finally {
      segmentScanner.close();
    }
    if(action != MemStoreCompactionStrategy.Action.FLATTEN_COUNT_UNIQUE_KEYS) {
      numUniqueKeys = CellSet.UNKNOWN_NUM_UNIQUES;
    }
    // build the immutable CellSet
    CellArrayMap cam = new CellArrayMap(getComparator(), cells, 0, idx, false);
    // update the CellSet of this Segment
    this.setCellSet(oldCellSet, new CellSet(cam, numUniqueKeys));
  }

}
