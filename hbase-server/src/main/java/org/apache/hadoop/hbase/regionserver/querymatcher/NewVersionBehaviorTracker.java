/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;

/**
 * A tracker both implementing ColumnTracker and DeleteTracker, used for mvcc-sensitive scanning.
 * We should make sure in one QueryMatcher the ColumnTracker and DeleteTracker is the same instance.
 */
@InterfaceAudience.Private
public class NewVersionBehaviorTracker implements ColumnTracker, DeleteTracker {

  private byte[] lastCqArray;
  private int lastCqLength;
  private int lastCqOffset;
  private long lastCqTs;
  private long lastCqMvcc;
  private byte lastCqType;
  private int columnIndex;
  private int countCurrentCol;

  protected int maxVersions;
  private int resultMaxVersions;
  private byte[][] columns;
  private int minVersions;
  private long oldestStamp;
  private CellComparator comparator;

  // These two maps have same structure.
  // Each node is a versions deletion (DeleteFamily or DeleteColumn). Key is the mvcc of the marker,
  // value is a data structure which contains infos we need that happens before this node's mvcc and
  // after the previous node's mvcc. The last node is a special node whose key is max_long that
  // saves infos after last deletion. See DeleteVersionsNode's comments for details.
  // The delColMap is constructed and used for each cq, and thedelFamMap is constructed when cq is
  // null and saving family-level delete markers. Each time the cq is changed, we should
  // reconstruct delColMap as a deep copy of delFamMap.
  protected NavigableMap<Long, DeleteVersionsNode> delColMap = new TreeMap<>();
  protected NavigableMap<Long, DeleteVersionsNode> delFamMap = new TreeMap<>();

  /**
   * Note maxVersion and minVersion must set according to cf's conf, not user's scan parameter.
   *
   * @param columns           columns specified user in query
   * @param comparartor       the cell comparator
   * @param minVersion        The minimum number of versions to keep(used when TTL is set).
   * @param maxVersion        The maximum number of versions in CF's conf
   * @param resultMaxVersions maximum versions to return per column, which may be different from
   *                          maxVersion
   * @param oldestUnexpiredTS the oldest timestamp we are interested in, based on TTL
   */
  public NewVersionBehaviorTracker(NavigableSet<byte[]> columns, CellComparator comparartor,
      int minVersion, int maxVersion, int resultMaxVersions, long oldestUnexpiredTS) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.resultMaxVersions = resultMaxVersions;
    this.oldestStamp = oldestUnexpiredTS;
    if (columns != null && columns.size() > 0) {
      this.columns = new byte[columns.size()][];
      int i = 0;
      for (byte[] column : columns) {
        this.columns[i++] = column;
      }
    }
    this.comparator = comparartor;
    reset();
  }

  @Override
  public void beforeShipped() throws IOException {
    // Do nothing
  }

  /**
   * A data structure which contains infos we need that happens before this node's mvcc and
   * after the previous node's mvcc. A node means there is a version deletion at the mvcc and ts.
   */
  protected class DeleteVersionsNode {
    public long ts;
    public long mvcc;

    // <timestamp, set<mvcc>>
    // Key is ts of version deletes, value is its mvccs.
    // We may delete more than one time for a version.
    private Map<Long, SortedSet<Long>> deletesMap = new HashMap<>();

    // <mvcc, set<mvcc>>
    // Key is mvcc of version deletes, value is mvcc of visible puts before the delete effect.
    private NavigableMap<Long, SortedSet<Long>> mvccCountingMap = new TreeMap<>();

    protected DeleteVersionsNode(long ts, long mvcc) {
      this.ts = ts;
      this.mvcc = mvcc;
      mvccCountingMap.put(Long.MAX_VALUE, new TreeSet<Long>());
    }

    protected DeleteVersionsNode() {
      this(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public void addVersionDelete(Cell cell) {
      SortedSet<Long> set = deletesMap.get(cell.getTimestamp());
      if (set == null) {
        set = new TreeSet<>();
        deletesMap.put(cell.getTimestamp(), set);
      }
      set.add(cell.getSequenceId());
      // The init set should be the puts whose mvcc is smaller than this Delete. Because
      // there may be some Puts masked by them. The Puts whose mvcc is larger than this Delete can
      // not be copied to this node because we may delete one version and the oldest put may not be
      // masked.
      SortedSet<Long> nextValue = mvccCountingMap.ceilingEntry(cell.getSequenceId()).getValue();
      SortedSet<Long> thisValue = new TreeSet<>(nextValue.headSet(cell.getSequenceId()));
      mvccCountingMap.put(cell.getSequenceId(), thisValue);
    }

    protected DeleteVersionsNode getDeepCopy() {
      DeleteVersionsNode node = new DeleteVersionsNode(ts, mvcc);
      for (Map.Entry<Long, SortedSet<Long>> e : deletesMap.entrySet()) {
        node.deletesMap.put(e.getKey(), new TreeSet<>(e.getValue()));
      }
      for (Map.Entry<Long, SortedSet<Long>> e : mvccCountingMap.entrySet()) {
        node.mvccCountingMap.put(e.getKey(), new TreeSet<>(e.getValue()));
      }
      return node;
    }
  }

  /**
   * Reset the map if it is different with the last Cell.
   * Save the cq array/offset/length for next Cell.
   *
   * @return If this put has duplicate ts with last cell, return the mvcc of last cell.
   * Else return MAX_VALUE.
   */
  protected long prepare(Cell cell) {
    if (isColumnQualifierChanged(cell)) {
      // The last cell is family-level delete and this is not, or the cq is changed,
      // we should construct delColMap as a deep copy of delFamMap.
      delColMap.clear();
      for (Map.Entry<Long, DeleteVersionsNode> e : delFamMap.entrySet()) {
        delColMap.put(e.getKey(), e.getValue().getDeepCopy());
      }
      countCurrentCol = 0;
    } else if (!PrivateCellUtil.isDelete(lastCqType) && lastCqType == cell.getTypeByte()
        && lastCqTs == cell.getTimestamp()) {
      // Put with duplicate timestamp, ignore.
      return lastCqMvcc;
    }
    lastCqArray = cell.getQualifierArray();
    lastCqOffset = cell.getQualifierOffset();
    lastCqLength = cell.getQualifierLength();
    lastCqTs = cell.getTimestamp();
    lastCqMvcc = cell.getSequenceId();
    lastCqType = cell.getTypeByte();
    return Long.MAX_VALUE;
  }

  private boolean isColumnQualifierChanged(Cell cell) {
    if (delColMap.isEmpty() && lastCqArray == null && cell.getQualifierLength() == 0
      && (PrivateCellUtil.isDeleteColumns(cell) || PrivateCellUtil.isDeleteColumnVersion(cell))) {
      // for null columnQualifier
      return true;
    }
    return !PrivateCellUtil.matchingQualifier(cell, lastCqArray, lastCqOffset, lastCqLength);
  }

  // DeleteTracker
  @Override
  public void add(Cell cell) {
    prepare(cell);
    byte type = cell.getTypeByte();
    switch (Type.codeToType(type)) {
    // By the order of seen. We put null cq at first.
    case DeleteFamily: // Delete all versions of all columns of the specified family
      delFamMap.put(cell.getSequenceId(),
          new DeleteVersionsNode(cell.getTimestamp(), cell.getSequenceId()));
      break;
    case DeleteFamilyVersion: // Delete all columns of the specified family and specified version
      delFamMap.ceilingEntry(cell.getSequenceId()).getValue().addVersionDelete(cell);
      break;

    // These two kinds of markers are mix with Puts.
    case DeleteColumn: // Delete all versions of the specified column
      delColMap.put(cell.getSequenceId(),
          new DeleteVersionsNode(cell.getTimestamp(), cell.getSequenceId()));
      break;
    case Delete: // Delete the specified version of the specified column.
      delColMap.ceilingEntry(cell.getSequenceId()).getValue().addVersionDelete(cell);
      break;
    default:
      throw new AssertionError("Unknown delete marker type for " + cell);
    }
  }

  /**
   * This method is not idempotent, we will save some info to judge VERSION_MASKED.
   * @param cell - current cell to check if deleted by a previously seen delete
   * @return We don't distinguish DeleteColumn and DeleteFamily. We only return code for column.
   */
  @Override
  public DeleteResult isDeleted(Cell cell) {
    long duplicateMvcc = prepare(cell);

    for (Map.Entry<Long, DeleteVersionsNode> e : delColMap.tailMap(cell.getSequenceId())
        .entrySet()) {
      DeleteVersionsNode node = e.getValue();
      long deleteMvcc = Long.MAX_VALUE;
      SortedSet<Long> deleteVersionMvccs = node.deletesMap.get(cell.getTimestamp());
      if (deleteVersionMvccs != null) {
        SortedSet<Long> tail = deleteVersionMvccs.tailSet(cell.getSequenceId());
        if (!tail.isEmpty()) {
          deleteMvcc = tail.first();
        }
      }
      SortedMap<Long, SortedSet<Long>> subMap =
          node.mvccCountingMap
              .subMap(cell.getSequenceId(), true, Math.min(duplicateMvcc, deleteMvcc), true);
      for (Map.Entry<Long, SortedSet<Long>> seg : subMap.entrySet()) {
        if (seg.getValue().size() >= maxVersions) {
          return DeleteResult.VERSION_MASKED;
        }
        seg.getValue().add(cell.getSequenceId());
      }
      if (deleteMvcc < Long.MAX_VALUE) {
        return DeleteResult.VERSION_DELETED;
      }

      if (cell.getTimestamp() <= node.ts) {
        return DeleteResult.COLUMN_DELETED;
      }
    }
    if (duplicateMvcc < Long.MAX_VALUE) {
      return DeleteResult.VERSION_MASKED;
    }
    return DeleteResult.NOT_DELETED;
  }

  @Override
  public boolean isEmpty() {
    return delColMap.size() == 1 && delColMap.get(Long.MAX_VALUE).mvccCountingMap.size() == 1
        && delFamMap.size() == 1 && delFamMap.get(Long.MAX_VALUE).mvccCountingMap.size() == 1;
  }

  @Override
  public void update() {
    // ignore
  }

  //ColumnTracker

  @Override
  public MatchCode checkColumn(Cell cell, byte type) throws IOException {
    if (columns == null) {
        return MatchCode.INCLUDE;
    }

    while (!done()) {
      int c = CellUtil.compareQualifiers(cell,
        columns[columnIndex], 0, columns[columnIndex].length);
      if (c < 0) {
        return MatchCode.SEEK_NEXT_COL;
      }

      if (c == 0) {
        // We drop old version in #isDeleted, so here we must return INCLUDE.
        return MatchCode.INCLUDE;
      }

      columnIndex++;
    }
    // No more columns left, we are done with this query
    return MatchCode.SEEK_NEXT_ROW;
  }

  @Override
  public MatchCode checkVersions(Cell cell, long timestamp, byte type,
      boolean ignoreCount) throws IOException {
    assert !PrivateCellUtil.isDelete(type);
    // We drop old version in #isDeleted, so here we won't SKIP because of versioning. But we should
    // consider TTL.
    if (ignoreCount) {
      return MatchCode.INCLUDE;
    }
    countCurrentCol++;
    if (timestamp < this.oldestStamp) {
      if (countCurrentCol == minVersions) {
        return MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
      }
      if (countCurrentCol > minVersions) {
        // This may not be reached, only for safety.
        return MatchCode.SEEK_NEXT_COL;
      }
    }

    if (countCurrentCol == resultMaxVersions) {
      // We have enough number of versions for user's requirement.
      return MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
    }
    if (countCurrentCol > resultMaxVersions) {
      // This may not be reached, only for safety
      return MatchCode.SEEK_NEXT_COL;
    }
    return MatchCode.INCLUDE;
  }

  @Override
  public void reset() {
    delColMap.clear();
    delFamMap.clear();
    lastCqArray = null;
    lastCqLength = 0;
    lastCqOffset = 0;
    lastCqTs = Long.MIN_VALUE;
    lastCqMvcc = 0;
    lastCqType = 0;
    columnIndex = 0;
    countCurrentCol = 0;
    resetInternal();
  }

  protected void resetInternal(){
    delFamMap.put(Long.MAX_VALUE, new DeleteVersionsNode());
  }

  @Override
  public boolean done() {
    return columns != null && columnIndex >= columns.length;
  }

  @Override
  public ColumnCount getColumnHint() {
    if (columns != null) {
      if (columnIndex < columns.length) {
        return new ColumnCount(columns[columnIndex]);
      }
    }
    return null;
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
    // TODO maybe we can optimize.
    return MatchCode.SEEK_NEXT_COL;
  }

  @Override
  public boolean isDone(long timestamp) {
    // We can not skip Cells with small ts.
    return false;
  }

  @Override
  public CellComparator getCellComparator() {
    return this.comparator;
  }

}
