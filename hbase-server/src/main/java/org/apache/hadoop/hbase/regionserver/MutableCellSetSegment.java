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

import java.util.Iterator;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This mutable store segment encapsulates a mutable cell set and its respective memory allocation
 * buffers (MSLAB).
 */
@InterfaceAudience.Private
final class MutableCellSetSegment extends MutableSegment {

  private volatile CellSet cellSet;
  private final CellComparator comparator;

  // Instantiate objects only using factory
  MutableCellSetSegment(CellSet cellSet, MemStoreLAB memStoreLAB, long size,
      CellComparator comparator) {
    super(memStoreLAB, size);
    this.cellSet = cellSet;
    this.comparator = comparator;
  }

  @Override
  public SegmentScanner getSegmentScanner(long readPoint) {
    return new MutableCellSetSegmentScanner(this, readPoint);
  }

  @Override
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  @Override
  public int getCellsCount() {
    return getCellSet().size();
  }

  @Override
  public long add(Cell cell) {
    boolean succ = getCellSet().add(cell);
    long s = AbstractMemStore.heapSizeChange(cell, succ);
    updateMetaInfo(cell, s);
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if(cell.getTagsLength() > 0) {
      tagsPresent = true;
    }
    return s;
  }

  @Override
  public long rollback(Cell cell) {
    Cell found = get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      long sz = AbstractMemStore.heapSizeChange(cell, true);
      remove(cell);
      incSize(-sz);
      return sz;
    }
    return 0;
  }

  @Override
  public Cell getFirstAfter(Cell cell) {
    SortedSet<Cell> snTailSet = tailSet(cell);
    if (!snTailSet.isEmpty()) {
      return snTailSet.first();
    }
    return null;
  }

  @Override
  public void dump(Log log) {
    for (Cell cell: getCellSet()) {
      log.debug(cell);
    }
  }

  @Override
  public SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }
  @Override
  public CellSet getCellSet() {
    return cellSet;
  }
  @Override
  public CellComparator getComparator() {
    return comparator;
  }

  //*** Methods for MemStoreSegmentsScanner
  public Cell last() {
    return getCellSet().last();
  }

  public Iterator<Cell> iterator() {
    return getCellSet().iterator();
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    return getCellSet().headSet(firstKeyOnRow);
  }

  public int compare(Cell left, Cell right) {
    return getComparator().compare(left, right);
  }

  public int compareRows(Cell left, Cell right) {
    return getComparator().compareRows(left, right);
  }

  private Cell get(Cell cell) {
    return getCellSet().get(cell);
  }

  private boolean remove(Cell e) {
    return getCellSet().remove(e);
  }

  // methods for tests
  @Override
  Cell first() {
    return this.getCellSet().first();
  }

}
