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

package org.apache.hadoop.hbase.codec.prefixtree.decode;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;

import com.google.common.primitives.UnsignedBytes;

/**
 * <p>
 * Searcher extends the capabilities of the Scanner + ReversibleScanner to add the ability to
 * position itself on a requested Cell without scanning through cells before it. The PrefixTree is
 * set up to be a Trie of rows, so finding a particular row is extremely cheap.
 * </p>
 * Once it finds the row, it does a binary search through the cells inside the row, which is not as
 * fast as the trie search, but faster than iterating through every cell like existing block
 * formats
 * do. For this reason, this implementation is targeted towards schemas where rows are narrow
 * enough
 * to have several or many per block, and where you are generally looking for the entire row or
 * the
 * first cell. It will still be fast for wide rows or point queries, but could be improved upon.
 */
@InterfaceAudience.Private
public class PrefixTreeArraySearcher extends PrefixTreeArrayReversibleScanner implements
    CellSearcher {

  /*************** construct ******************************/

  public PrefixTreeArraySearcher(PrefixTreeBlockMeta blockMeta, int rowTreeDepth,
      int rowBufferLength, int qualifierBufferLength, int tagsBufferLength) {
    super(blockMeta, rowTreeDepth, rowBufferLength, qualifierBufferLength, tagsBufferLength);
  }


  /********************* CellSearcher methods *******************/

  @Override
  public boolean positionAt(Cell key) {
    return CellScannerPosition.AT == positionAtOrAfter(key);
  }

  @Override
  public CellScannerPosition positionAtOrBefore(Cell key) {
    reInitFirstNode();
    int fanIndex = -1;

    while(true){
      //detect row mismatch.  break loop if mismatch
      int currentNodeDepth = rowLength;
      int rowTokenComparison = compareToCurrentToken(key);
      if(rowTokenComparison != 0){
        return fixRowTokenMissReverse(rowTokenComparison);
      }

      //exact row found, move on to qualifier & ts
      if(rowMatchesAfterCurrentPosition(key)){
        return positionAtQualifierTimestamp(key, true);
      }

      //detect dead end (no fan to descend into)
      if(!currentRowNode.hasFan()){
        if(hasOccurrences()){//must be leaf or nub
          populateLastNonRowFields();
          return CellScannerPosition.BEFORE;
        }else{
          //TODO i don't think this case is exercised by any tests
          return fixRowFanMissReverse(0);
        }
      }

      //keep hunting for the rest of the row
      byte searchForByte = CellUtil.getRowByte(key, currentNodeDepth);
      fanIndex = currentRowNode.whichFanNode(searchForByte);
      if(fanIndex < 0){//no matching row.  return early
        int insertionPoint = -fanIndex - 1;
        return fixRowFanMissReverse(insertionPoint);
      }
      //found a match, so dig deeper into the tree
      followFan(fanIndex);
    }
  }

  /**
   * Identical workflow as positionAtOrBefore, but split them to avoid having ~10 extra
   * if-statements. Priority on readability and debugability.
   */
  @Override
  public CellScannerPosition positionAtOrAfter(Cell key) {
    reInitFirstNode();
    int fanIndex = -1;

    while(true){
      //detect row mismatch.  break loop if mismatch
      int currentNodeDepth = rowLength;
      int rowTokenComparison = compareToCurrentToken(key);
      if(rowTokenComparison != 0){
        return fixRowTokenMissForward(rowTokenComparison);
      }

      //exact row found, move on to qualifier & ts
      if(rowMatchesAfterCurrentPosition(key)){
        return positionAtQualifierTimestamp(key, false);
      }

      //detect dead end (no fan to descend into)
      if(!currentRowNode.hasFan()){
        if(hasOccurrences()){
          if (rowLength < key.getRowLength()) {
            nextRow();
          } else {
            populateFirstNonRowFields();
          }
          return CellScannerPosition.AFTER;
        }else{
          //TODO i don't think this case is exercised by any tests
          return fixRowFanMissForward(0);
        }
      }

      //keep hunting for the rest of the row
      byte searchForByte = CellUtil.getRowByte(key, currentNodeDepth);
      fanIndex = currentRowNode.whichFanNode(searchForByte);
      if(fanIndex < 0){//no matching row.  return early
        int insertionPoint = -fanIndex - 1;
        return fixRowFanMissForward(insertionPoint);
      }
      //found a match, so dig deeper into the tree
      followFan(fanIndex);
    }
  }

  @Override
  public boolean seekForwardTo(Cell key) {
    if(currentPositionIsAfter(key)){
      //our position is after the requested key, so can't do anything
      return false;
    }
    return positionAt(key);
  }

  @Override
  public CellScannerPosition seekForwardToOrBefore(Cell key) {
    //Do we even need this check or should upper layers avoid this situation.  It's relatively
    //expensive compared to the rest of the seek operation.
    if(currentPositionIsAfter(key)){
      //our position is after the requested key, so can't do anything
      return CellScannerPosition.AFTER;
    }

    return positionAtOrBefore(key);
  }

  @Override
  public CellScannerPosition seekForwardToOrAfter(Cell key) {
    //Do we even need this check or should upper layers avoid this situation.  It's relatively
    //expensive compared to the rest of the seek operation.
    if(currentPositionIsAfter(key)){
      //our position is after the requested key, so can't do anything
      return CellScannerPosition.AFTER;
    }

    return positionAtOrAfter(key);
  }

  /**
   * The content of the buffers doesn't matter here, only that afterLast=true and beforeFirst=false
   */
  @Override
  public void positionAfterLastCell() {
    resetToBeforeFirstEntry();
    beforeFirst = false;
    afterLast = true;
  }


  /***************** Object methods ***************************/

  @Override
  public boolean equals(Object obj) {
    //trivial override to confirm intent (findbugs)
    return super.equals(obj);
  }


  /****************** internal methods ************************/

  protected boolean currentPositionIsAfter(Cell cell){
    return compareTo(cell) > 0;
  }

  protected CellScannerPosition positionAtQualifierTimestamp(Cell key, boolean beforeOnMiss) {
    int minIndex = 0;
    int maxIndex = currentRowNode.getLastCellIndex();
    int diff;
    while (true) {
      int midIndex = (maxIndex + minIndex) / 2;//don't worry about overflow
      diff = populateNonRowFieldsAndCompareTo(midIndex, key);

      if (diff == 0) {// found exact match
        return CellScannerPosition.AT;
      } else if (minIndex == maxIndex) {// even termination case
        break;
      } else if ((minIndex + 1) == maxIndex) {// odd termination case
        diff = populateNonRowFieldsAndCompareTo(maxIndex, key);
        if(diff > 0){
          diff = populateNonRowFieldsAndCompareTo(minIndex, key);
        }
        break;
      } else if (diff < 0) {// keep going forward
        minIndex = currentCellIndex;
      } else {// went past it, back up
        maxIndex = currentCellIndex;
      }
    }

    if (diff == 0) {
      return CellScannerPosition.AT;

    } else if (diff < 0) {// we are before key
      if (beforeOnMiss) {
        return CellScannerPosition.BEFORE;
      }
      if (advance()) {
        return CellScannerPosition.AFTER;
      }
      return CellScannerPosition.AFTER_LAST;

    } else {// we are after key
      if (!beforeOnMiss) {
        return CellScannerPosition.AFTER;
      }
      if (previous()) {
        return CellScannerPosition.BEFORE;
      }
      return CellScannerPosition.BEFORE_FIRST;
    }
  }

  /**
   * compare this.row to key.row but starting at the current rowLength
   * @param key Cell being searched for
   * @return true if row buffer contents match key.row
   */
  protected boolean rowMatchesAfterCurrentPosition(Cell key) {
    if (!currentRowNode.hasOccurrences()) {
      return false;
    }
    int thatRowLength = key.getRowLength();
    if (rowLength != thatRowLength) {
      return false;
    }
    return true;
  }

  // TODO move part of this to Cell comparator?
  /**
   * Compare only the bytes within the window of the current token
   * @param key
   * @return return -1 if key is lessThan (before) this, 0 if equal, and 1 if key is after
   */
  protected int compareToCurrentToken(Cell key) {
    int startIndex = rowLength - currentRowNode.getTokenLength();
    int endIndexExclusive = startIndex + currentRowNode.getTokenLength();
    for (int i = startIndex; i < endIndexExclusive; ++i) {
      if (i >= key.getRowLength()) {// key was shorter, so it's first
        return -1;
      }
      byte keyByte = CellUtil.getRowByte(key, i);
      byte thisByte = rowBuffer[i];
      if (keyByte == thisByte) {
        continue;
      }
      return UnsignedBytes.compare(keyByte, thisByte);
    }
    if (!currentRowNode.hasOccurrences() && rowLength >= key.getRowLength()) { // key was shorter
        return -1;
    }
    return 0;
  }

  protected void followLastFansUntilExhausted(){
    while(currentRowNode.hasFan()){
      followLastFan();
    }
  }


  /****************** complete seek when token mismatch ******************/

  /**
   * @param searcherIsAfterInputKey &lt;0: input key is before the searcher's position<br/>
   *          &gt;0: input key is after the searcher's position
   */
  protected CellScannerPosition fixRowTokenMissReverse(int searcherIsAfterInputKey) {
    if (searcherIsAfterInputKey < 0) {//searcher position is after the input key, so back up
      boolean foundPreviousRow = previousRow(true);
      if(foundPreviousRow){
        populateLastNonRowFields();
        return CellScannerPosition.BEFORE;
      }else{
        return CellScannerPosition.BEFORE_FIRST;
      }

    }else{//searcher position is before the input key
      if(currentRowNode.hasOccurrences()){
        populateFirstNonRowFields();
        return CellScannerPosition.BEFORE;
      }
      boolean foundNextRow = nextRow();
      if(foundNextRow){
        return CellScannerPosition.AFTER;
      }else{
        return CellScannerPosition.AFTER_LAST;
      }
    }
  }

  /**
   * @param searcherIsAfterInputKey &lt;0: input key is before the searcher's position<br>
   *                   &gt;0: input key is after the searcher's position
   */
  protected CellScannerPosition fixRowTokenMissForward(int searcherIsAfterInputKey) {
    if (searcherIsAfterInputKey < 0) {//searcher position is after the input key
      if(currentRowNode.hasOccurrences()){
        populateFirstNonRowFields();
        return CellScannerPosition.AFTER;
      }
      boolean foundNextRow = nextRow();
      if(foundNextRow){
        return CellScannerPosition.AFTER;
      }else{
        return CellScannerPosition.AFTER_LAST;
      }

    }else{//searcher position is before the input key, so go forward
      discardCurrentRowNode(true);
      boolean foundNextRow = nextRow();
      if(foundNextRow){
        return CellScannerPosition.AFTER;
      }else{
        return CellScannerPosition.AFTER_LAST;
      }
    }
  }


  /****************** complete seek when fan mismatch ******************/

  protected CellScannerPosition fixRowFanMissReverse(int fanInsertionPoint){
    if(fanInsertionPoint == 0){//we need to back up a row
      if (currentRowNode.hasOccurrences()) {
        populateLastNonRowFields();
        return CellScannerPosition.BEFORE;
      }
      boolean foundPreviousRow = previousRow(true);//true -> position on last cell in row
      if(foundPreviousRow){
        populateLastNonRowFields();
        return CellScannerPosition.BEFORE;
      }
      return CellScannerPosition.BEFORE_FIRST;
    }

    //follow the previous fan, but then descend recursively forward
    followFan(fanInsertionPoint - 1);
    followLastFansUntilExhausted();
    populateLastNonRowFields();
    return CellScannerPosition.BEFORE;
  }

  protected CellScannerPosition fixRowFanMissForward(int fanInsertionPoint){
    if(fanInsertionPoint >= currentRowNode.getFanOut()){
      discardCurrentRowNode(true);
      if (!nextRow()) {
        return CellScannerPosition.AFTER_LAST;
      } else {
        return CellScannerPosition.AFTER;
      }
    }

    followFan(fanInsertionPoint);
    if(hasOccurrences()){
      populateFirstNonRowFields();
      return CellScannerPosition.AFTER;
    }

    if(nextRowInternal()){
      populateFirstNonRowFields();
      return CellScannerPosition.AFTER;

    }else{
      return CellScannerPosition.AFTER_LAST;
    }
  }

}
