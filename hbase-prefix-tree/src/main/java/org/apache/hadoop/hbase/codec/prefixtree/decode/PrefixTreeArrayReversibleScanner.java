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
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.ReversibleCellScanner;

/**
 * Methods for going backwards through a PrefixTree block.  This class is split out on its own to
 * simplify the Scanner superclass and Searcher subclass.
 */
@InterfaceAudience.Private
public class PrefixTreeArrayReversibleScanner extends PrefixTreeArrayScanner implements
    ReversibleCellScanner {

  /***************** construct ******************************/

  public PrefixTreeArrayReversibleScanner(PrefixTreeBlockMeta blockMeta, int rowTreeDepth,
      int rowBufferLength, int qualifierBufferLength, int tagsBufferLength) {
    super(blockMeta, rowTreeDepth, rowBufferLength, qualifierBufferLength, tagsBufferLength);
  }


  /***************** Object methods ***************************/

  @Override
  public boolean equals(Object obj) {
    //trivial override to confirm intent (findbugs)
    return super.equals(obj);
  }


  /***************** methods **********************************/

  @Override
  public boolean previous() {
    if (afterLast) {
      afterLast = false;
      positionAtLastCell();
      return true;
    }
    if (beforeFirst) {
      return false;
    }
    if (isFirstCellInRow()) {
      previousRowInternal();
      if (beforeFirst) {
        return false;
      }
      populateLastNonRowFields();
      return true;
    }
    populatePreviousNonRowFields();
    return true;
  }

  @Override
  public boolean previousRow(boolean endOfRow) {
    previousRowInternal();
    if(beforeFirst){
      return false;
    }
    if(endOfRow){
      populateLastNonRowFields();
    }else{
      populateFirstNonRowFields();
    }
    return true;
  }

  private boolean previousRowInternal() {
    if (beforeFirst) {
      return false;
    }
    if (afterLast) {
      positionAtLastRow();
      return true;
    }
    if (currentRowNode.hasOccurrences()) {
      discardCurrentRowNode(false);
      if(currentRowNode==null){
        return false;
      }
    }
    while (!beforeFirst) {
      if (isDirectlyAfterNub()) {//we are about to back up to the nub
        currentRowNode.resetFanIndex();//sets it to -1, which is before the first leaf
        nubCellsRemain = true;//this positions us on the nub
        return true;
      }
      if (currentRowNode.hasPreviousFanNodes()) {
        followPreviousFan();
        descendToLastRowFromCurrentPosition();
      } else {// keep going up the stack until we find previous fan positions
        discardCurrentRowNode(false);
        if(currentRowNode==null){
          return false;
        }
      }
      if (currentRowNode.hasOccurrences()) {// escape clause
        return true;// found some values
      }
    }
    return false;// went past the beginning
  }
  
  protected boolean isDirectlyAfterNub() {
    return currentRowNode.isNub() && currentRowNode.getFanIndex()==0;
  }

  protected void positionAtLastRow() {
    reInitFirstNode();
    descendToLastRowFromCurrentPosition();
  }

  protected void descendToLastRowFromCurrentPosition() {
    while (currentRowNode.hasChildren()) {
      followLastFan();
    }
  }

  protected void positionAtLastCell() {
    positionAtLastRow();
    populateLastNonRowFields();
  }

}
