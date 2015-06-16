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

package org.apache.hadoop.hbase.codec.prefixtree.scanner;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;

/**
 * Methods for seeking to a random {@link Cell} inside a sorted collection of cells. Indicates that
 * the implementation is able to navigate between cells without iterating through every cell.
 */
@InterfaceAudience.Private
public interface CellSearcher extends ReversibleCellScanner {
  /**
   * Reset any state in the scanner so it appears it was freshly opened.
   */
  void resetToBeforeFirstEntry();

  /**
   * <p>
   * Do everything within this scanner's power to find the key. Look forward and backwards.
   * </p>
   * <p>
   * Abort as soon as we know it can't be found, possibly leaving the Searcher in an invalid state.
   * </p>
   * @param key position the CellScanner exactly on this key
   * @return true if the cell existed and getCurrentCell() holds a valid cell
   */
  boolean positionAt(Cell key);

  /**
   * <p>
   * Same as positionAt(..), but go to the extra effort of finding the previous key if there's no
   * exact match.
   * </p>
   * @param key position the CellScanner on this key or the closest cell before
   * @return AT if exact match<br/>
   *         BEFORE if on last cell before key<br/>
   *         BEFORE_FIRST if key was before the first cell in this scanner's scope
   */
  CellScannerPosition positionAtOrBefore(Cell key);

  /**
   * <p>
   * Same as positionAt(..), but go to the extra effort of finding the next key if there's no exact
   * match.
   * </p>
   * @param key position the CellScanner on this key or the closest cell after
   * @return AT if exact match<br/>
   *         AFTER if on first cell after key<br/>
   *         AFTER_LAST if key was after the last cell in this scanner's scope
   */
  CellScannerPosition positionAtOrAfter(Cell key);

  /**
   * <p>
   * Note: Added for backwards compatibility with
   * {@link org.apache.hadoop.hbase.regionserver.KeyValueScanner#reseek}
   * </p><p>
   * Look for the key, but only look after the current position. Probably not needed for an
   * efficient tree implementation, but is important for implementations without random access such
   * as unencoded KeyValue blocks.
   * </p>
   * @param key position the CellScanner exactly on this key
   * @return true if getCurrent() holds a valid cell
   */
  boolean seekForwardTo(Cell key);

  /**
   * <p>
   * Same as seekForwardTo(..), but go to the extra effort of finding the next key if there's no
   * exact match.
   * </p>
   * @param key
   * @return AT if exact match<br>
   *         AFTER if on first cell after key<br>
   *         AFTER_LAST if key was after the last cell in this scanner's scope
   */
  CellScannerPosition seekForwardToOrBefore(Cell key);

  /**
   * <p>
   * Same as seekForwardTo(..), but go to the extra effort of finding the next key if there's no
   * exact match.
   * </p>
   * @param key
   * @return AT if exact match<br>
   *         AFTER if on first cell after key<br>
   *         AFTER_LAST if key was after the last cell in this scanner's scope
   */
  CellScannerPosition seekForwardToOrAfter(Cell key);

  /**
   * <p>
   * Note: This may not be appropriate to have in the interface.  Need to investigate.
   * </p>
   * Position the scanner in an invalid state after the last cell: CellScannerPosition.AFTER_LAST.
   * This is used by tests and for handling certain edge cases.
   */
  void positionAfterLastCell();

}
