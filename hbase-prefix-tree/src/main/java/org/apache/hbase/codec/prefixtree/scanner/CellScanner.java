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

package org.apache.hbase.codec.prefixtree.scanner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hbase.Cell;

/**
 * Alternate name may be CellInputStream
 * <p/>
 * An interface for iterating through a sequence of cells. Similar to Java's Iterator, but without
 * the hasNext() or remove() methods. The hasNext() method is problematic because it may require
 * actually loading the next object, which in turn requires storing the previous object somewhere.
 * The core data block decoder should be as fast as possible, so we push the complexity and
 * performance expense of concurrently tracking multiple cells to layers above the CellScanner.
 * <p/>
 * The getCurrentCell() method will return a reference to a Cell implementation. This reference may
 * or may not point to a reusable cell implementation, so users of the CellScanner should not, for
 * example, accumulate a List of Cells. All of the references may point to the same object, which
 * would be the latest state of the underlying Cell. In short, the Cell is mutable.
 * <p/>
 * At a minimum, an implementation will need to be able to advance from one cell to the next in a
 * LinkedList fashion. The nextQualifier(), nextFamily(), and nextRow() methods can all be
 * implemented by calling nextCell(), however, if the DataBlockEncoding supports random access into
 * the block then it may provide smarter versions of these methods.
 * <p/>
 * Typical usage:
 * 
 * <pre>
 * while (scanner.nextCell()) {
 *   Cell cell = scanner.getCurrentCell();
 *   // do something
 * }
 * </pre>
 */
@InterfaceAudience.Private
public interface CellScanner{

  /**
   * Reset any state in the scanner so it appears it was freshly opened.
   */
  void resetToBeforeFirstEntry();

  /**
   * @return the current Cell which may be mutable
   */
  Cell getCurrent();

  /**
   * Advance the scanner 1 cell.
   * @return true if the next cell is found and getCurrentCell() will return a valid Cell
   */
  boolean next();

}
