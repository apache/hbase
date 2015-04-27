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
import org.apache.hadoop.hbase.CellScanner;

/**
 * An extension of CellScanner indicating the scanner supports iterating backwards through cells.
 * <p>
 * Note: This was not added to suggest that HBase should support client facing reverse Scanners,
 * but
 * because some {@link CellSearcher} implementations, namely PrefixTree, need a method of backing
 * up if the positionAt(..) method goes past the requested cell.
 */
@InterfaceAudience.Private
public interface ReversibleCellScanner extends CellScanner {

  /**
   * Try to position the scanner one Cell before the current position.
   * @return true if the operation was successful, meaning getCurrentCell() will return a valid
   *         Cell.<br>
   *         false if there were no previous cells, meaning getCurrentCell() will return null.
   *         Scanner position will be
   *         {@link org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition#BEFORE_FIRST}
   */
  boolean previous();

  /**
   * Try to position the scanner in the row before the current row.
   * @param endOfRow true for the last cell in the previous row; false for the first cell
   * @return true if the operation was successful, meaning getCurrentCell() will return a valid
   *         Cell.<br>
   *         false if there were no previous cells, meaning getCurrentCell() will return null.
   *         Scanner position will be
   *         {@link org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition#BEFORE_FIRST}
   */
  boolean previousRow(boolean endOfRow);
}
