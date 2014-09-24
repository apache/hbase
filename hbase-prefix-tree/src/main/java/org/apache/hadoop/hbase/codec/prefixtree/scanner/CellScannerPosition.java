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

/**
 * An indicator of the state of the scanner after an operation such as nextCell() or
 * positionAt(..). For example:
 * <ul>
 * <li>In a DataBlockScanner, the AFTER_LAST position indicates to the parent StoreFileScanner that
 * it should load the next block.</li>
 * <li>In a StoreFileScanner, the AFTER_LAST position indicates that the file has been exhausted.
 * </li>
 * <li>In a RegionScanner, the AFTER_LAST position indicates that the scanner should move to the
 * next region.</li>
 * </ul>
 */
@InterfaceAudience.Private
public enum CellScannerPosition {

  /**
   * getCurrentCell() will NOT return a valid cell. Calling nextCell() will advance to the first
   * cell.
   */
  BEFORE_FIRST,

  /**
   * getCurrentCell() will return a valid cell, but it is not the cell requested by positionAt(..),
   * rather it is the nearest cell before the requested cell.
   */
  BEFORE,

  /**
   * getCurrentCell() will return a valid cell, and it is exactly the cell that was requested by
   * positionAt(..).
   */
  AT,

  /**
   * getCurrentCell() will return a valid cell, but it is not the cell requested by positionAt(..),
   * rather it is the nearest cell after the requested cell.
   */
  AFTER,

  /**
   * getCurrentCell() will NOT return a valid cell. Calling nextCell() will have no effect.
   */
  AFTER_LAST

}
