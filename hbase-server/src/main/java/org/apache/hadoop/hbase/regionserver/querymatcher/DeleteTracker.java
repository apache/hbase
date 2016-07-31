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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;

/**
 * This interface is used for the tracking and enforcement of Deletes during the course of a Get or
 * Scan operation.
 * <p>
 * This class is utilized through three methods:
 * <ul>
 * <li>{@link #add} when encountering a Delete</li>
 * <li>{@link #isDeleted} when checking if a Put KeyValue has been deleted</li>
 * <li>{@link #update} when reaching the end of a StoreFile</li>
 * </ul>
 */
@InterfaceAudience.Private
public interface DeleteTracker {

  /**
   * Add the specified cell to the list of deletes to check against for this row operation.
   * <p>
   * This is called when a Delete is encountered in a StoreFile.
   * @param cell - the delete cell
   */
  void add(Cell cell);

  /**
   * Check if the specified cell buffer has been deleted by a previously seen delete.
   * @param cell - current cell to check if deleted by a previously seen delete
   * @return deleteResult The result tells whether the KeyValue is deleted and why
   */
  DeleteResult isDeleted(Cell cell);

  /**
   * @return true if there are no current delete, false otherwise
   */
  boolean isEmpty();

  /**
   * Called at the end of every StoreFile.
   * <p>
   * Many optimized implementations of Trackers will require an update at when the end of each
   * StoreFile is reached.
   */
  void update();

  /**
   * Called between rows.
   * <p>
   * This clears everything as if a new DeleteTracker was instantiated.
   */
  void reset();

  /**
   * Return codes for comparison of two Deletes.
   * <p>
   * The codes tell the merging function what to do.
   * <p>
   * INCLUDE means add the specified Delete to the merged list. NEXT means move to the next element
   * in the specified list(s).
   */
  enum DeleteCompare {
    INCLUDE_OLD_NEXT_OLD,
    INCLUDE_OLD_NEXT_BOTH,
    INCLUDE_NEW_NEXT_NEW,
    INCLUDE_NEW_NEXT_BOTH,
    NEXT_OLD,
    NEXT_NEW
  }

  /**
   * Returns codes for delete result. The codes tell the ScanQueryMatcher whether the kv is deleted
   * and why. Based on the delete result, the ScanQueryMatcher will decide the next operation
   */
  enum DeleteResult {
    FAMILY_DELETED, // The KeyValue is deleted by a delete family.
    FAMILY_VERSION_DELETED, // The KeyValue is deleted by a delete family version.
    COLUMN_DELETED, // The KeyValue is deleted by a delete column.
    VERSION_DELETED, // The KeyValue is deleted by a version delete.
    NOT_DELETED
  }

}
