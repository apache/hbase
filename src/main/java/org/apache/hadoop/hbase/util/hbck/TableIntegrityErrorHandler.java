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

package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.util.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.TableInfo;

/**
 * This interface provides callbacks for handling particular table integrity
 * invariant violations.  This could probably be boiled down to handling holes
 * and handling overlaps but currently preserves the older more specific error
 * condition codes.
 */
public interface TableIntegrityErrorHandler {

  TableInfo getTableInfo();

  /**
   * Set the TableInfo used by all HRegionInfos fabricated by other callbacks
   */
  void setTableInfo(TableInfo ti);

  /**
   * Callback for handling case where a Table has a first region that does not
   * have an empty start key.
   *
   * @param hi An HbckInfo of the second region in a table.  This should have
   *    a non-empty startkey, and can be used to fabricate a first region that
   *    has an empty start key.
   */
  void handleRegionStartKeyNotEmpty(HbckInfo hi) throws IOException;
  
  /**
   * Callback for handling case where a Table has a last region that does not
   * have an empty end key.
   *
   * @param curEndKey The end key of the current last region. There should be a new region
   *    with start key as this and an empty end key.
   */
  void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException;

  /**
   * Callback for handling a region that has the same start and end key.
   *
   * @param hi An HbckInfo for a degenerate key.
   */
  void handleDegenerateRegion(HbckInfo hi) throws IOException;

  /**
   * Callback for handling two regions that have the same start key.  This is
   * a specific case of a region overlap.
   */
  void handleDuplicateStartKeys(HbckInfo hi1, HbckInfo hi2) throws IOException;

  /**
   * Callback for handling two reigons that overlap in some arbitrary way.
   * This is a specific case of region overlap, and called for each possible
   * pair. If two regions have the same start key, the handleDuplicateStartKeys
   * method is called.
   */
  void handleOverlapInRegionChain(HbckInfo hi1, HbckInfo hi2)
      throws IOException;

  /**
   * Callback for handling a region hole between two keys.
   */
  void handleHoleInRegionChain(byte[] holeStart, byte[] holeEnd)
      throws IOException;

  /**
   * Callback for handling an group of regions that overlap.
   */
  void handleOverlapGroup(Collection<HbckInfo> overlap) throws IOException;
}
