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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A cell implementing this interface would mean that the memory area backing this cell will refer
 * to a memory area that could be part of a larger common memory area used by the
 * RegionServer. If an exclusive instance is required, use the {@link #cloneToCell()} to have the
 * contents of the cell copied to an exclusive memory area.
 */
@InterfaceAudience.Private
public interface ShareableMemory {
  /**
   * Does a deep copy of the contents to a new memory area and
   * returns it in the form of a cell.
   * @return Cell the deep cloned cell
   */
  public Cell cloneToCell();
}