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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Strategy interface for extracting tenant information from cells
 * following SOLID's Interface Segregation Principle.
 */
@InterfaceAudience.Private
public interface TenantExtractor {
  /**
   * Extract tenant prefix from a cell
   * @param cell The cell to extract tenant information from
   * @return The tenant prefix as a byte array
   */
  byte[] extractTenantPrefix(Cell cell);
  
  /**
   * Check if the tenant prefix has changed from the previous cell
   * @param previousCell The previous cell or null if first cell
   * @param currentCell The current cell
   * @return true if tenant prefix has changed, false otherwise
   */
  boolean hasTenantPrefixChanged(Cell previousCell, Cell currentCell);
} 