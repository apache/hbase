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
 * Strategy interface for extracting tenant information from cells following SOLID's Interface
 * Segregation Principle.
 */
@InterfaceAudience.Private
public interface TenantExtractor {
  /**
   * Extract tenant ID from a cell
   * @param cell The cell to extract tenant information from
   * @return The tenant ID as a byte array
   */
  byte[] extractTenantId(Cell cell);

  /**
   * Extract tenant section ID from a cell for use in section index blocks
   * @param cell The cell to extract tenant section information from
   * @return The tenant section ID as a byte array
   */
  byte[] extractTenantSectionId(Cell cell);

  /**
   * Get the tenant prefix length used for extraction
   * @return The length of the tenant prefix in bytes
   */
  int getPrefixLength();
}
