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
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default implementation of TenantExtractor that extracts tenant information based on configurable
 * prefix length at the beginning of row keys.
 */
@InterfaceAudience.Private
public class DefaultTenantExtractor implements TenantExtractor {
  /** The length of the tenant prefix to extract */
  private final int prefixLength;

  /**
   * Constructor for DefaultTenantExtractor.
   * @param prefixLength the length of the tenant prefix to extract from row keys
   */
  public DefaultTenantExtractor(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  @Override
  public byte[] extractTenantId(Cell cell) {
    return extractPrefix(cell);
  }

  @Override
  public byte[] extractTenantSectionId(Cell cell) {
    // Tenant section ID is same as tenant ID
    return extractPrefix(cell);
  }

  /**
   * Extract tenant prefix from a cell.
   * @param cell The cell to extract tenant information from
   * @return The tenant prefix as a byte array
   */
  private byte[] extractPrefix(Cell cell) {
    if (prefixLength <= 0) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    // Get row length and ensure it's sufficient
    int rowLength = cell.getRowLength();
    if (rowLength < prefixLength) {
      throw new IllegalArgumentException("Row key too short for configured prefix length. "
        + "Row key length: " + rowLength + ", required: " + prefixLength);
    }

    // Create and populate result array - always from start of row
    byte[] prefix = new byte[prefixLength];
    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), prefix, 0, prefixLength);
    return prefix;
  }

  /**
   * Get the tenant prefix length.
   * @return The configured tenant prefix length
   */
  @Override
  public int getPrefixLength() {
    return prefixLength;
  }
}
