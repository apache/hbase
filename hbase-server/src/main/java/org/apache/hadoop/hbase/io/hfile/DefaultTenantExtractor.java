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

import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
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
  private static final int MAX_PREFIX_LENGTH = Short.MAX_VALUE;

  public DefaultTenantExtractor(int prefixLength) {
    if (prefixLength <= 0 || prefixLength > MAX_PREFIX_LENGTH) {
      throw new IllegalArgumentException(
        "Tenant prefix length must be in [1, " + MAX_PREFIX_LENGTH + "], got: " + prefixLength);
    }
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
   * @return The tenant prefix as a byte array, or null if the row key is shorter than the
   *         configured prefix length (e.g., synthetic seek keys, bloom filter probes)
   */
  private byte[] extractPrefix(Cell cell) {
    Objects.requireNonNull(cell, "cell must not be null");
    int rowLength = cell.getRowLength();
    if (rowLength < prefixLength) {
      return null;
    }

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

  /**
   * Zero-allocation comparison: checks whether the cell's row prefix matches the given section ID
   * by comparing directly from the cell's backing byte array.
   */
  @Override
  public boolean matchesTenantSectionId(Cell cell, byte[] currentSectionId) {
    if (cell == null || currentSectionId == null) {
      return false;
    }
    int rowLength = cell.getRowLength();
    if (rowLength < prefixLength || currentSectionId.length != prefixLength) {
      return false;
    }
    return org.apache.hadoop.hbase.util.Bytes.equals(cell.getRowArray(), cell.getRowOffset(),
      prefixLength, currentSectionId, 0, prefixLength);
  }
}
