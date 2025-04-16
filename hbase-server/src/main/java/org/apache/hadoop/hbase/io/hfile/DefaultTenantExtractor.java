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
 * Default implementation of TenantExtractor that extracts tenant information
 * based on configurable prefix length and offset in row keys.
 */
@InterfaceAudience.Private
public class DefaultTenantExtractor implements TenantExtractor {
  private final int prefixLength;
  private final int prefixOffset;
  
  public DefaultTenantExtractor(int prefixLength, int prefixOffset) {
    this.prefixLength = prefixLength;
    this.prefixOffset = prefixOffset;
  }
  
  @Override
  public byte[] extractTenantPrefix(Cell cell) {
    if (prefixLength <= 0) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    
    // Get row length and ensure it's sufficient
    int rowLength = cell.getRowLength();
    if (rowLength < prefixOffset + prefixLength) {
      throw new IllegalArgumentException("Row key too short for configured prefix parameters. " +
          "Row key length: " + rowLength + ", required: " + (prefixOffset + prefixLength));
    }
    
    byte[] prefix = new byte[prefixLength];
    // Copy directly from the cell's row bytes
    System.arraycopy(cell.getRowArray(), cell.getRowOffset() + prefixOffset, 
                    prefix, 0, prefixLength);
    return prefix;
  }
  
  
  /**
   * Get the tenant prefix length.
   * @return The configured tenant prefix length
   */
  public int getPrefixLength() {
    return prefixLength;
  }
  
  /**
   * Get the tenant prefix offset.
   * @return The configured tenant prefix offset
   */
  public int getPrefixOffset() {
    return prefixOffset;
  }
} 