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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
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
    
    byte[] rowKey = CellUtil.cloneRow(cell);
    if (rowKey.length < prefixOffset + prefixLength) {
      throw new IllegalArgumentException("Row key too short for configured prefix parameters. " +
          "Row key length: " + rowKey.length + ", required: " + (prefixOffset + prefixLength));
    }
    
    byte[] prefix = new byte[prefixLength];
    System.arraycopy(rowKey, prefixOffset, prefix, 0, prefixLength);
    return prefix;
  }
  
  @Override
  public boolean hasTenantPrefixChanged(Cell previousCell, Cell currentCell) {
    if (previousCell == null) {
      return false;
    }
    
    if (prefixLength <= 0) {
      return false;
    }
    
    // Extract tenant prefixes and compare them
    byte[] prevPrefix = extractTenantPrefix(previousCell);
    byte[] currPrefix = extractTenantPrefix(currentCell);
    
    return !Bytes.equals(prevPrefix, currPrefix);
  }
} 