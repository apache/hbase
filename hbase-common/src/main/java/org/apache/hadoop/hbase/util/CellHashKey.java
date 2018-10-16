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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extracts the byte for the hash calculation from the given cell
 */
@InterfaceAudience.Private
public abstract class CellHashKey extends HashKey<Cell> {

  protected static final byte[] LATEST_TS = Bytes.toBytes(HConstants.LATEST_TIMESTAMP);
  protected static final byte MAX_TYPE = KeyValue.Type.Maximum.getCode();

  public CellHashKey(Cell cell) {
    super(cell);
  }
}
