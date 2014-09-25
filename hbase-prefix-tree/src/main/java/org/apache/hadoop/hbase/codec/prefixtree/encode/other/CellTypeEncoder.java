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

package org.apache.hadoop.hbase.codec.prefixtree.encode.other;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Detect if every KV has the same KeyValue.Type, in which case we don't need to store it for each
 * KV.  If(allSameType) during conversion to byte[], then we can store the "onlyType" in blockMeta,
 * therefore not repeating it for each cell and saving 1 byte per cell.
 */
@InterfaceAudience.Private
public class CellTypeEncoder {

  /************* fields *********************/

  protected boolean pendingFirstType = true;
  protected boolean allSameType = true;
  protected byte onlyType;


  /************* construct *********************/

  public void reset() {
    pendingFirstType = true;
    allSameType = true;
  }


  /************* methods *************************/

  public void add(byte type) {
    if (pendingFirstType) {
      onlyType = type;
      pendingFirstType = false;
    } else if (onlyType != type) {
      allSameType = false;
    }
  }


  /**************** get/set **************************/

  public boolean areAllSameType() {
    return allSameType;
  }

  public byte getOnlyType() {
    return onlyType;
  }

}
