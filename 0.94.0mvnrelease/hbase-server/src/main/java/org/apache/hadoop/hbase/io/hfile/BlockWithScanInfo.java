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
package org.apache.hadoop.hbase.io.hfile;

/**
 * BlockWithScanInfo is wrapper class for HFileBlock with other attributes. These attributes are
 * supposed to be much cheaper to be maintained in each caller thread than in HFileBlock itself.
 */
public class BlockWithScanInfo {
  private final HFileBlock hFileBlock;
  /**
   * The first key in the next block following this one in the HFile.
   * If this key is unknown, this is reference-equal with HConstants.NO_NEXT_INDEXED_KEY
   */
  private final byte[] nextIndexedKey;

  public BlockWithScanInfo(HFileBlock hFileBlock, byte[] nextIndexedKey) {
    this.hFileBlock = hFileBlock;
    this.nextIndexedKey = nextIndexedKey;
  }

  public HFileBlock getHFileBlock() {
    return hFileBlock;
  }

  public byte[] getNextIndexedKey() {
    return nextIndexedKey;
  }
}
