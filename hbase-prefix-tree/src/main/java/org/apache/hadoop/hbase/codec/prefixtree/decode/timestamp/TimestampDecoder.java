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

package org.apache.hadoop.hbase.codec.prefixtree.decode.timestamp;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.util.vint.UFIntTool;

/**
 * Given a block and its blockMeta, this will decode the timestamp for the i-th Cell in the block.
 */
@InterfaceAudience.Private
public class TimestampDecoder {

  protected PrefixTreeBlockMeta blockMeta;
  protected byte[] block;


  /************** construct ***********************/

  public TimestampDecoder() {
  }

  public void initOnBlock(PrefixTreeBlockMeta blockMeta, byte[] block) {
    this.block = block;
    this.blockMeta = blockMeta;
  }


  /************** methods *************************/

  public long getLong(int index) {
    if (blockMeta.getTimestampIndexWidth() == 0) {//all timestamps in the block were identical
      return blockMeta.getMinTimestamp();
    }
    int startIndex = blockMeta.getAbsoluteTimestampOffset() + blockMeta.getTimestampDeltaWidth()
        * index;
    long delta = UFIntTool.fromBytes(block, startIndex, blockMeta.getTimestampDeltaWidth());
    return blockMeta.getMinTimestamp() + delta;
  }
}
