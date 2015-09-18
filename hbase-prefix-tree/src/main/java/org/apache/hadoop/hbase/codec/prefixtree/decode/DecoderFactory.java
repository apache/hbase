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

package org.apache.hadoop.hbase.codec.prefixtree.decode;


import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;
import org.apache.hadoop.hbase.nio.ByteBuff;
/**
 * Static wrapper class for the ArraySearcherPool.
 */
@InterfaceAudience.Private
public class DecoderFactory {
  private static final ArraySearcherPool POOL = new ArraySearcherPool();

  //TODO will need a PrefixTreeSearcher on top of CellSearcher
  public static PrefixTreeArraySearcher checkOut(final ByteBuff buffer, 
      boolean includeMvccVersion) {
    PrefixTreeArraySearcher searcher = POOL.checkOut(buffer,
      includeMvccVersion);
    return searcher;
  }

  public static void checkIn(CellSearcher pSearcher) {
    if (pSearcher == null) {
      return;
    }
    if (! (pSearcher instanceof PrefixTreeArraySearcher)) {
      throw new IllegalArgumentException("Cannot return "+pSearcher.getClass()+" to "
          +DecoderFactory.class);
    }
    PrefixTreeArraySearcher searcher = (PrefixTreeArraySearcher) pSearcher;
    POOL.checkIn(searcher);
  }


  /**************************** helper ******************************/
  public static PrefixTreeArraySearcher ensureArraySearcherValid(ByteBuff buffer,
      PrefixTreeArraySearcher searcher, boolean includeMvccVersion) {
    if (searcher == null) {
      PrefixTreeBlockMeta blockMeta = new PrefixTreeBlockMeta(buffer);
      searcher = new PrefixTreeArraySearcher(blockMeta, blockMeta.getRowTreeDepth(),
          blockMeta.getMaxRowLength(), blockMeta.getMaxQualifierLength(),
          blockMeta.getMaxTagsLength());
      searcher.initOnBlock(blockMeta, buffer, includeMvccVersion);
      return searcher;
    }

    PrefixTreeBlockMeta blockMeta = searcher.getBlockMeta();
    blockMeta.initOnBlock(buffer);
    if (!searcher.areBuffersBigEnough()) {
      int maxRowTreeStackNodes = Math.max(blockMeta.getRowTreeDepth(),
        searcher.getMaxRowTreeStackNodes());
      int rowBufferLength = Math.max(blockMeta.getMaxRowLength(), searcher.getRowBufferLength());
      int qualifierBufferLength = Math.max(blockMeta.getMaxQualifierLength(),
        searcher.getQualifierBufferLength());
      int tagBufferLength = Math.max(blockMeta.getMaxTagsLength(), searcher.getTagBufferLength());
      searcher = new PrefixTreeArraySearcher(blockMeta, maxRowTreeStackNodes, rowBufferLength,
          qualifierBufferLength, tagBufferLength);
    }
    //this is where we parse the BlockMeta
    searcher.initOnBlock(blockMeta, buffer, includeMvccVersion);
    return searcher;
  }

}
