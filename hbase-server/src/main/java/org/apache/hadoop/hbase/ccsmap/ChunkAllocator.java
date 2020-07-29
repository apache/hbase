/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
public class ChunkAllocator {

  private final HeapMode heapMode;
  // ID starts from maxChunkCount, so the unpooled chunks ID range is [maxChunkCount, +)
  private final AtomicLong unpooledChunkIDGenerator;
  // ID starts from 0, so the pooled chunks ID range is [0, maxChunkCount)
  private final AtomicLong pooledChunkIDGenerator = new AtomicLong(-1);

  public ChunkAllocator(HeapMode heapMode, long maxChunkCount) {
    this.heapMode = heapMode;
    unpooledChunkIDGenerator = new AtomicLong(maxChunkCount);
  }

  /**
   * Allocate a pooled chunk with specified size.
   * @param size size of a chunk
   * @return a chunk
   */
  AbstractHeapChunk allocatePooledChunk(int size) {
    return heapMode == HeapMode.ON_HEAP ?
      new OnHeapChunk(pooledChunkIDGenerator.incrementAndGet(), size) :
      new OffHeapChunk(pooledChunkIDGenerator.incrementAndGet(), size);
  }

  /**
   * Allocate a unpooled chunk with specified size.
   * @param size size of a chunk
   * @return a chunk
   */
  AbstractHeapChunk allocateUnpooledChunk(int size) {
    return new OnHeapChunk(unpooledChunkIDGenerator.getAndIncrement(), size, false);
  }

}
