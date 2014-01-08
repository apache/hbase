/*
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.io.hfile.bucket;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.RawHFileBlock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Block Entry stored in the memory with key,data and so on
 */
class RAMQueueEntry implements HeapSize {
  private BlockCacheKey key;
  private RawHFileBlock block;
  private long accessTime;
  private boolean inMemory;

  public final static long RAM_QUEUE_ENTRY_OVERHEAD =
          ClassSize.OBJECT +
          // key, block
          2 * ClassSize.REFERENCE +
          // accessTime
          Bytes.SIZEOF_LONG +
          // inMemory
          Bytes.SIZEOF_BOOLEAN;

  public RAMQueueEntry(BlockCacheKey bck, RawHFileBlock block, long accessTime,
                       boolean inMemory) {
    this.key = bck;
    this.block = block;
    this.accessTime = accessTime;
    this.inMemory = inMemory;
  }

  public BlockCacheKey getKey() {
    return key;
  }

  public RawHFileBlock getRawHFileBlock() {
    return block;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public boolean isInMemory() {
    return inMemory;
  }

  public void access(long accessTime) {
    this.accessTime = accessTime;
  }

  @Override
  public long heapSize() {
    return ClassSize.align(RAM_QUEUE_ENTRY_OVERHEAD);
  }
}
