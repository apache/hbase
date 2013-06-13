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

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Block Entry stored in the memory with key,data and so on
 */
class RAMQueueEntry {
  private BlockCacheKey key;
  private byte[] data;
  private long accessTime;
  private boolean inMemory;

  public RAMQueueEntry(BlockCacheKey bck, byte[] data, long accessTime,
      boolean inMemory) {
    this.key = bck;
    this.data = data;
    this.accessTime = accessTime;
    this.inMemory = inMemory;
  }

  public byte[] getData() {
    return data;
  }

  public BlockCacheKey getKey() {
    return key;
  }

  public void access(long accessTime) {
    this.accessTime = accessTime;
  }

  public BucketCache.BucketEntry writeToCache(final IOEngine ioEngine,
      final BucketAllocator bucketAllocator,
      final AtomicLong realCacheSize) throws CacheFullException, IOException,
      BucketAllocatorException {
    int len = data.length;
    if (len == 0) {
      return null;
    }
    long offset = bucketAllocator.allocateBlock(len);
    BucketCache.BucketEntry bucketEntry = new BucketCache.BucketEntry(offset, len, accessTime,
        inMemory);
    try {
      ioEngine.write(data, offset);
    } catch (IOException ioe) {
      // free it in bucket allocator
      bucketAllocator.freeBlock(offset);
      throw ioe;
    }

    realCacheSize.addAndGet(len);
    return bucketEntry;
  }
}
