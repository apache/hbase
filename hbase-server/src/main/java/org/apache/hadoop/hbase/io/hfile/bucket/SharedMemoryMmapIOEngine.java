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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * IO engine that stores data in pmem devices such as DCPMM. This engine also mmaps the file from
 * the given path. But note that this path has to be a path on the pmem device so that when mmapped
 * the file's address is mapped to the Pmem's address space and not in the DRAM. Since this address
 * space is exclusive for the Pmem device there is no swapping out of the mmapped contents that
 * generally happens when DRAM's free space is not enough to hold the specified file's mmapped
 * contents. This gives us the option of using the {@code MemoryType#SHARED} type when serving the
 * data from this pmem address space. We need not copy the blocks to the onheap space as we need to
 * do for the case of {@code ExclusiveMemoryMmapIOEngine}.
 */
@InterfaceAudience.Private
public class SharedMemoryMmapIOEngine extends FileMmapIOEngine {

  // TODO this will support only one path over Pmem. To make use of multiple Pmem devices mounted,
  // we need to support multiple paths like files IOEngine. Support later.
  public SharedMemoryMmapIOEngine(String filePath, long capacity) throws IOException {
    super(filePath, capacity);
  }

  @Override
  public boolean usesSharedMemory() {
    return true;
  }

  @Override
  public Cacheable read(long offset, int length, CacheableDeserializer<Cacheable> deserializer)
      throws IOException {
    ByteBuff dstBuffer = bufferArray.asSubByteBuff(offset, length);
    // Here the buffer that is created directly refers to the buffer in the actual buckets.
    // When any cell is referring to the blocks created out of these buckets then it means that
    // those cells are referring to a shared memory area which if evicted by the BucketCache would
    // lead to corruption of results. Hence we set the type of the buffer as SHARED_MEMORY
    // so that the readers using this block are aware of this fact and do the necessary action
    // to prevent eviction till the results are either consumed or copied
    return deserializer.deserialize(dstBuffer, true, MemoryType.SHARED);
  }
}
