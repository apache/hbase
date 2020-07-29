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

package org.apache.hadoop.hbase.ccsmap;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A chunk of heap memory out of which allocations are sliced.
 */
@InterfaceAudience.Private
public interface HeapChunk {

  /**
   * @return ID of a Chunk
   */
  long getChunkID();

  /**
   * @return offset of this Chunk
   */
  int getPosition();

  /**
   * Try to allocate len bytes from the chunk. Note, alignment will happen.
   * @return the start offset of the successful allocation, or -1 to indicate not-enough-space
   */
  int allocate(int len);

  /**
   * @return the total len of this Chunk.
   */
  int getLimit();

  /**
   * @return if this Chunk is a pooled
   */
  boolean isPooledChunk();

  /**
   * @return This chunk's backing ByteBuffer.
   */
  ByteBuffer getByteBuffer();

  /**
   * Creates a new byte buffer that shares this buffer's content.
   * @param offset start offset
   * @param len share length
   * @return a ByteBuffer
   */
  ByteBuffer asSubByteBuffer(int offset, int len);

  /**
   * @return number of free space in bytes, after alignment
   */
  int occupancy();

}
