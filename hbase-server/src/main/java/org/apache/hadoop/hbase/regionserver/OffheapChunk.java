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
package org.apache.hadoop.hbase.regionserver;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An off heap chunk implementation.
 */
@InterfaceAudience.Private
public class OffheapChunk extends Chunk {

  OffheapChunk(int size, int id, ChunkType chunkType) {
    // better if this is always created fromPool. This should not be called
    super(size, id, chunkType);
  }

  OffheapChunk(int size, int id, ChunkType chunkType, boolean fromPool) {
    super(size, id, chunkType, fromPool);
    assert fromPool == true;
  }

  @Override
  void allocateDataBuffer() {
    if (data == null) {
      data = ByteBuffer.allocateDirect(this.size);
      data.putInt(0, this.getId());
    }
  }
}
