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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Chain of ByteBuffers. Used writing out an array of byte buffers. Writes in chunks.
 */
@InterfaceAudience.Private
class BufferChain {
  private final ByteBuffer[] buffers;
  private int remaining = 0;
  private int bufferOffset = 0;
  private int size;

  BufferChain(ByteBuffer... buffers) {
    for (ByteBuffer b : buffers) {
      this.remaining += b.remaining();
    }
    this.size = remaining;
    this.buffers = buffers;
  }

  /**
   * Expensive. Makes a new buffer to hold a copy of what is in contained ByteBuffers. This call
   * drains this instance; it cannot be used subsequent to the call.
   * @return A new byte buffer with the content of all contained ByteBuffers.
   */
  byte[] getBytes() {
    if (!hasRemaining()) throw new IllegalAccessError();
    byte[] bytes = new byte[this.remaining];
    int offset = 0;
    for (ByteBuffer bb : this.buffers) {
      int length = bb.remaining();
      bb.get(bytes, offset, length);
      offset += length;
    }
    return bytes;
  }

  boolean hasRemaining() {
    return remaining > 0;
  }

  /**
   * Write out our chain of buffers in chunks
   * @param channel   Where to write
   * @param chunkSize Size of chunks to write.
   * @return Amount written. n
   */
  long write(GatheringByteChannel channel, int chunkSize) throws IOException {
    int chunkRemaining = chunkSize;
    ByteBuffer lastBuffer = null;
    int bufCount = 0;
    int restoreLimit = -1;

    while (chunkRemaining > 0 && bufferOffset + bufCount < buffers.length) {
      lastBuffer = buffers[bufferOffset + bufCount];
      if (!lastBuffer.hasRemaining()) {
        bufferOffset++;
        continue;
      }
      bufCount++;
      if (lastBuffer.remaining() > chunkRemaining) {
        restoreLimit = lastBuffer.limit();
        lastBuffer.limit(lastBuffer.position() + chunkRemaining);
        chunkRemaining = 0;
        break;
      } else {
        chunkRemaining -= lastBuffer.remaining();
      }
    }
    assert lastBuffer != null;
    if (chunkRemaining == chunkSize) {
      assert !hasRemaining();
      // no data left to write
      return 0;
    }
    try {
      long ret = channel.write(buffers, bufferOffset, bufCount);
      if (ret > 0) {
        remaining = (int) (remaining - ret);
      }
      return ret;
    } finally {
      if (restoreLimit >= 0) {
        lastBuffer.limit(restoreLimit);
      }
    }
  }

  int size() {
    return size;
  }

  ByteBuffer[] getBuffers() {
    return this.buffers;
  }
}
