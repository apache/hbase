package org.apache.hadoop.hbase.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.nio.ByteBuffer;

/**
 * Wraps the memory allocated by the Arena. It provides the abstraction of a
 * ByteBuffer to the outside world.
 */
public class MemoryBuffer {

  private final long offset;
  private final int size;
  private final ByteBuffer buffer;
  public static final int UNDEFINED_OFFSET = -1;

  public MemoryBuffer(final ByteBuffer buffer, long offset, int size) {
    this.offset = offset;
    this.size = size;
    this.buffer = buffer;
  }

  public MemoryBuffer(ByteBuffer buffer) {
    this.offset = UNDEFINED_OFFSET;
    this.size = buffer.remaining();
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public long getOffset() {
    return offset;
  }

  public int getSize() {
    return size;
  }

  public void flip() {
    if (offset != UNDEFINED_OFFSET) {
      buffer.limit(buffer.position());
      buffer.position((int)offset);
    } else {
      buffer.flip();
    }
  }
}
