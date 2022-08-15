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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.AbstractByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.PooledByteBufAllocator;

/**
 * A pooled ByteBufAllocator that does not prefer direct buffers regardless of platform settings.
 * <p>
 * In some cases direct buffers are still required, like IO buffers where the buffer will be used in
 * conjunction with a native method call, so we cannot force all buffer usage on heap. But we can
 * strongly prefer it.
 */
@InterfaceAudience.Private
public class HeapByteBufAllocator extends AbstractByteBufAllocator {

  public static final HeapByteBufAllocator DEFAULT = new HeapByteBufAllocator();

  private final PooledByteBufAllocator delegate =
    new PooledByteBufAllocator(false /* preferDirect */);

  @Override
  public boolean isDirectBufferPooled() {
    return delegate.isDirectBufferPooled();
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    return delegate.heapBuffer(initialCapacity, maxCapacity);
  }

  @Override
  protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    return delegate.directBuffer(initialCapacity, maxCapacity);
  }

}
