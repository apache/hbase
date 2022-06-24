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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.buffer.AbstractByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;

/**
 * A custom byte buf allocator for TestNettyRpcServer.
 */
public class SimpleByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocator {

  static final Logger LOG = LoggerFactory.getLogger(SimpleByteBufAllocator.class);

  @Override
  public boolean isDirectBufferPooled() {
    return false;
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    LOG.info("newHeapBuffer initialCapacity={}, maxCapacity={}", initialCapacity, maxCapacity);
    return Unpooled.buffer(initialCapacity, maxCapacity);
  }

  @Override
  protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    LOG.info("newDirectBuffer initialCapacity={}, maxCapacity={}", initialCapacity, maxCapacity);
    return Unpooled.directBuffer(initialCapacity, maxCapacity);
  }

}
