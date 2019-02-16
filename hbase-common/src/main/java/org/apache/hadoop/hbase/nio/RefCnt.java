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
package org.apache.hadoop.hbase.nio;

import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.AbstractReferenceCounted;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCounted;

/**
 * Maintain an reference count integer inside to track life cycle of {@link ByteBuff}, if the
 * reference count become 0, it'll call {@link Recycler#free()} once.
 */
@InterfaceAudience.Private
class RefCnt extends AbstractReferenceCounted {

  private Recycler recycler = ByteBuffAllocator.NONE;

  RefCnt(Recycler recycler) {
    this.recycler = recycler;
  }

  @Override
  protected final void deallocate() {
    this.recycler.free();
  }

  @Override
  public final ReferenceCounted touch(Object hint) {
    throw new UnsupportedOperationException();
  }
}
