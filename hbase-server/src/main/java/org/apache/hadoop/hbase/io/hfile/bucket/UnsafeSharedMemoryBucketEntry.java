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
package org.apache.hadoop.hbase.io.hfile.bucket;

import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BucketEntry;
import org.apache.hadoop.hbase.util.UnsafeAccess;
import org.apache.yetus.audience.InterfaceAudience;

import sun.misc.Unsafe;

@InterfaceAudience.Private
public class UnsafeSharedMemoryBucketEntry extends BucketEntry {
  private static final long serialVersionUID = 707544024564058801L;

  // We are just doing what AtomicInteger doing for the Atomic incrementAndGet/decrementAndGet.
  // We are avoiding the need to have a field of AtomicIneger type and have it as just int type.
  // We would like to reduce the head overhead per object of this type as much as possible.
  // Doing this direct Unsafe usage save us 16 bytes per Object.
  // ie Just using 4 bytes for int type than 20 bytes requirement for an AtomicInteger (16 bytes)
  // and 4 bytes reference to it.
  private static final Unsafe unsafe = UnsafeAccess.theUnsafe;
  private static final long refCountOffset;

  static {
    try {
      refCountOffset = unsafe
          .objectFieldOffset(UnsafeSharedMemoryBucketEntry.class.getDeclaredField("refCount"));
    } catch (Exception ex) {
      throw new Error(ex);
    }
  }

  // Set this when we were not able to forcefully evict the block
  private volatile boolean markedForEvict;
  private volatile int refCount = 0;

  public UnsafeSharedMemoryBucketEntry(long offset, int length, long accessCounter,
      boolean inMemory) {
    super(offset, length, accessCounter, inMemory);
  }

  @Override
  protected int getRefCount() {
    return this.refCount;
  }

  @Override
  protected int incrementRefCountAndGet() {
    return unsafe.getAndAddInt(this, refCountOffset, 1) + 1;
  }

  @Override
  protected int decrementRefCountAndGet() {
    return unsafe.getAndAddInt(this, refCountOffset, -1) - 1;
  }

  @Override
  protected boolean isMarkedForEvict() {
    return this.markedForEvict;
  }

  @Override
  protected void markForEvict() {
    this.markedForEvict = true;
  }
}