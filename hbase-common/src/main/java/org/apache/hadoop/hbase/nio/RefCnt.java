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
package org.apache.hadoop.hbase.nio;

import com.google.errorprone.annotations.RestrictedApi;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.AbstractReferenceCounted;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCounted;
import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakDetector;
import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakDetectorFactory;
import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakTracker;

/**
 * Maintain an reference count integer inside to track life cycle of {@link ByteBuff}, if the
 * reference count become 0, it'll call {@link Recycler#free()} exactly once.
 */
@InterfaceAudience.Private
public class RefCnt extends AbstractReferenceCounted {

  public static final ResourceLeakDetector<RefCnt> detector =
    ResourceLeakDetectorFactory.instance().newResourceLeakDetector(RefCnt.class);
  private final Recycler recycler;
  private final ResourceLeakTracker<RefCnt> leak;

  /**
   * Create an {@link RefCnt} with an initial reference count = 1. If the reference count become
   * zero, the recycler will do nothing. Usually, an Heap {@link ByteBuff} will use this kind of
   * refCnt to track its life cycle, it help to abstract the code path although it's not really
   * needed to track on heap ByteBuff.
   */
  public static RefCnt create() {
    return new RefCnt(ByteBuffAllocator.NONE);
  }

  public static RefCnt create(Recycler recycler) {
    return new RefCnt(recycler);
  }

  public RefCnt(Recycler recycler) {
    this.recycler = recycler;
    this.leak = recycler == ByteBuffAllocator.NONE ? null : detector.track(this);
  }

  /**
   * Returns true if this refCnt has a recycler.
   */
  public boolean hasRecycler() {
    return recycler != ByteBuffAllocator.NONE;
  }

  @Override
  public ReferenceCounted retain() {
    maybeRecord();
    return super.retain();
  }

  @Override
  public ReferenceCounted retain(int increment) {
    maybeRecord();
    return super.retain(increment);
  }

  @Override
  public boolean release() {
    maybeRecord();
    return super.release();
  }

  @Override
  public boolean release(int decrement) {
    maybeRecord();
    return super.release(decrement);
  }

  @Override
  protected final void deallocate() {
    this.recycler.free();
    if (leak != null) {
      this.leak.close(this);
    }
  }

  @Override
  public RefCnt touch() {
    maybeRecord();
    return this;
  }

  @Override
  public final ReferenceCounted touch(Object hint) {
    maybeRecord(hint);
    return this;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public Recycler getRecycler() {
    return recycler;
  }

  private void maybeRecord() {
    maybeRecord(null);
  }

  private void maybeRecord(Object hint) {
    if (leak != null) {
      leak.record(hint);
    }
  }
}
