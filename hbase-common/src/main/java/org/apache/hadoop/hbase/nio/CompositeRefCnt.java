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

import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCounted;

/**
 * The CompositeRefCnt is mainly used by exclusive memory HFileBlock, it has a innerRefCnt
 * to share with BucketEntry, in order to summarize the number of RPC requests. So when
 * BucketCache#freeEntireBuckets is called, will not violate the LRU policy.
 * <p>
 * And it has its own refCnt & Recycler, Once the cells shipped to client, then both the
 * Cacheable#refCnt & BucketEntry#refCnt will be decreased. when Cacheable's refCnt decrease
 * to 0, it's ByteBuff will be reclaimed. and when BucketEntry#refCnt decrease to 0, the
 * Bucket can be evicted.
 */
@InterfaceAudience.Private
public class CompositeRefCnt extends RefCnt {

  private Optional<RefCnt> innerRefCnt;

  public CompositeRefCnt(RefCnt orignal, RefCnt inner) {
    super(orignal.getRecycler());
    this.innerRefCnt = Optional.ofNullable(inner);
  }

  @VisibleForTesting
  public Optional<RefCnt> getInnerRefCnt() {
    return this.innerRefCnt;
  }

  @Override
  public boolean release() {
    return super.release() && innerRefCnt.map(refCnt -> refCnt.release()).orElse(true);
  }

  @Override
  public ReferenceCounted retain() {
    return innerRefCnt.map(refCnt -> refCnt.retain()).orElseGet(() -> super.retain());
  }
}
