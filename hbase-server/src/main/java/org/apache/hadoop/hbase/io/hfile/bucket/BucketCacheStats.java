/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Class that implements cache metrics for bucket cache.
 */
@InterfaceAudience.Private
public class BucketCacheStats extends CacheStats {
  private final AtomicLong ioHitCount = new AtomicLong(0);
  private final AtomicLong ioHitTime = new AtomicLong(0);
  private final static int nanoTime = 1000000;
  private long lastLogTime = EnvironmentEdgeManager.currentTime();

  BucketCacheStats() {
    super("BucketCache");
  }

  @Override
  public String toString() {
    return super.toString() + ", ioHitsPerSecond=" + getIOHitsPerSecond() +
      ", ioTimePerHit=" + getIOTimePerHit();
  }

  public void ioHit(long time) {
    ioHitCount.incrementAndGet();
    ioHitTime.addAndGet(time);
  }

  public long getIOHitsPerSecond() {
    long now = EnvironmentEdgeManager.currentTime();
    long took = (now - lastLogTime) / 1000;
    lastLogTime = now;
    return took == 0? 0: ioHitCount.get() / took;
  }

  public double getIOTimePerHit() {
    long time = ioHitTime.get() / nanoTime;
    long count = ioHitCount.get();
    return ((float) time / (float) count);
  }

  public void reset() {
    ioHitCount.set(0);
    ioHitTime.set(0);
  }
}
