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
package org.apache.hadoop.hbase.util.clock;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.EnvironmentEdge.Clock;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * SpinAdvancingClock implements a strategy for currentTimeAdvancing that spins on the CPU
 * waiting for the clock to tick over.
 */
@InterfaceAudience.Private
public class SpinAdvancingClock implements Clock {

  protected HashedBytes name;
  protected AtomicInteger refCount = new AtomicInteger();
  protected AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());

  public SpinAdvancingClock(HashedBytes name) {
    this.name = name;
  }

  @Override
  public HashedBytes getName() {
    return name;
  }

  @Override
  public void get() {
    refCount.incrementAndGet();
  }

  @Override
  public boolean remove() {
    return refCount.decrementAndGet() <= 0;
  }

  @Override
  public long currentTime() {
    return lastTime.updateAndGet(x -> {
      long now = System.currentTimeMillis();
      if (now > x) {
        return update(now);
      }
      return x;
    });
  }

  @Override
  public long currentTimeAdvancing() throws InterruptedException {
    long now;
    while (true) {
      now = System.currentTimeMillis();
      if (now > lastTime.get()) {
        final long updateTime = now;
        return lastTime.updateAndGet(x -> update(updateTime));
      }
      spin();
    }
  }

  // Broken out to inlinable method for subclassing and instrumentation.
  protected void spin() throws InterruptedException { }

  // Broken out to inlinable method for subclassing and instrumentation.
  protected long update(long now) {
    return now;
  }

  // Visible for testing
  public int getRefCount() {
    return refCount.get();
  }

}
