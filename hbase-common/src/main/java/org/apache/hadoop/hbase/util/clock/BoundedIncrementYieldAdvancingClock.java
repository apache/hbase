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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * BoundedIncrementYieldAdvancingClock uses AtomicLong#updateAndGet to increment our
 * clock's notion of current tick at each method call until currentTimeMillis arrives
 * at a time greater than our clock's notion of current tick, up to a certain bound,
 * at which point it will switch to a yielding strategy until the system clock catches
 * up.
 */
@InterfaceAudience.Private
public class BoundedIncrementYieldAdvancingClock extends IncrementAdvancingClock {

  static final int MAX_ADVANCE = 1000;

  protected AtomicLong currentAdvance = new AtomicLong(0);

  public BoundedIncrementYieldAdvancingClock(HashedBytes name) {
    super(name);
  }

  @Override
  public long currentTime() {
    return super.currentTime();
  }

  @Override
  public long currentTimeAdvancing() throws InterruptedException {
    // If we have advanced too far, now we have to wait for the system clock to
    // catch up.
    if (currentAdvance.incrementAndGet() >= MAX_ADVANCE) {
      while (true) {
        long now = System.currentTimeMillis();
        if (now > lastTime.get()) {
          lastTime.set(update(now));
          return now;
        }
        spin();
      }
    // Otherwise, it's fine to advance our notion of the system time.
    } else {
      return lastTime.updateAndGet(x -> {
        long now = System.currentTimeMillis();
        if (now > x) {
          return update(now);
        }
        return advance(x);
      });
    }
  }

  // Broken out to inlinable method for subclassing and instrumentation.
  protected long advance(long last) {
    // System clock hasn't moved forward. Increment our notion of current tick to keep
    // the time advancing.
    currentAdvance.incrementAndGet();
    return last + 1;
  }

  // Broken out to inlinable method for subclassing and instrumentation.
  protected long update(long now) {
    // System clock has moved ahead of our notion of current tick. Move us forward to match.
    // We do that just by returning the current time, which was passed in to us as 'n'.
    currentAdvance.set(0);
    return now;
  }

  // Broken out to inlinable method for subclassing and instrumentation.
  protected void spin() throws InterruptedException {
    Thread.sleep(1, 0); // black magic to yield on all platforms
  }

}
