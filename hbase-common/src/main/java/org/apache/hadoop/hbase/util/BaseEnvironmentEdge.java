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
package org.apache.hadoop.hbase.util;

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.util.clock.BoundedIncrementYieldAdvancingClock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base implementation of an environment edge.
 */
@InterfaceAudience.Private
public abstract class BaseEnvironmentEdge implements EnvironmentEdge {

  // TODO: Hardcoded for now. Should this be configurable? BoundedIncrementYieldAdvancingClock
  // is the best option, as determined by microbenchmarks. See HBASE-25913.
  private static final Constructor<?> CLOCK_IMPL_CONSTRUCTOR;
  static {
    try {
      CLOCK_IMPL_CONSTRUCTOR =
        BoundedIncrementYieldAdvancingClock.class.getConstructor(HashedBytes.class);
    } catch (Exception e) {
      // If there is a problem this exception will bubble up and ultimately cause a JVM abort,
      // which is what we want, because it means a developer failed to update here after
      // changing how clocks are constructed.
      throw new RuntimeException(e);
    }
  }

  /**
   * A clock instance representing the system time directly, so we introduce no overheads for the
   * vast majority of users of EnvironmentEdgeManager.currentTime.
   */
  private static final Clock SYSTEM_CLOCK = new Clock() {
    final HashedBytes NAME = new HashedBytes(Bytes.toBytes("DEFAULT"));
    @Override
    public HashedBytes getName() {
      return NAME;
    }
    @Override
    public long currentTime() {
      return System.currentTimeMillis();
    }
    @Override
    public long currentTimeAdvancing() {
      throw new UnsupportedOperationException(
        "Default clock does not implement currentTimeAdvancing()");
    }
    @Override
    public void get() {
      throw new UnsupportedOperationException("Default clock does not implement get()");
    }
    @Override
    public boolean remove() {
      throw new UnsupportedOperationException("Default clock does not implement remove()");
    }
  };

  @Override
  public long currentTime() {
    return SYSTEM_CLOCK.currentTime();
  }

  private static final ConcurrentHashMap<HashedBytes, Clock> clockMap = new ConcurrentHashMap<>();

  @Override
  public Clock getClock(final HashedBytes name) {
    Clock clock = clockMap.computeIfAbsent(name, k -> {
      try {
        return (Clock)CLOCK_IMPL_CONSTRUCTOR.newInstance(k);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    clock.get(); // increment reference count
    return clock;
  }

  @Override
  public boolean removeClock(Clock clock) {
    if (clock.remove()) { // only remove when refcount drops to zero
      if (clockMap.remove(clock.getName()) != null) {
        return true;
      }
    }
    return false;
  }

}
