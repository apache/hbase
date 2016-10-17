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

package org.apache.hadoop.hbase.procedure2.util;

import java.util.Objects;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DelayedUtil {
  private DelayedUtil() { }

  public interface DelayedWithTimeout extends Delayed {
    long getTimeoutTimestamp();
  }

  public static final DelayedWithTimeout DELAYED_POISON = new DelayedWithTimeout() {
    @Override
    public long getTimeoutTimestamp() {
      return 0;
    }

    @Override
    public long getDelay(final TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(final Delayed o) {
      return Long.compare(0, DelayedUtil.getTimeoutTimestamp(o));
    }

    @Override
    public boolean equals(final Object other) {
      return this == other;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(POISON)";
    }
  };

  public static <E extends Delayed> E takeWithoutInterrupt(final DelayQueue<E> queue) {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  public static long getRemainingTime(final TimeUnit resultUnit, final long timeoutTime) {
    final long currentTime = EnvironmentEdgeManager.currentTime();
    if (currentTime >= timeoutTime) {
      return 0;
    }
    return resultUnit.convert(timeoutTime - currentTime, TimeUnit.MILLISECONDS);
  }

  public static int compareDelayed(final Delayed o1, final Delayed o2) {
    return Long.compare(getTimeoutTimestamp(o1), getTimeoutTimestamp(o2));
  }

  private static long getTimeoutTimestamp(final Delayed o) {
    assert o instanceof DelayedWithTimeout : "expected DelayedWithTimeout instance, got " + o;
    return ((DelayedWithTimeout)o).getTimeoutTimestamp();
  }

  public static abstract class DelayedObject implements DelayedWithTimeout {
    @Override
    public long getDelay(final TimeUnit unit) {
      return DelayedUtil.getRemainingTime(unit, getTimeoutTimestamp());
    }

    @Override
    public int compareTo(final Delayed other) {
      return DelayedUtil.compareDelayed(this, other);
    }
  }

  public static abstract class DelayedContainer<T> extends DelayedObject {
    private final T object;

    public DelayedContainer(final T object) {
      this.object = object;
    }

    public T getObject() {
      return this.object;
    }

    @Override
    public boolean equals(final Object other) {
      if (other == this) return true;
      if (!(other instanceof DelayedContainer)) return false;
      return Objects.equals(getObject(), ((DelayedContainer)other).getObject());
    }

    @Override
    public int hashCode() {
      return object != null ? object.hashCode() : 0;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + getObject() + ")";
    }
  }

  public static class DelayedContainerWithTimestamp<T> extends DelayedContainer<T> {
    private long timeoutTimestamp;

    public DelayedContainerWithTimestamp(final T object, final long timeoutTimestamp) {
      super(object);
      setTimeoutTimestamp(timeoutTimestamp);
    }

    @Override
    public long getTimeoutTimestamp() {
      return timeoutTimestamp;
    }

    public void setTimeoutTimestamp(final long timeoutTimestamp) {
      this.timeoutTimestamp = timeoutTimestamp;
    }
  }
}
