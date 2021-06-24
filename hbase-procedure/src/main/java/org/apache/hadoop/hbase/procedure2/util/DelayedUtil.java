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

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

// FIX namings. TODO.
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DelayedUtil {
  private DelayedUtil() { }

  /**
   * Add a timeout to a Delay
   */
  public interface DelayedWithTimeout extends Delayed {
    long getTimeout();
  }

  /**
   * POISON implementation; used to mark special state: e.g. shutdown.
   */
  public static final DelayedWithTimeout DELAYED_POISON = new DelayedWithTimeout() {
    @Override
    public long getTimeout() {
      return 0;
    }

    @Override
    public long getDelay(final TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(final Delayed o) {
      return Long.compare(0, DelayedUtil.getTimeout(o));
    }

    @Override
    public boolean equals(final Object other) {
      return this == other;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(POISON)";
    }
  };

  /**
   * @return null (if an interrupt) or an instance of E; resets interrupt on calling thread.
   */
  public static <E extends Delayed> E takeWithoutInterrupt(final DelayQueue<E> queue,
      final long timeout, final TimeUnit timeUnit) {
    try {
      return queue.poll(timeout, timeUnit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * @return Time remaining as milliseconds.
   */
  public static long getRemainingTime(final TimeUnit resultUnit, final long timeout) {
    final long currentTime = EnvironmentEdgeManager.currentTime();
    if (currentTime >= timeout) {
      return 0;
    }
    return resultUnit.convert(timeout - currentTime, TimeUnit.MILLISECONDS);
  }

  public static int compareDelayed(final Delayed o1, final Delayed o2) {
    return Long.compare(getTimeout(o1), getTimeout(o2));
  }

  private static long getTimeout(final Delayed o) {
    assert o instanceof DelayedWithTimeout : "expected DelayedWithTimeout instance, got " + o;
    return ((DelayedWithTimeout)o).getTimeout();
  }

  public static abstract class DelayedObject implements DelayedWithTimeout {
    @Override
    public long getDelay(final TimeUnit unit) {
      return DelayedUtil.getRemainingTime(unit, getTimeout());
    }

    @Override
    public int compareTo(final Delayed other) {
      return DelayedUtil.compareDelayed(this, other);
    }

    @Override
    public String toString() {
      long timeout = getTimeout();
      return "timeout=" + timeout + ", delay=" + getDelay(TimeUnit.MILLISECONDS);
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
      if (other == this) {
        return true;
      }

      if (!(other instanceof DelayedContainer)) {
        return false;
      }

      return Objects.equals(getObject(), ((DelayedContainer)other).getObject());
    }

    @Override
    public int hashCode() {
      return object != null ? object.hashCode() : 0;
    }

    @Override
    public String toString() {
      return "containedObject=" + getObject() + ", " + super.toString();
    }
  }

  /**
   * Has a timeout.
   */
  public static class DelayedContainerWithTimestamp<T> extends DelayedContainer<T> {
    private long timeout;

    public DelayedContainerWithTimestamp(final T object, final long timeout) {
      super(object);
      setTimeout(timeout);
    }

    @Override
    public long getTimeout() {
      return timeout;
    }

    public void setTimeout(final long timeout) {
      this.timeout = timeout;
    }
  }
}
