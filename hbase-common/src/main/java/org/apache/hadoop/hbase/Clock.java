/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hbase.util.AtomicUtils.updateMax;

/**
 * A clock is an implementation of an algorithm to get timestamps corresponding to one of the
 * {@link TimestampType}s for the current time. Different clock implementations can have
 * different semantics associated with them. Every such clock should be able to map its
 * representation of time to one of the {link TimestampType}s.
 * HBase has traditionally been using the {@link java.lang.System#currentTimeMillis()} to
 * timestamp events in HBase. {@link java.lang.System#currentTimeMillis()} does not give any
 * guarantees about monotonicity of time. We will keep this implementation of clock in place for
 * backward compatibility and call it SYSTEM clock.
 * It is easy to provide monotonically non decreasing time semantics by keeping track of the last
 * timestamp given by the clock and updating it on receipt of external message. This
 * implementation of clock is called SYSTEM_MONOTONIC.
 * SYSTEM Clock and SYSTEM_MONOTONIC clock as described above, both being physical clocks, they
 * cannot track causality. Hybrid Logical Clocks(HLC), as described in
 * <a href="http://www.cse.buffalo.edu/tech-reports/2014-04.pdf">HLC Paper</a>, helps tracking
 * causality using a
 * <a href="http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf">Logical
 * Clock</a> but always keeps the logical time close to the wall time or physical time. It kind
 * of has the advantages of both the worlds. One such advantage being getting consistent
 * snapshots in physical time as described in the paper. Hybrid Logical Clock has an additional
 * advantage that it is always monotonically increasing.
 * Note: It is assumed that any physical clock implementation has millisecond resolution else the
 * {@link TimestampType} implementation has to changed to accommodate it. It is decided after
 * careful discussion to go with millisecond resolution in the HLC design document attached in the
 * issue <a href="https://issues.apache.org/jira/browse/HBASE-14070">HBASE-14070 </a>.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Clock {
  long DEFAULT_MAX_CLOCK_SKEW_IN_MS = 30000;

  /**
   * This is a method to get the current time.
   *
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  long now() throws RuntimeException;

  /**
   * This is a method to update the current time with the passed timestamp.
   * @param timestamp
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  long update(long timestamp) throws RuntimeException;

  /**
   * @return true if the clock implementation gives monotonically non decreasing timestamps else
   * false.
   */
  boolean isMonotonic();

  /**
   * @return {@link org.apache.hadoop.hbase.TimestampType}
   */
  TimestampType getTimestampType();

  /**
   * @return {@link org.apache.hadoop.hbase.ClockType}
   */
  ClockType getClockType();

  /**
   * @return {@link TimeUnit} of the physical time used by the clock.
   */
  TimeUnit getTimeUnit();

  /**
   * Indicates that Physical Time or Logical Time component has overflowed. This extends
   * RuntimeException.
   */
  @SuppressWarnings("serial")
  class ClockException extends RuntimeException {
    public ClockException(String msg) {
      super(msg);
    }
  }

  Clock SYSTEM_CLOCK = new System();

  //////////////////////////////////////////////////////////////////
  // Implementation of clocks
  //////////////////////////////////////////////////////////////////

  /**
   * System clock is an implementation of clock which doesn't give any monotonic guarantees.
   * Since it directly represents system's actual clock which cannot be changed, update() function
   * is no-op.
   */
  class System implements Clock {
    @Override
    public long now() {
      return EnvironmentEdgeManager.currentTime();
    }

    @Override
    public long update(long timestamp) {
      return EnvironmentEdgeManager.currentTime();
    }

    @Override
    public boolean isMonotonic() {
      return false;
    }

    @Override
    public TimeUnit getTimeUnit() {
      return TimeUnit.MILLISECONDS;
    }

    @Override
    public TimestampType getTimestampType() {
      return TimestampType.PHYSICAL;
    }

    @Override
    public ClockType getClockType() {
      return ClockType.SYSTEM;
    }
  }

  /**
   * System clock is an implementation of clock which guarantees monotonically non-decreasing
   * timestamps.
   */
  class SystemMonotonic implements Clock {
    private final long maxClockSkewInMs;
    private final Clock systemClock;
    private final AtomicLong physicalTime = new AtomicLong();

    public SystemMonotonic(long maxClockSkewInMs) {
      this(SYSTEM_CLOCK, maxClockSkewInMs);
    }

    @VisibleForTesting
    public SystemMonotonic() {
      this(DEFAULT_MAX_CLOCK_SKEW_IN_MS);
    }

    @VisibleForTesting
    public SystemMonotonic(Clock systemClock) {
      this(systemClock, DEFAULT_MAX_CLOCK_SKEW_IN_MS);
    }

    @VisibleForTesting
    public SystemMonotonic(Clock systemClock, long maxClockSkewInMs) {
      this.systemClock = systemClock;
      this.maxClockSkewInMs = maxClockSkewInMs > 0 ?
          maxClockSkewInMs : DEFAULT_MAX_CLOCK_SKEW_IN_MS;
    }

    @Override
    public long now() {
      updateMax(physicalTime, systemClock.now());
      return physicalTime.get();
    }

    /**
     * @throws ClockException If timestamp exceeds max clock skew allowed.
     */
    public long update(long targetTimestamp) throws ClockException {
      final long systemTime = systemClock.now();
      if (maxClockSkewInMs > 0 && (targetTimestamp - systemTime) > maxClockSkewInMs) {
        throw new ClockException(
            "Received event with timestamp:" + getTimestampType().toString(targetTimestamp)
                + " which is greater than allowed clock skew ");
      }
      final long oldPhysicalTime = systemTime > targetTimestamp ? systemTime : targetTimestamp;
      updateMax(physicalTime, oldPhysicalTime);
      return physicalTime.get();
    }

    @Override
    public boolean isMonotonic() {
      return true;
    }

    @Override
    public TimeUnit getTimeUnit() {
      return systemClock.getTimeUnit();
    }

    @Override
    public TimestampType getTimestampType() {
      return TimestampType.PHYSICAL;
    }

    @Override
    public ClockType getClockType() {
      return ClockType.SYSTEM_MONOTONIC;
    }
  }

  /**
   * HLC clock implementation.
   * Monotonicity guarantee of physical component of time comes from {@link SystemMonotonic} clock.
   */
  class HLC implements Clock {
    private static final TimestampType TIMESTAMP_TYPE = TimestampType.HYBRID;
    private static final long MAX_PHYSICAL_TIME = TIMESTAMP_TYPE.getMaxPhysicalTime();
    private static final long MAX_LOGICAL_TIME = TIMESTAMP_TYPE.getMaxLogicalTime();
    private final Clock systemMonotonicClock;
    private long currentPhysicalTime = 0;
    private long currentLogicalTime = 0;

    public HLC(long maxClockSkewInMs) {
      this(new SystemMonotonic(maxClockSkewInMs));
    }

    @VisibleForTesting
    public HLC() {
      this(DEFAULT_MAX_CLOCK_SKEW_IN_MS);
    }

    /**
     * @param systemMonotonicClock Clock to get physical component of time. Should be monotonic
     *                             clock.
     */
    @VisibleForTesting
    public HLC(Clock systemMonotonicClock) {
      assert(systemMonotonicClock.isMonotonic());
      this.systemMonotonicClock = systemMonotonicClock;
    }

    @Override
    public synchronized long now() throws ClockException {
      final long newSystemTime = systemMonotonicClock.now();
      if (newSystemTime <= currentPhysicalTime) {
        currentLogicalTime++;
      } else if (newSystemTime > currentPhysicalTime) {
        currentLogicalTime = 0;
        currentPhysicalTime = newSystemTime;
      }
      checkPhysicalTimeOverflow(newSystemTime, MAX_PHYSICAL_TIME);
      checkLogicalTimeOverflow(currentLogicalTime, MAX_LOGICAL_TIME);

      return toTimestamp();
    }

    /**
     * Updates {@link HLC} with the given time received from elsewhere (possibly some other node).
     *
     * @param targetTime time from the external message.
     * @return a hybrid timestamp of HLC that is strict greater than given {@code targetTime} and
     * previously returned times.
     * @throws ClockException If timestamp exceeds max clock skew allowed.
     */
    @Override
    public synchronized long update(long targetTime)
        throws ClockException {
      final long targetPhysicalTime = TIMESTAMP_TYPE.getPhysicalTime(targetTime);
      final long targetLogicalTime = TIMESTAMP_TYPE.getLogicalTime(targetTime);
      final long oldPhysicalTime = currentPhysicalTime;
      currentPhysicalTime = systemMonotonicClock.update(targetPhysicalTime);

      checkPhysicalTimeOverflow(currentPhysicalTime, MAX_PHYSICAL_TIME);

      if (currentPhysicalTime == targetPhysicalTime && currentPhysicalTime == oldPhysicalTime) {
        currentLogicalTime = Math.max(currentLogicalTime, targetLogicalTime) + 1;
      } else if (currentPhysicalTime == targetPhysicalTime) {
        currentLogicalTime = targetLogicalTime + 1;
      } else if (currentPhysicalTime == oldPhysicalTime) {
        currentLogicalTime++;
      } else {
        currentLogicalTime = 0;
      }

      checkLogicalTimeOverflow(currentLogicalTime, MAX_LOGICAL_TIME);
      return toTimestamp();
    }

    @Override
    public boolean isMonotonic() {
      return true;
    }

    public TimeUnit getTimeUnit() {
      return systemMonotonicClock.getTimeUnit();
    }

    private long toTimestamp() {
      return TIMESTAMP_TYPE.toTimestamp(TimeUnit.MILLISECONDS, currentPhysicalTime,
          currentLogicalTime);
    }

    @VisibleForTesting
    synchronized long getLogicalTime() { return currentLogicalTime; }

    @VisibleForTesting
    synchronized long getPhysicalTime() { return currentPhysicalTime; }

    @Override
    public TimestampType getTimestampType() {
      return TIMESTAMP_TYPE;
    }

    @Override
    public ClockType getClockType() {
      return ClockType.HLC;
    }
  }

  //////////////////////////////////////////////////////////////////
  // Utility functions
  //////////////////////////////////////////////////////////////////

  // Only for testing.
  @VisibleForTesting
  static Clock getDummyClockOfGivenClockType(ClockType clockType) {
    if (clockType == ClockType.HLC) {
      return new Clock.HLC();
    } else if (clockType == ClockType.SYSTEM_MONOTONIC) {
      return new Clock.SystemMonotonic();
    } else {
      return new Clock.System();
    }
  }

  static void checkLogicalTimeOverflow(long logicalTime, long maxLogicalTime) {
    if (logicalTime >= maxLogicalTime) {
      // highly unlikely to happen, when it happens, we throw exception for the above layer to
      // handle it the way they wish to.
      throw new Clock.ClockException(
          "Logical Time Overflowed: " + logicalTime + "max " + "logical time: " + maxLogicalTime);
    }
  }

  static void checkPhysicalTimeOverflow(long physicalTime, long maxPhysicalTime) {
    if (physicalTime >= maxPhysicalTime) {
      // Extremely unlikely to happen, if this happens upper layers may have to kill the server.
      throw new Clock.ClockException(
          "Physical Time overflowed: " + physicalTime + " and max physical time:" + maxPhysicalTime);
    }
  }

}