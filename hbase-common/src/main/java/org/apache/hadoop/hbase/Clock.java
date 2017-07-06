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
   * @return true if the clock implementation gives monotonically increasing timestamps else false.
   */
  boolean isMonotonicallyIncreasing();

  /**
   * @return {@link org.apache.hadoop.hbase.TimestampType}
   */
  TimestampType getTimestampType();

  /**
   * @return {@link org.apache.hadoop.hbase.ClockType}
   */
  ClockType getClockType();


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

  //////////////////////////////////////////////////////////////////
  // Physical Clock
  //////////////////////////////////////////////////////////////////

  interface PhysicalClock {
    /**
     * This is a method to get the current time.
     *
     * @return Timestamp of current time in 64 bit representation corresponding to the particular
     * clock
     */
    long now() throws RuntimeException;

    /**
     * This is a method to get the unit of the physical time used by the clock
     *
     * @return A {@link TimeUnit}
     */
    TimeUnit getTimeUnit();
  }

  class JavaMillisPhysicalClock implements PhysicalClock {
    @Override
    public long now() {
      return EnvironmentEdgeManager.currentTime();
    }

    @Override
    public TimeUnit getTimeUnit() {
      return TimeUnit.MILLISECONDS;
    }
  }

  JavaMillisPhysicalClock DEFAULT_JAVA_MILLIS_PHYSICAL_CLOCK =
      new JavaMillisPhysicalClock();

  //////////////////////////////////////////////////////////////////
  // Implementation of clocks
  //////////////////////////////////////////////////////////////////

  /**
   * System clock is an implementation of clock which doesn't give any monotonic guarantees.
   */
  class System implements Clock, PhysicalClock {
    private final PhysicalClock physicalClock = DEFAULT_JAVA_MILLIS_PHYSICAL_CLOCK;
    private final ClockType clockType = ClockType.SYSTEM;
    private final TimestampType timestampType = TimestampType.PHYSICAL;

    @Override
    public long now() {
      return physicalClock.now();
    }

    @Override
    public long update(long timestamp) {
      return physicalClock.now();
    }

    @Override
    public boolean isMonotonic() {
      return false;
    }

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }

    @Override
    public TimestampType getTimestampType() { return timestampType; }

    @Override
    public ClockType getClockType() { return clockType; }
  }

  /**
   * System clock is an implementation of clock which guarantees monotonically non-decreasing
   * timestamps.
   */
  class SystemMonotonic implements Clock, PhysicalClock {
    private final long maxClockSkewInMs;
    private final PhysicalClock physicalClock;
    private final AtomicLong physicalTime = new AtomicLong();
    private final ClockType clockType = ClockType.SYSTEM_MONOTONIC;
    private final TimestampType timestampType = TimestampType.PHYSICAL;

    public SystemMonotonic(PhysicalClock physicalClock, long maxClockSkewInMs) {
      this.physicalClock = physicalClock;
      this.maxClockSkewInMs = maxClockSkewInMs > 0 ?
          maxClockSkewInMs : DEFAULT_MAX_CLOCK_SKEW_IN_MS;
    }

    public SystemMonotonic() {
      this.physicalClock = DEFAULT_JAVA_MILLIS_PHYSICAL_CLOCK;
      this.maxClockSkewInMs = DEFAULT_MAX_CLOCK_SKEW_IN_MS;
    }

    @Override
    public long now() {
      long systemTime = physicalClock.now();
      updateMax(physicalTime, systemTime);
      return physicalTime.get();
    }

    public long update(long targetTimestamp) throws ClockException {
      final long systemTime = physicalClock.now();
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
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }

    @VisibleForTesting
    void setPhysicalTime(long time) {
      physicalTime.set(time);
    }

    @Override
    public TimestampType getTimestampType() { return timestampType; }

    @Override
    public ClockType getClockType() { return clockType; }
  }

  class HLC implements Clock, PhysicalClock {
    private final PhysicalClock physicalClock;
    private final long maxClockSkew;
    private final long maxPhysicalTime;
    private final long maxLogicalTime;
    private long physicalTime;
    private long logicalTime;
    private final ClockType clockType = ClockType.HLC;
    private final TimestampType timestampType = TimestampType.HYBRID;

    public HLC(PhysicalClock physicalClock, long maxClockSkew) {
      this.physicalClock = physicalClock;
      this.maxClockSkew = maxClockSkew > 0 ? maxClockSkew : DEFAULT_MAX_CLOCK_SKEW_IN_MS;
      this.maxPhysicalTime = timestampType.getMaxPhysicalTime();
      this.maxLogicalTime = timestampType.getMaxLogicalTime();
      this.physicalTime = 0;
      this.logicalTime = 0;
    }

    public HLC() {
      this.physicalClock = DEFAULT_JAVA_MILLIS_PHYSICAL_CLOCK;
      this.maxClockSkew = DEFAULT_MAX_CLOCK_SKEW_IN_MS;
      this.maxPhysicalTime = timestampType.getMaxPhysicalTime();
      this.maxLogicalTime = timestampType.getMaxLogicalTime();
      this.physicalTime = 0;
      this.logicalTime = 0;
    }

    @Override
    public synchronized long now() throws ClockException {
      final long systemTime = physicalClock.now();

      checkPhysicalTimeOverflow(systemTime, maxPhysicalTime);
      checkLogicalTimeOverflow(logicalTime, maxLogicalTime);

      if (systemTime <= physicalTime) {
        logicalTime++;
      } else if (systemTime > physicalTime) {
        logicalTime = 0;
        physicalTime = systemTime;
      }

      return toTimestamp();
    }

    /**
     * Updates {@link HLC} with the given timestamp received from elsewhere (possibly
     * some other node). Returned timestamp is strict greater than msgTimestamp and local
     * timestamp.
     *
     * @param timestamp timestamp from the external message.
     * @return a hybrid timestamp of HLC that is strictly greater than local timestamp and
     * msgTimestamp
     * @throws ClockException
     */
    @Override
    public synchronized long update(long timestamp)
        throws ClockException {
      final long targetPhysicalTime = timestampType.getPhysicalTime(timestamp);
      final long targetLogicalTime = timestampType.getLogicalTime(timestamp);
      final long oldPhysicalTime = physicalTime;
      final long systemTime = physicalClock.now();

      physicalTime = Math.max(Math.max(oldPhysicalTime, targetPhysicalTime), systemTime);
      checkPhysicalTimeOverflow(systemTime, maxPhysicalTime);

      if (targetPhysicalTime - systemTime > maxClockSkew) {
        throw new ClockException("Received event with timestamp:" +
            timestampType.toString(timestamp) + " which is greater than allowed clock skew");
      }
      if (physicalTime == oldPhysicalTime && oldPhysicalTime == targetPhysicalTime) {
        logicalTime = Math.max(logicalTime, targetLogicalTime) + 1;
      } else if (physicalTime == targetPhysicalTime) {
        logicalTime = targetLogicalTime + 1;
      } else if (physicalTime == oldPhysicalTime) {
        logicalTime++;
      } else {
        logicalTime = 0;
      }

      checkLogicalTimeOverflow(logicalTime, maxLogicalTime);
      return toTimestamp();
    }

    @Override
    public boolean isMonotonic() {
      return true;
    }

    @Override
    public boolean isMonotonicallyIncreasing() {
      return true;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }

    private long toTimestamp() {
      return timestampType.toTimestamp(getTimeUnit(), physicalTime, logicalTime);
    }

    @VisibleForTesting
    synchronized void setLogicalTime(long logicalTime) {
      this.logicalTime = logicalTime;
    }

    @VisibleForTesting
    synchronized void setPhysicalTime(long physicalTime) {
      this.physicalTime = physicalTime;
    }

    @Override
    public TimestampType getTimestampType() { return timestampType; }

    @Override
    public ClockType getClockType() { return clockType; }
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