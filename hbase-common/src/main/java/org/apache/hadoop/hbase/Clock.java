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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.HBaseException;
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
public abstract class Clock {
  private static final Log LOG = LogFactory.getLog(Clock.class);

  protected PhysicalClock physicalClock;
  protected TimestampType timestampType;
  public ClockType clockType;

  Clock(PhysicalClock physicalClock) {
    this.physicalClock = physicalClock;
  }

  // Only for testing.
  @VisibleForTesting
  public static Clock getDummyClockOfGivenClockType(ClockType clockType) {
    if(clockType == ClockType.HLC) {
      return new Clock.HLC();
    } else if(clockType == ClockType.SYSTEM_MONOTONIC) {
      return new Clock.SystemMonotonic();
    } else {
      return new Clock.System();
    }
  }

  /**
   * Indicates that Physical Time or Logical Time component has overflowed. This extends
   * RuntimeException.
   */
  @SuppressWarnings("serial") public static class ClockException extends RuntimeException {
    public ClockException(String msg) {
      super(msg);
    }
  }

  /**
   * This is a method to get the current time.
   *
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  public abstract long now() throws RuntimeException;

  /**
   * This is a method to update the current time with the passed timestamp.
   * @param timestamp
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  public abstract long update(long timestamp) throws RuntimeException;

  /**
   * @return true if the clock implementation gives monotonically non decreasing timestamps else
   * false.
   */
  public abstract boolean isMonotonic();

  /**
   * @return true if the clock implementation gives monotonically increasing timestamps else false.
   */
  public abstract boolean isMonotonicallyIncreasing();

  /**
   * @return {@link org.apache.hadoop.hbase.TimestampType}
   */
  public TimestampType getTimestampType(){
    return timestampType;
  }

  interface Monotonic {
    // This is currently equal to the HBase default.
    long DEFAULT_MAX_CLOCK_SKEW = 30000;

    /**
     * This is a method to update the local clock on receipt of a timestamped message from
     * the external world.
     *
     * @param timestamp The timestamp present in the message received by the node from outside.
     */
    long update(long timestamp) throws RuntimeException, HBaseException;
  }

  public interface PhysicalClock {
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

  public static class JavaMillisPhysicalClock implements PhysicalClock {
    @Override public long now() {
      return EnvironmentEdgeManager.currentTime();
    }

    @Override public TimeUnit getTimeUnit() {
      return TimeUnit.MILLISECONDS;
    }
  }

  /**
   * Returns the default physical clock used in HBase. It is currently based on
   * {@link java.lang.System#currentTimeMillis()}
   *
   * @return the default PhysicalClock
   */
  public static PhysicalClock getDefaultPhysicalClock() {
    return new JavaMillisPhysicalClock();
  }

  /**
   * System clock is an implementation of clock which doesn't give any monotonic guarantees.
   */
  public static class System extends Clock implements PhysicalClock {

    public System() {
      super(getDefaultPhysicalClock());
      this.timestampType = TimestampType.PHYSICAL;
      this.clockType = ClockType.SYSTEM;
    }

    @Override public long now() {
      return physicalClock.now();
    }

    @Override public long update(long timestamp) {
      return physicalClock.now();
    }

    @Override public boolean isMonotonic() {
      return false;
    }

    @Override public boolean isMonotonicallyIncreasing() {
      return false;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }
  }

  /**
   * System clock is an implementation of clock which guarantees monotonically non-decreasing
   * timestamps.
   */
  public static class SystemMonotonic extends Clock implements Monotonic, PhysicalClock {
    private long maxClockSkew;
    private static final long OFFSET = 5000;
    AtomicLong physicalTime = new AtomicLong();

    public SystemMonotonic(PhysicalClock physicalClock, long maxClockSkew) {
      super(physicalClock);
      this.maxClockSkew = maxClockSkew > 0 ? maxClockSkew : DEFAULT_MAX_CLOCK_SKEW;
      this.timestampType = TimestampType.PHYSICAL;
      this.clockType = ClockType.SYSTEM_MONOTONIC;
    }

    public SystemMonotonic() {
      super(getDefaultPhysicalClock());
      this.maxClockSkew = DEFAULT_MAX_CLOCK_SKEW;
      this.timestampType = TimestampType.PHYSICAL;
      this.clockType = ClockType.SYSTEM_MONOTONIC;
    }

    @Override public long now() {
      long systemTime = physicalClock.now();
      updateMax(physicalTime, systemTime);
      return physicalTime.get();
    }

    public long update(long messageTimestamp) throws ClockException {
      long systemTime = physicalClock.now();
      if (maxClockSkew > 0 && (messageTimestamp - systemTime) > maxClockSkew) {
        throw new ClockException(
            "Received event with timestamp:" + timestampType.toString(messageTimestamp)
                + " which is greater than allowed clock skew ");
      }
      long physicalTime_ = systemTime > messageTimestamp ? systemTime : messageTimestamp;
      updateMax(physicalTime, physicalTime_);
      return physicalTime.get();
    }

    @Override public boolean isMonotonic() {
      return true;
    }

    @Override public boolean isMonotonicallyIncreasing() {
      return false;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }

    @VisibleForTesting void setPhysicalTime(long time) {
      physicalTime.set(time);
    }
  }

  public static class HLC extends Clock implements Monotonic, PhysicalClock {
    private long maxClockSkew;
    private long physicalTime;
    private long logicalTime;
    private long maxPhysicalTime;
    private long maxLogicalTime;

    public HLC(PhysicalClock physicalClock, long maxClockSkew) {
      super(physicalClock);
      this.maxClockSkew = maxClockSkew > 0 ? maxClockSkew : DEFAULT_MAX_CLOCK_SKEW;
      this.timestampType = TimestampType.HYBRID;
      this.maxPhysicalTime = timestampType.getMaxPhysicalTime();
      this.maxLogicalTime = timestampType.getMaxLogicalTime();
      this.physicalTime = 0;
      this.logicalTime = 0;
      this.clockType = ClockType.HLC;
    }

    public HLC() {
      super(getDefaultPhysicalClock());
      this.maxClockSkew = DEFAULT_MAX_CLOCK_SKEW;
      this.timestampType = TimestampType.HYBRID;
      this.maxPhysicalTime = timestampType.getMaxPhysicalTime();
      this.maxLogicalTime = timestampType.getMaxLogicalTime();
      this.physicalTime = 0;
      this.logicalTime = 0;
      this.clockType = ClockType.HLC;
    }

    @Override public synchronized long now() throws ClockException {
      long systemTime = physicalClock.now();
      long physicalTime_ = physicalTime;
      if (systemTime >= maxPhysicalTime) {
        // Extremely unlikely to happen, if this happens upper layers may have to kill the server.
        throw new ClockException(
            "PT overflowed: " + systemTime + " and max physical time:" + maxPhysicalTime);
      }

      if (logicalTime >= maxLogicalTime) {
        // highly unlikely to happen, when it happens, we throw exception for the above layer to
        // handle.
        throw new ClockException(
            "Logical Time Overflowed: " + logicalTime + "max " + "logical " + "time:"
                + maxLogicalTime);
      }

      if (systemTime > physicalTime_) physicalTime = systemTime;

      if (physicalTime == physicalTime_) {
        logicalTime++;
      } else {
        logicalTime = 0;
      }

      return toTimestamp();
    }

    /**
     * Updates {@link HLC} with the given timestamp received from elsewhere (possibly
     * some other node). Returned timestamp is strict greater than msgTimestamp and local
     * timestamp.
     *
     * @param messageTimestamp timestamp from the external message.
     * @return a hybrid timestamp of HLC that is strictly greater than local timestamp and
     * msgTimestamp
     * @throws ClockException
     */
    @Override public synchronized long update(long messageTimestamp)
        throws ClockException {
      long messagePhysicalTime = timestampType.getPhysicalTime(messageTimestamp);
      long messageLogicalTime = timestampType.getLogicalTime(messageTimestamp);
      // variable to keep old physical time when we update it.
      long physicalTime_ = physicalTime;
      long systemTime = physicalClock.now();

      physicalTime = Math.max(Math.max(physicalTime_, messagePhysicalTime), systemTime);

      if (systemTime >= maxPhysicalTime) {
        // Extremely unlikely to happen, if this happens upper layers may have to kill the server.
        throw new ClockException(
            "Physical Time overflowed: " + systemTime + " and max physical time:"
                + maxPhysicalTime);
      } else if (messagePhysicalTime - systemTime > maxClockSkew) {
        throw new ClockException(
            "Received event with timestamp:" + timestampType.toString(messageTimestamp)
                + " which is greater than allowed clock skew ");
      } else if (physicalTime == physicalTime_ && physicalTime_ == messagePhysicalTime) {
        logicalTime = Math.max(logicalTime, messageLogicalTime) + 1;
      } else if (physicalTime == messagePhysicalTime) {
        logicalTime = messageLogicalTime + 1;
      } else if (physicalTime == physicalTime_) {
        logicalTime++;
      } else {
        logicalTime = 0;
      }

      if (logicalTime >= maxLogicalTime) {
        // highly unlikely to happen, when it happens, we throw exception for the above layer to
        // handle it the way they wish to.
        throw new ClockException(
            "Logical Time Overflowed: " + logicalTime + "max " + "logical time: " + maxLogicalTime);
      }
      return toTimestamp();
    }

    @Override public boolean isMonotonic() {
      return true;
    }

    @Override public boolean isMonotonicallyIncreasing() {
      return true;
    }

    public TimeUnit getTimeUnit() {
      return physicalClock.getTimeUnit();
    }

    private long toTimestamp() {
      return timestampType.toTimestamp(getTimeUnit(), physicalTime, logicalTime);
    }

    @VisibleForTesting synchronized void setLogicalTime(long logicalTime) {
      this.logicalTime = logicalTime;
    }

    @VisibleForTesting synchronized void setPhysicalTime(long physicalTime) {
      this.physicalTime = physicalTime;
    }
  }
}
