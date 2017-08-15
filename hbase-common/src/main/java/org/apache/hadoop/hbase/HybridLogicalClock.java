package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.concurrent.TimeUnit;

/**
 * Hybrid logical clock implementation.
 * Monotonicity guarantee of physical component of time comes from {@link SystemMonotonicClock}.
 */
@InterfaceAudience.Private
public class HybridLogicalClock implements Clock {
  private static final TimestampType TIMESTAMP_TYPE = TimestampType.HYBRID;
  private static final long MAX_PHYSICAL_TIME = TIMESTAMP_TYPE.getMaxPhysicalTime();
  private static final long MAX_LOGICAL_TIME = TIMESTAMP_TYPE.getMaxLogicalTime();
  private final Clock systemMonotonicClock;
  private long currentPhysicalTime = 0;
  private long currentLogicalTime = 0;

  public HybridLogicalClock(long maxClockSkewInMs) {
    this(new SystemMonotonicClock(maxClockSkewInMs));
  }

  @VisibleForTesting
  public HybridLogicalClock() {
    this(DEFAULT_MAX_CLOCK_SKEW_IN_MS);
  }

  /**
   * @param systemMonotonicClock Clock to get physical component of time. Should be monotonic
   *                             clock.
   */
  @VisibleForTesting
  public HybridLogicalClock(Clock systemMonotonicClock) {
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
   * Updates {@link org.apache.hadoop.hbase.HybridLogicalClock} with the given time received from elsewhere (possibly
   * some other node).
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
    return ClockType.HYBRID_LOGICAL;
  }

  static void checkLogicalTimeOverflow(long logicalTime, long maxLogicalTime) {
    if (logicalTime >= maxLogicalTime) {
      // highly unlikely to happen, when it happens, we throw exception for the above layer to
      // handle it the way they wish to.
      throw new ClockException(
          "Logical Time Overflowed: " + logicalTime + "max " + "logical time: " + maxLogicalTime);
    }
  }

  static void checkPhysicalTimeOverflow(long physicalTime, long maxPhysicalTime) {
    if (physicalTime >= maxPhysicalTime) {
      // Extremely unlikely to happen, if this happens upper layers may have to kill the server.
      throw new ClockException(
          "Physical Time overflowed: " + physicalTime + " and max physical time:" + maxPhysicalTime);
    }
  }
}
