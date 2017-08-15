package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hbase.util.AtomicUtils.updateMax;

/**
 * System monotonic clock is an implementation of clock which guarantees monotonically
 * non-decreasing timestamps.
 */
@InterfaceAudience.Private
public class SystemMonotonicClock implements Clock {
  static final Clock SYSTEM_CLOCK = new SystemClock();
  private final long maxClockSkewInMs;
  private final Clock systemClock;
  private final AtomicLong physicalTime = new AtomicLong();

  public SystemMonotonicClock(long maxClockSkewInMs) {
    this(SYSTEM_CLOCK, maxClockSkewInMs);
  }

  @VisibleForTesting
  public SystemMonotonicClock() {
    this(DEFAULT_MAX_CLOCK_SKEW_IN_MS);
  }

  @VisibleForTesting
  public SystemMonotonicClock(Clock systemClock) {
    this(systemClock, DEFAULT_MAX_CLOCK_SKEW_IN_MS);
  }

  @VisibleForTesting
  public SystemMonotonicClock(Clock systemClock, long maxClockSkewInMs) {
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
