package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.concurrent.TimeUnit;

/**
 * System clock is an implementation of clock which doesn't give any monotonic guarantees.
 * Since it directly represents system's actual clock which cannot be changed, update() function
 * is no-op.
 */
@InterfaceAudience.Private
public class SystemClock implements Clock {
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
