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
package org.apache.hadoop.hbase.regionserver.compactions;

import com.google.common.math.LongMath;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Exponential compaction window implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ExponentialCompactionWindowFactory extends CompactionWindowFactory {

  private static final Log LOG = LogFactory.getLog(ExponentialCompactionWindowFactory.class);

  public static final String BASE_WINDOW_MILLIS_KEY =
    "hbase.hstore.compaction.date.tiered.base.window.millis";
  public static final String WINDOWS_PER_TIER_KEY =
    "hbase.hstore.compaction.date.tiered.windows.per.tier";
  public static final String MAX_TIER_AGE_MILLIS_KEY =
    "hbase.hstore.compaction.date.tiered.max.tier.age.millis";

  private final class Window extends CompactionWindow {

    /**
     * Will not promote to next tier for window before it.
     */
    private final long maxTierAgeCutoff;

    /**
     * How big a range of timestamps fit inside the window in milliseconds.
     */
    private final long windowMillis;

    /**
     * A timestamp t is within the window iff t / size == divPosition.
     */
    private final long divPosition;

    public Window(long baseWindowMillis, long divPosition, long maxTierAgeCutoff) {
      this.windowMillis = baseWindowMillis;
      this.divPosition = divPosition;
      this.maxTierAgeCutoff = maxTierAgeCutoff;
    }

    @Override
    public int compareToTimestamp(long timestamp) {
      if (timestamp < 0) {
        try {
          timestamp = LongMath.checkedSubtract(timestamp, windowMillis - 1);
        } catch (ArithmeticException ae) {
          timestamp = Long.MIN_VALUE;
        }
      }
      long pos = timestamp / windowMillis;
      return divPosition == pos ? 0 : divPosition < pos ? -1 : 1;
    }

    @Override
    public Window nextEarlierWindow() {
      // Don't promote to the next tier if there is not even 1 window at current tier
      // or if the next window crosses the max age.
      if (divPosition % windowsPerTier > 0
          || startMillis() - windowMillis * windowsPerTier < maxTierAgeCutoff) {
        return new Window(windowMillis, divPosition - 1, maxTierAgeCutoff);
      } else {
        return new Window(windowMillis * windowsPerTier, divPosition / windowsPerTier - 1,
            maxTierAgeCutoff);
      }
    }

    @Override
    public long startMillis() {
      try {
        return LongMath.checkedMultiply(windowMillis, divPosition);
      } catch (ArithmeticException ae) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public long endMillis() {
      try {
        return LongMath.checkedMultiply(windowMillis, (divPosition + 1));
      } catch (ArithmeticException ae) {
        return Long.MAX_VALUE;
      }
    }
  }

  private final long baseWindowMillis;
  private final int windowsPerTier;
  private final long maxTierAgeMillis;

  private long getMaxTierAgeCutoff(long now) {
    try {
      return LongMath.checkedSubtract(now, maxTierAgeMillis);
    } catch (ArithmeticException ae) {
      LOG.warn("Value for " + MAX_TIER_AGE_MILLIS_KEY + ": " + maxTierAgeMillis
        + ". Will always promote to next tier.");
      return Long.MIN_VALUE;
    }
  }

  public ExponentialCompactionWindowFactory(CompactionConfiguration comConf) {
    Configuration conf = comConf.conf;
    baseWindowMillis = conf.getLong(BASE_WINDOW_MILLIS_KEY, 3600000 * 6);
    windowsPerTier = conf.getInt(WINDOWS_PER_TIER_KEY, 4);
    maxTierAgeMillis = conf.getLong(MAX_TIER_AGE_MILLIS_KEY,
      comConf.getDateTieredMaxStoreFileAgeMillis());
    LOG.info(this);
  }

  @Override
  public CompactionWindow newIncomingWindow(long now) {
    return new Window(baseWindowMillis, now / baseWindowMillis, getMaxTierAgeCutoff(now));
  }

  @Override
  public String toString() {
    return String.format(
      "%s [base window in milliseconds %d, windows per tier %d, max tier age in milliseconds %d]",
      getClass().getSimpleName(), baseWindowMillis, windowsPerTier, maxTierAgeMillis);
  }

}
