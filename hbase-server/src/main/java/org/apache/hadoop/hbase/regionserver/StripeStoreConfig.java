/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;

/**
 * Configuration class for stripe store and compactions.
 * See {@link StripeStoreFileManager} for general documentation.
 * See getters for the description of each setting.
 */
@InterfaceAudience.Private
public class StripeStoreConfig {
  private static final Log LOG = LogFactory.getLog(StripeStoreConfig.class);

  /** The maximum number of files to compact within a stripe; same as for regular compaction. */
  public static final String MAX_FILES_KEY = "hbase.store.stripe.compaction.maxFiles";
  /** The minimum number of files to compact within a stripe; same as for regular compaction. */
  public static final String MIN_FILES_KEY = "hbase.store.stripe.compaction.minFiles";

  /**  The minimum number of files to compact when compacting L0; same as minFiles for regular
   * compaction. Given that L0 causes unnecessary overwriting of the data, should be higher than
   * regular minFiles. */
  public static final String MIN_FILES_L0_KEY = "hbase.store.stripe.compaction.minFilesL0";

  /** The size the stripe should achieve to be considered for splitting into multiple stripes.
   Stripe will be split when it can be fully compacted, and it is above this size. */
  public static final String SIZE_TO_SPLIT_KEY = "hbase.store.stripe.sizeToSplit";
  /** The target count of new stripes to produce when splitting a stripe. A floating point
   number, default is 2. Values less than 1 will be converted to 1/x. Non-whole numbers will
   produce unbalanced splits, which may be good for some cases. In this case the "smaller" of
   the new stripes will always be the rightmost one. If the stripe is bigger than sizeToSplit
   when splitting, this will be adjusted by a whole increment. */
  public static final String SPLIT_PARTS_KEY = "hbase.store.stripe.splitPartCount";
  /** The initial stripe count to create. If the row distribution is roughly the same over time,
   it's good to set this to a count of stripes that is expected to be achieved in most regions,
   to get this count from the outset and prevent unnecessary splitting. */
  public static final String INITIAL_STRIPE_COUNT_KEY = "hbase.store.stripe.initialStripeCount";

  /** Whether to flush memstore to L0 files, or directly to stripes. */
  public static final String FLUSH_TO_L0_KEY = "hbase.store.stripe.compaction.flushToL0";

  /** When splitting region, the maximum size imbalance to allow in an attempt to split at a
   stripe boundary, so that no files go to both regions. Most users won't need to change that. */
  public static final String MAX_REGION_SPLIT_IMBALANCE_KEY =
      "hbase.store.stripe.region.split.max.imbalance";


  private final float maxRegionSplitImbalance;
  private final int level0CompactMinFiles;
  private final int stripeCompactMinFiles;
  private final int stripeCompactMaxFiles;

  private final int initialCount;
  private final long sizeToSplitAt;
  private final float splitPartCount;
  private final boolean flushIntoL0;
  private final long splitPartSize; // derived from sizeToSplitAt and splitPartCount

  private static final double EPSILON = 0.001; // good enough for this, not a real epsilon.
  public StripeStoreConfig(Configuration config, StoreConfigInformation sci) {
    this.level0CompactMinFiles = config.getInt(MIN_FILES_L0_KEY, 4);
    this.flushIntoL0 = config.getBoolean(FLUSH_TO_L0_KEY, false);
    int minMinFiles = flushIntoL0 ? 3 : 4; // make sure not to compact tiny files too often.
    int minFiles = config.getInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, -1);
    this.stripeCompactMinFiles = config.getInt(MIN_FILES_KEY, Math.max(minMinFiles, minFiles));
    this.stripeCompactMaxFiles = config.getInt(MAX_FILES_KEY,
        config.getInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 10));
    this.maxRegionSplitImbalance = getFloat(config, MAX_REGION_SPLIT_IMBALANCE_KEY, 1.5f, true);

    float splitPartCount = getFloat(config, SPLIT_PARTS_KEY, 2f, true);
    if (Math.abs(splitPartCount - 1.0) < EPSILON) {
      LOG.error("Split part count cannot be 1 (" + splitPartCount + "), using the default");
      splitPartCount = 2f;
    }
    this.splitPartCount = splitPartCount;
    // Arbitrary default split size - 4 times the size of one L0 compaction.
    // If we flush into L0 there's no split compaction, but for default value it is ok.
    double flushSize = sci.getMemstoreFlushSize();
    if (flushSize == 0) {
      flushSize = 128 * 1024 * 1024;
    }
    long defaultSplitSize = (long)(flushSize * getLevel0MinFiles() * 4 * splitPartCount);
    this.sizeToSplitAt = config.getLong(SIZE_TO_SPLIT_KEY, defaultSplitSize);
    int initialCount = config.getInt(INITIAL_STRIPE_COUNT_KEY, 1);
    if (initialCount == 0) {
      LOG.error("Initial stripe count is 0, using the default");
      initialCount = 1;
    }
    this.initialCount = initialCount;
    this.splitPartSize = (long)(this.sizeToSplitAt / this.splitPartCount);
  }

  private static float getFloat(
      Configuration config, String key, float defaultValue, boolean moreThanOne) {
    float value = config.getFloat(key, defaultValue);
    if (value < EPSILON) {
      LOG.warn(String.format(
          "%s is set to 0 or negative; using default value of %f", key, defaultValue));
      value = defaultValue;
    } else if ((value > 1f) != moreThanOne) {
      value = 1f / value;
    }
    return value;
  }

  public float getMaxSplitImbalance() {
    return this.maxRegionSplitImbalance;
  }

  public int getLevel0MinFiles() {
    return level0CompactMinFiles;
  }

  public int getStripeCompactMinFiles() {
    return stripeCompactMinFiles;
  }

  public int getStripeCompactMaxFiles() {
    return stripeCompactMaxFiles;
  }

  public boolean isUsingL0Flush() {
    return flushIntoL0;
  }

  public long getSplitSize() {
    return sizeToSplitAt;
  }

  public int getInitialCount() {
    return initialCount;
  }

  public float getSplitCount() {
    return splitPartCount;
  }

  /**
   * @return the desired size of the target stripe when splitting, in bytes.
   *         Derived from {@link #getSplitSize()} and {@link #getSplitCount()}.
   */
  public long getSplitPartSize() {
    return splitPartSize;
  }
}
