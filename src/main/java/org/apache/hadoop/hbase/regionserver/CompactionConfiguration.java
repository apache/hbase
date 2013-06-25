/**
 * Copyright 2012 The Apache Software Foundation
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Control knobs for default compaction algorithm :
 * <p/>
 * maxCompactSize - upper bound on file size to be included in minor compactions
 * minCompactSize - lower bound below which compaction is selected without ratio test
 * shouldExcludeBulk - whether to exclude bulk import files from minor compactions
 * minFilesToCompact - lower bound on number of files in any minor compaction
 * maxFilesToCompact - upper bound on number of files in any minor compaction
 * compactionRatio - Ratio used for compaction
 * <p/>
 * Set parameter as "hbase.hstore.compaction.<attribute>"
 */

//TODO: revisit this class for online parameter updating

public class CompactionConfiguration {

  static final Log LOG = LogFactory.getLog(CompactionManager.class);

  Configuration conf;
  Store store;

  /** Since all these properties can change online, they are volatile **/
  volatile long maxCompactSize;
  volatile long minCompactSize;
  volatile boolean shouldExcludeBulk;
  volatile int minFilesToCompact;
  volatile int maxFilesToCompact;
  volatile double compactionRatio;
  volatile double offPeakCompactionRatio;
  volatile int offPeakStartHour;
  volatile int offPeakEndHour;
  volatile long throttlePoint;
  volatile boolean shouldDeleteExpired;
  volatile long majorCompactionPeriod;
  volatile float majorCompactionJitter;

  /** Default values for the properties **/
  static final long defaultMaxCompactSize = Long.MAX_VALUE;
  static final boolean defaultShouldExcludeBulk = false;
  static final int defaultMaxFilesToCompact = 10;
  static final float defaultCompactionRatio = 1.2F;
  static final float defaultOffPeakCompactionRatio = 5.0F;
  static final int defaultOffPeakStartHour = -1;
  static final int defaultOffPeakEndHour = -1;
  static final boolean defaultShouldDeleteExpired = true;
  static final long defaultMajorCompactionPeriod = 1000*60*60*24;
  static final float defaultMajorCompactionJitter = 0.20F;

  CompactionConfiguration(Configuration conf, Store store) {
    this.conf = conf;
    this.store = store;

    String strPrefix = HConstants.HSTORE_COMPACTION_PREFIX;

    maxCompactSize = conf.getLong(strPrefix + "max.size", defaultMaxCompactSize);
    minCompactSize = conf.getLong(strPrefix + "min.size",
            store.getHRegion().memstoreFlushSize);
    shouldExcludeBulk = conf.getBoolean(strPrefix + "exclude.bulk",
            defaultShouldExcludeBulk);
    minFilesToCompact = Math.max(2, conf.getInt(strPrefix + "min",
          /*old name*/ conf.getInt("hbase.hstore.compactionThreshold",
                                    HConstants.DEFAULT_MIN_FILES_TO_COMPACT)));
    maxFilesToCompact = conf.getInt(strPrefix + "max",
            defaultMaxFilesToCompact);
    compactionRatio = conf.getFloat(strPrefix + "ratio",
            defaultCompactionRatio);
    offPeakCompactionRatio = conf.getFloat(strPrefix + "ratio.offpeak",
            defaultOffPeakCompactionRatio);

    offPeakStartHour = conf.getInt("hbase.offpeak.start.hour",
            defaultOffPeakStartHour);
    offPeakEndHour = conf.getInt("hbase.offpeak.end.hour",
            defaultOffPeakEndHour);

    throttlePoint =
            conf.getLong("hbase.regionserver.thread.compaction.throttle",
            2 * maxFilesToCompact * store.getHRegion().memstoreFlushSize);
    shouldDeleteExpired =
            conf.getBoolean("hbase.store.delete.expired.storefile",
                            defaultShouldDeleteExpired);
    majorCompactionPeriod =
            conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD,
                        defaultMajorCompactionPeriod);
    majorCompactionJitter =
            conf.getFloat("hbase.hregion.majorcompaction.jitter",
                          defaultMajorCompactionJitter);
  }

  /**
   * @return lower bound below which compaction is selected without ratio test
   */
  long getMinCompactSize() {
    return minCompactSize;
  }

  /**
   * @return upper bound on file size to be included in minor compactions
   */
  long getMaxCompactSize() {
    return maxCompactSize;
  }

  /**
   * @return upper bound on number of files to be included in minor compactions
   */
  int getMinFilesToCompact() {
    return minFilesToCompact;
  }

  /**
   * @return upper bound on number of files to be included in minor compactions
   */
  int getMaxFilesToCompact() {
    return maxFilesToCompact;
  }

  /**
   * @return whether to exclude bulk import files from minor compactions
   */
  boolean shouldExcludeBulk() {
    return shouldExcludeBulk;
  }

  /**
   * @return Ratio used for compaction
   */
  double getCompactionRatio() {
    return compactionRatio;
  }

  /**
   * @return Off peak Ratio used for compaction
   */
  double getCompactionRatioOffPeak() {
    return offPeakCompactionRatio;
  }

  /**
   * @return Hour at which off-peak compactions start
   */
  int getOffPeakStartHour() {
    return offPeakStartHour;
  }

  /**
   * @return Hour at which off-peak compactions end
   */
  int getOffPeakEndHour() {
    return offPeakEndHour;
  }

  /**
   * @return ThrottlePoint used for classifying small and large compactions
   */
  long getThrottlePoint() {
    return throttlePoint;
  }

  /**
   * @return Major compaction period from compaction.
   * Major compactions are selected periodically according to this parameter plus jitter
   */
  long getMajorCompactionPeriod() {
    return majorCompactionPeriod;
  }

  /**
   * @return Major the jitter fraction, the fraction within which the major compaction period is
   *  randomly chosen from the majorCompactionPeriod in each store.
   */
  float getMajorCompactionJitter() {
    return majorCompactionJitter;
  }

  /**
   * @return Whether expired files should be deleted ASAP using compactions
   */
  boolean shouldDeleteExpired() {
    return shouldDeleteExpired;
  }

  /**
   * Update the compaction configuration, when an online change is made.
   *
   * @param newConf
   */
  protected void updateConfiguration(Configuration newConf) {
    String strPrefix = HConstants.HSTORE_COMPACTION_PREFIX;

    // Check if the compaction ratio has changed.
    String compactionRatioStr = strPrefix + "ratio";
    double newCompactionRatio = newConf.getFloat(compactionRatioStr,
            defaultCompactionRatio);
    if (newCompactionRatio != this.compactionRatio) {
      LOG.info("Changing the value of " + compactionRatioStr + " from " +
              this.compactionRatio + " to " + newCompactionRatio);
      this.compactionRatio = newCompactionRatio;
    }

    // Check if the off peak compaction ratio has changed.
    String offPeakCompactionRatioStr = strPrefix + "ratio.offpeak";
    double newOffPeakCompactionRatio =
            newConf.getFloat(offPeakCompactionRatioStr,
                    defaultOffPeakCompactionRatio);
    if (newOffPeakCompactionRatio != this.offPeakCompactionRatio) {
      LOG.info("Changing the value of " + offPeakCompactionRatioStr + " from " +
              this.offPeakCompactionRatio + " to " + newOffPeakCompactionRatio);
      this.offPeakCompactionRatio = newOffPeakCompactionRatio;
    }

    // Check if the throttle point has changed.
    String throttlePointStr = "hbase.regionserver.thread.compaction.throttle";
    long newThrottlePoint = newConf.getLong(throttlePointStr,
            2 * maxFilesToCompact * store.getHRegion().memstoreFlushSize);
    if (newThrottlePoint != this.throttlePoint) {
      LOG.info("Changing the value of " + throttlePointStr + " from " +
              this.throttlePoint + " to " + newThrottlePoint);
      this.throttlePoint = newThrottlePoint;
    }

    // Check if the minFilesToCompact has changed.
    String minFilesToCompactStr = strPrefix + "min";
    int newMinFilesToCompact = Math.max(2, newConf.getInt(minFilesToCompactStr,
          /*old name*/ newConf.getInt("hbase.hstore.compactionThreshold",
            HConstants.DEFAULT_MIN_FILES_TO_COMPACT)));
    if (newMinFilesToCompact != this.minFilesToCompact) {
      LOG.info("Changing the value of " + minFilesToCompactStr + " from " +
              this.minFilesToCompact + " to " + newMinFilesToCompact);
      this.minFilesToCompact = newMinFilesToCompact;
    }

    // Check if the maxFile to compact has changed.
    String maxFilesToCompactStr = strPrefix + "max";
    int newMaxFilesToCompact = newConf.getInt(maxFilesToCompactStr,
            defaultMaxFilesToCompact);
    if (newMaxFilesToCompact != this.maxFilesToCompact) {
      LOG.info("Changing the value of " + maxFilesToCompactStr + " from " +
              this.maxFilesToCompact + " to " + newMaxFilesToCompact);
      this.maxFilesToCompact = newMaxFilesToCompact;
    }

    // Check if the Off Peak Start Hour has changed.
    String offPeakStartHourStr = "hbase.offpeak.start.hour";
    int newOffPeakStartHour = newConf.getInt(offPeakStartHourStr,
            defaultOffPeakStartHour);
    if (newOffPeakStartHour != this.offPeakStartHour) {
      LOG.info("Changing the value of " + offPeakStartHourStr + " from " +
              this.offPeakStartHour + " to " + newOffPeakStartHour);
      this.offPeakStartHour = newOffPeakStartHour;
    }

    // Check if the Off Peak End Hour has changed.
    String offPeakEndHourStr = "hbase.offpeak.end.hour";
    int newOffPeakEndHour = newConf.getInt(offPeakEndHourStr,
            defaultOffPeakEndHour);
    if (newOffPeakEndHour != this.offPeakEndHour) {
      LOG.info("Changing the value of " + offPeakEndHourStr + " from " +
              this.offPeakEndHour + " to " + newOffPeakEndHour);
      this.offPeakEndHour = newOffPeakEndHour;
    }

    // Check if the Min Compaction Size has changed
    String minCompactSizeStr = strPrefix + "min.size";
    long newMinCompactSize = newConf.getLong(minCompactSizeStr,
            store.getHRegion().memstoreFlushSize);
    if (newMinCompactSize != this.minCompactSize) {
      LOG.info("Changing the value of " + minCompactSizeStr + " from " +
              this.minCompactSize + " to " + newMinCompactSize);
      this.minCompactSize = newMinCompactSize;
    }

    // Check if the Max Compaction Size has changed.
    String maxCompactSizeStr = strPrefix + "max.size";
    long newMaxCompactSize = newConf.getLong(maxCompactSizeStr,
            defaultMaxCompactSize);
    if (newMaxCompactSize != this.maxCompactSize) {
      LOG.info("Changing the value of " + maxCompactSizeStr + " from " +
              this.maxCompactSize + " to " + newMaxCompactSize);
      this.maxCompactSize = newMaxCompactSize;
    }

    // Check if shouldExcludeBulk has changed.
    String shouldExcludeBulkStr = strPrefix + "exclude.bulk";
    boolean newShouldExcludeBulk = newConf.getBoolean(shouldExcludeBulkStr,
            defaultShouldExcludeBulk);
    if (newShouldExcludeBulk != this.shouldExcludeBulk) {
      LOG.info("Changing the value of " + shouldExcludeBulkStr + " from " +
              this.shouldExcludeBulk + " to " + newShouldExcludeBulk);
      this.shouldExcludeBulk = newShouldExcludeBulk;
    }

    // Check if shouldDeleteExpired has changed.
    String shouldDeleteExpiredStr = "hbase.store.delete.expired.storefile";
    boolean newShouldDeleteExpired =
            newConf.getBoolean(shouldDeleteExpiredStr,
                    defaultShouldDeleteExpired);
    if (newShouldDeleteExpired != this.shouldDeleteExpired) {
      LOG.info("Changing the value of " + shouldDeleteExpiredStr + " from " +
              this.shouldDeleteExpired + " to " + newShouldDeleteExpired);
      this.shouldDeleteExpired = newShouldDeleteExpired;
    }

    // Check if majorCompactionPeriod has changed.
    long newMajorCompactionPeriod =
            newConf.getLong(HConstants.MAJOR_COMPACTION_PERIOD,
                    defaultMajorCompactionPeriod);
    if (newMajorCompactionPeriod != this.majorCompactionPeriod) {
      LOG.info("Changing the value of " + HConstants.MAJOR_COMPACTION_PERIOD +
              " from " + this.majorCompactionPeriod + " to " +
              newMajorCompactionPeriod);
      this.majorCompactionPeriod = newMajorCompactionPeriod;
    }

    // Check if majorCompactionJitter has changed.
    String majorCompactionJitterStr = "hbase.hregion.majorcompaction.jitter";
    float newMajorCompactionJitter =
            newConf.getFloat(majorCompactionJitterStr,
                    defaultMajorCompactionJitter);
    if (newMajorCompactionJitter != this.majorCompactionJitter) {
      LOG.info("Changing the value of " + majorCompactionJitterStr + " from " +
               this.majorCompactionJitter + " to " + newMajorCompactionJitter);
      this.majorCompactionJitter = newMajorCompactionJitter;
    }
    this.conf = newConf;
  }
}