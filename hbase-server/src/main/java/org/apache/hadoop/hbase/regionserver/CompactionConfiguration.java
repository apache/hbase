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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Control knobs for default compaction algorithm :
 * <p/>
 * maxCompactSize - upper bound on file size to be included in minor compactions
 * minCompactSize - lower bound below which compaction is selected without ratio test
 * minFilesToCompact - lower bound on number of files in any minor compaction
 * maxFilesToCompact - upper bound on number of files in any minor compaction
 * compactionRatio - Ratio used for compaction
 * <p/>
 * Set parameter as "hbase.hstore.compaction.<attribute>"
 */

//TODO: revisit this class for online parameter updating

public class CompactionConfiguration {

  static final Log LOG = LogFactory.getLog(CompactionConfiguration.class);

  Configuration conf;
  Store store;

  long maxCompactSize;
  long minCompactSize;
  int minFilesToCompact;
  int maxFilesToCompact;
  double compactionRatio;
  double offPeekCompactionRatio;
  int offPeakStartHour;
  int offPeakEndHour;
  long throttlePoint;
  boolean shouldDeleteExpired;
  long majorCompactionPeriod;
  float majorCompactionJitter;

  CompactionConfiguration(Configuration conf, Store store) {
    this.conf = conf;
    this.store = store;

    String strPrefix = "hbase.hstore.compaction.";

    maxCompactSize = conf.getLong(strPrefix + "max.size", Long.MAX_VALUE);
    minCompactSize = conf.getLong(strPrefix + "min.size", store.getHRegion().memstoreFlushSize);
    minFilesToCompact = Math.max(2, conf.getInt(strPrefix + "min",
          /*old name*/ conf.getInt("hbase.hstore.compactionThreshold", 3)));
    maxFilesToCompact = conf.getInt(strPrefix + "max", 10);
    compactionRatio = conf.getFloat(strPrefix + "ratio", 1.2F);
    offPeekCompactionRatio = conf.getFloat(strPrefix + "ratio.offpeak", 5.0F);
    offPeakStartHour = conf.getInt("hbase.offpeak.start.hour", -1);
    offPeakEndHour = conf.getInt("hbase.offpeak.end.hour", -1);

    if (!isValidHour(offPeakStartHour) || !isValidHour(offPeakEndHour)) {
      if (!(offPeakStartHour == -1 && offPeakEndHour == -1)) {
        LOG.warn("Ignoring invalid start/end hour for peak hour : start = " +
            this.offPeakStartHour + " end = " + this.offPeakEndHour +
            ". Valid numbers are [0-23]");
      }
      this.offPeakStartHour = this.offPeakEndHour = -1;
    }

    throttlePoint =  conf.getLong("hbase.regionserver.thread.compaction.throttle",
          2 * maxFilesToCompact * store.getHRegion().memstoreFlushSize);
    shouldDeleteExpired = conf.getBoolean("hbase.store.delete.expired.storefile", true);
    majorCompactionPeriod = conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD, 1000*60*60*24);
    majorCompactionJitter = conf.getFloat("hbase.hregion.majorcompaction.jitter", 0.20F);

    LOG.info("Compaction configuration " + this.toString());
  }

  @Override
  public String toString() {
    return String.format(
      "size [%d, %d); files [%d, %d); ratio %f; off-peak ratio %f; off-peak hours %d-%d; " +
      "throttle point %d;%s delete expired; major period %d, major jitter %f",
      minCompactSize,
      maxCompactSize,
      minFilesToCompact,
      maxFilesToCompact,
      compactionRatio,
      offPeekCompactionRatio,
      offPeakStartHour,
      offPeakEndHour,
      throttlePoint,
      shouldDeleteExpired ? "" : " don't",
      majorCompactionPeriod,
      majorCompactionJitter);
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
   * @return Ratio used for compaction
   */
  double getCompactionRatio() {
    return compactionRatio;
  }

  /**
   * @return Off peak Ratio used for compaction
   */
  double getCompactionRatioOffPeak() {
    return offPeekCompactionRatio;
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
  
  private static boolean isValidHour(int hour) {
    return (hour >= 0 && hour <= 23);
  }
}