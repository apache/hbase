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

package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;

/**
 * Compaction configuration for a particular instance of HStore.
 * Takes into account both global settings and ones set on the column family/store.
 * Control knobs for default compaction algorithm:
 * <p/>
 * maxCompactSize - upper bound on file size to be included in minor compactions
 * minCompactSize - lower bound below which compaction is selected without ratio test
 * minFilesToCompact - lower bound on number of files in any minor compaction
 * maxFilesToCompact - upper bound on number of files in any minor compaction
 * compactionRatio - Ratio used for compaction
 * <p/>
 * Set parameter as "hbase.hstore.compaction.<attribute>"
 */

//TODO: revisit this class for online parameter updating (both in xml and on the CF)
@InterfaceAudience.Private
public class CompactionConfiguration {

  static final Log LOG = LogFactory.getLog(CompactionConfiguration.class);

  private static final String CONFIG_PREFIX = "hbase.hstore.compaction.";
  public static final String HBASE_HSTORE_MIN_LOCALITY_TO_SKIP_MAJOR_COMPACT =
      "hbase.hstore.min.locality.to.skip.major.compact";
  public static final String RATIO_KEY = CONFIG_PREFIX + "ratio";
  public static final String MIN_KEY = CONFIG_PREFIX + "min";
  public static final String MAX_KEY = CONFIG_PREFIX + "max";

  Configuration conf;
  StoreConfigInformation storeConfigInfo;

  long maxCompactSize;
  long minCompactSize;
  int minFilesToCompact;
  int maxFilesToCompact;
  double compactionRatio;
  double offPeekCompactionRatio;
  long throttlePoint;
  long majorCompactionPeriod;
  float majorCompactionJitter;
  final float minLocalityToForceCompact;


  CompactionConfiguration(Configuration conf, StoreConfigInformation storeConfigInfo) {
    this.conf = conf;
    this.storeConfigInfo = storeConfigInfo;

    maxCompactSize = conf.getLong(CONFIG_PREFIX + "max.size", Long.MAX_VALUE);
    minCompactSize = conf.getLong(CONFIG_PREFIX + "min.size",
        storeConfigInfo.getMemstoreFlushSize());
    minFilesToCompact = Math.max(2, conf.getInt(MIN_KEY,
          /*old name*/ conf.getInt("hbase.hstore.compactionThreshold", 3)));
    maxFilesToCompact = conf.getInt(MAX_KEY, 10);
    compactionRatio = conf.getFloat(RATIO_KEY, 1.2F);
    offPeekCompactionRatio = conf.getFloat(CONFIG_PREFIX + "ratio.offpeak", 5.0F);

    throttlePoint =  conf.getLong("hbase.regionserver.thread.compaction.throttle",
          2 * maxFilesToCompact * storeConfigInfo.getMemstoreFlushSize());
    majorCompactionPeriod = conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD, 1000*60*60*24*7);
    // Make it 0.5 so jitter has us fall evenly either side of when the compaction should run
    majorCompactionJitter = conf.getFloat("hbase.hregion.majorcompaction.jitter", 0.50F);
    minLocalityToForceCompact = conf.getFloat(HBASE_HSTORE_MIN_LOCALITY_TO_SKIP_MAJOR_COMPACT, 0f);

    LOG.info(this);
  }

  @Override
  public String toString() {
    return String.format(
      "size [%d, %d); files [%d, %d); ratio %f; off-peak ratio %f; throttle point %d;"
      + " major period %d, major jitter %f, min locality to compact %f\"",
      minCompactSize,
      maxCompactSize,
      minFilesToCompact,
      maxFilesToCompact,
      compactionRatio,
      offPeekCompactionRatio,
      throttlePoint,
      majorCompactionPeriod,
      majorCompactionJitter,
      minLocalityToForceCompact
    );
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
  public int getMinFilesToCompact() {
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
   * @return Major the jitter fraction, the fraction within which the major compaction
   *  period is randomly chosen from the majorCompactionPeriod in each store.
   */
  float getMajorCompactionJitter() {
    return majorCompactionJitter;
  }

  /**
   * @return Block locality ratio, the ratio at which we will include old regions with a single
   * store file for major compaction.  Used to improve block locality for regions that
   * haven't had writes in a while but are still being read.
   */
  float getMinLocalityToForceCompact() {
    return minLocalityToForceCompact;
  }
}
