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
 * <p>
 * Compaction configuration for a particular instance of HStore.
 * Takes into account both global settings and ones set on the column family/store.
 * Control knobs for default compaction algorithm:
 * </p>
 * <p>
 * maxCompactSize - upper bound on file size to be included in minor compactions
 * minCompactSize - lower bound below which compaction is selected without ratio test
 * minFilesToCompact - lower bound on number of files in any minor compaction
 * maxFilesToCompact - upper bound on number of files in any minor compaction
 * compactionRatio - Ratio used for compaction
 * minLocalityToForceCompact - Locality threshold for a store file to major compact (HBASE-11195)
 * </p>
 * Set parameter as "hbase.hstore.compaction.&lt;attribute&gt;"
 */

@InterfaceAudience.Private
public class CompactionConfiguration {

  private static final Log LOG = LogFactory.getLog(CompactionConfiguration.class);

  public static final String HBASE_HSTORE_COMPACTION_RATIO_KEY = "hbase.hstore.compaction.ratio";
  public static final String HBASE_HSTORE_COMPACTION_RATIO_OFFPEAK_KEY =
    "hbase.hstore.compaction.ratio.offpeak";
  public static final String HBASE_HSTORE_COMPACTION_MIN_KEY = "hbase.hstore.compaction.min";
  public static final String HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY =
    "hbase.hstore.compaction.min.size";
  public static final String HBASE_HSTORE_COMPACTION_MAX_KEY = "hbase.hstore.compaction.max";
  public static final String HBASE_HSTORE_COMPACTION_DISCHARGER_THREAD_COUNT =
      "hbase.hstore.compaction.discharger.thread.count";
  public static final String HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY =
    "hbase.hstore.compaction.max.size";
  public static final String HBASE_HSTORE_COMPACTION_MAX_SIZE_OFFPEAK_KEY =
      "hbase.hstore.compaction.max.size.offpeak";
  public static final String HBASE_HSTORE_OFFPEAK_END_HOUR = "hbase.offpeak.end.hour";
  public static final String HBASE_HSTORE_OFFPEAK_START_HOUR = "hbase.offpeak.start.hour";
  public static final String HBASE_HSTORE_MIN_LOCALITY_TO_SKIP_MAJOR_COMPACT =
      "hbase.hstore.min.locality.to.skip.major.compact";

  public static final String HBASE_HFILE_COMPACTION_DISCHARGER_THREAD_COUNT =
      "hbase.hfile.compaction.discharger.thread.count";
  public static final String HBASE_HFILE_COMPACTION_DISCHARGER_INTERVAL =
      "hbase.hfile.compaction.discharger.interval";

  Configuration conf;
  StoreConfigInformation storeConfigInfo;

  private final double offPeakCompactionRatio;
  /** Since all these properties can change online, they are volatile **/
  private final long maxCompactSize;
  private final long offPeakMaxCompactSize;
  private final long minCompactSize;
  private final int minFilesToCompact;
  private final int maxFilesToCompact;
  private final double compactionRatio;
  private final long throttlePoint;
  private final long majorCompactionPeriod;
  private final float majorCompactionJitter;
  private final float minLocalityToForceCompact;

  CompactionConfiguration(Configuration conf, StoreConfigInformation storeConfigInfo) {
    this.conf = conf;
    this.storeConfigInfo = storeConfigInfo;

    maxCompactSize = conf.getLong(HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, Long.MAX_VALUE);
    offPeakMaxCompactSize = conf.getLong(HBASE_HSTORE_COMPACTION_MAX_SIZE_OFFPEAK_KEY, 
      maxCompactSize);      
    minCompactSize = conf.getLong(HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY,
        storeConfigInfo.getMemstoreFlushSize());
    minFilesToCompact = Math.max(2, conf.getInt(HBASE_HSTORE_COMPACTION_MIN_KEY,
          /*old name*/ conf.getInt("hbase.hstore.compactionThreshold", 3)));
    maxFilesToCompact = conf.getInt(HBASE_HSTORE_COMPACTION_MAX_KEY, 10);
    compactionRatio = conf.getFloat(HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2F);
    offPeakCompactionRatio = conf.getFloat(HBASE_HSTORE_COMPACTION_RATIO_OFFPEAK_KEY, 5.0F);

    throttlePoint = conf.getLong("hbase.regionserver.thread.compaction.throttle",
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
      "size [%d, %d, %d); files [%d, %d); ratio %f; off-peak ratio %f; throttle point %d;"
      + " major period %d, major jitter %f, min locality to compact %f",
      minCompactSize,
      maxCompactSize,
      offPeakMaxCompactSize,
      minFilesToCompact,
      maxFilesToCompact,
      compactionRatio,
      offPeakCompactionRatio,
      throttlePoint,
      majorCompactionPeriod,
      majorCompactionJitter,
      minLocalityToForceCompact);
  }

  /**
   * @return lower bound below which compaction is selected without ratio test
   */
  public long getMinCompactSize() {
    return minCompactSize;
  }

  /**
   * @return upper bound on file size to be included in minor compactions
   */
  public long getMaxCompactSize() {
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
  public int getMaxFilesToCompact() {
    return maxFilesToCompact;
  }

  /**
   * @return Ratio used for compaction
   */
  public double getCompactionRatio() {
    return compactionRatio;
  }

  /**
   * @return Off peak Ratio used for compaction
   */
  public double getCompactionRatioOffPeak() {
    return offPeakCompactionRatio;
  }

  /**
   * @return ThrottlePoint used for classifying small and large compactions
   */
  public long getThrottlePoint() {
    return throttlePoint;
  }

  /**
   * @return Major compaction period from compaction.
   * Major compactions are selected periodically according to this parameter plus jitter
   */
  public long getMajorCompactionPeriod() {
    return majorCompactionPeriod;
  }

  /**
   * @return Major the jitter fraction, the fraction within which the major compaction
   *  period is randomly chosen from the majorCompactionPeriod in each store.
   */
  public float getMajorCompactionJitter() {
    return majorCompactionJitter;
  }

  /**
   * @return Block locality ratio, the ratio at which we will include old regions with a single
   * store file for major compaction.  Used to improve block locality for regions that
   * haven't had writes in a while but are still being read.
   */
  public float getMinLocalityToForceCompact() {
    return minLocalityToForceCompact;
  }

  public long getOffPeakMaxCompactSize() {
    return offPeakMaxCompactSize;
  }
  
  public long getMaxCompactSize(boolean mayUseOffpeak) {
    if (mayUseOffpeak) {
      return getOffPeakMaxCompactSize();
    } else {
      return getMaxCompactSize();
    }
  }
}
