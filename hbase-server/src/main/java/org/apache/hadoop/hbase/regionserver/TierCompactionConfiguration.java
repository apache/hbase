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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.text.DecimalFormat;

/**
 * Control knobs for default compaction algorithm
 */
@InterfaceAudience.Private
public class TierCompactionConfiguration extends CompactionConfiguration {

  private CompactionTier[] compactionTier;
  private boolean recentFirstOrder;

  TierCompactionConfiguration(Configuration conf, Store store) {
    super(conf, store);

    String strPrefix = "hbase.hstore.compaction.";
    String strSchema = "tbl." + store.getHRegion().getTableDesc().getNameAsString()
                       + "cf." + store.getFamily().getNameAsString() + ".";
    String strDefault = "Default.";
    String strAttribute;
    // If value not set for family, use default family (by passing null).
    // If default value not set, use 1 tier.

    strAttribute = "NumCompactionTiers";
    compactionTier = new CompactionTier[
      conf.getInt(strPrefix + strSchema  + strAttribute,
      conf.getInt(strPrefix + strDefault + strAttribute,
      1))];

    strAttribute = "IsRecentFirstOrder";
    recentFirstOrder =
      conf.getBoolean(strPrefix + strSchema  + strAttribute,
      conf.getBoolean(strPrefix + strDefault + strAttribute,
      true));

    strAttribute = "MinCompactSize";
    minCompactSize =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      0));

    strAttribute = "MaxCompactSize";
    maxCompactSize =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      Long.MAX_VALUE));

    strAttribute = "ShouldDeleteExpired";
    shouldDeleteExpired =
      conf.getBoolean(strPrefix + strSchema  + strAttribute,
      conf.getBoolean(strPrefix + strDefault + strAttribute,
      shouldDeleteExpired));

    strAttribute = "ThrottlePoint";
    throttlePoint =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      throttlePoint));

    strAttribute = "MajorCompactionPeriod";
    majorCompactionPeriod =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      majorCompactionPeriod));

    strAttribute = "MajorCompactionJitter";
    majorCompactionJitter =
      conf.getFloat(
          strPrefix + strSchema + strAttribute,
          conf.getFloat(
              strPrefix + strDefault + strAttribute,
              majorCompactionJitter
          )
      );

    for (int i = 0; i < compactionTier.length; i++) {
      compactionTier[i] = new CompactionTier(i);
    }
  }
  /**
   * @return Number of compaction Tiers
   */
  int getNumCompactionTiers() {
    return compactionTier.length;
  }

  /**
   * @return The i-th tier from most recent
   */
  CompactionTier getCompactionTier(int i) {
    return compactionTier[i];
  }

  /**
   * @return Whether the tiers will be checked for compaction from newest to oldest
   */
  boolean isRecentFirstOrder() {
    return recentFirstOrder;
  }

  /**
   * Parameters for each tier
   */
  class CompactionTier {

    private long maxAgeInDisk;
    private long maxSize;
    private double tierCompactionRatio;
    private int tierMinFilesToCompact;
    private int tierMaxFilesToCompact;
    private int endingIndexForTier;

    CompactionTier(int tier) {
      String strPrefix = "hbase.hstore.compaction.";
      String strSchema = "tbl." + store.getHRegion().getTableDesc().getNameAsString()
                         + "cf." + store.getFamily().getNameAsString() + ".";
      String strDefault = "Default.";
      String strDefTier = "";
      String strTier = "Tier." + String.valueOf(tier) + ".";
      String strAttribute;

      /**
       * Use value set for current family, current tier
       * If not set, use value set for current family, default tier
       * if not set, use value set for Default family, current tier
       * If not set, use value set for Default family, default tier
       * Else just use a default value
       */

      strAttribute = "MaxAgeInDisk";
      maxAgeInDisk =
        conf.getLong(strPrefix + strSchema  + strTier + strAttribute,
        conf.getLong(strPrefix + strDefault + strTier + strAttribute,
        Long.MAX_VALUE));

      strAttribute = "MaxSize";
      maxSize =
        conf.getLong(strPrefix + strSchema  + strTier + strAttribute,
        conf.getLong(strPrefix + strDefault + strTier + strAttribute,
        Long.MAX_VALUE));

      strAttribute = "CompactionRatio";
      tierCompactionRatio = (double)
        conf.getFloat(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getFloat(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getFloat(strPrefix + strDefault + strTier  + strAttribute,
        conf.getFloat(strPrefix + strDefault + strDefTier + strAttribute,
        (float) compactionRatio))));

      strAttribute = "MinFilesToCompact";
      tierMinFilesToCompact =
        conf.getInt(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getInt(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier  + strAttribute,
        conf.getInt(strPrefix + strDefault + strDefTier + strAttribute,
        minFilesToCompact))));

      strAttribute = "MaxFilesToCompact";
      tierMaxFilesToCompact =
        conf.getInt(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getInt(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier  + strAttribute,
        conf.getInt(strPrefix + strDefault + strDefTier + strAttribute,
        maxFilesToCompact))));

      strAttribute = "EndingIndexForTier";
      endingIndexForTier =
        conf.getInt(strPrefix + strSchema  + strTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier + strAttribute,
        tier));

      //make sure this value is not incorrectly set
      if (endingIndexForTier < 0 || endingIndexForTier > tier) {
        LOG.error("EndingIndexForTier improperly set. Using default value.");
        endingIndexForTier = tier;
      }

    }

    /**
     * @return Upper bound on storeFile's minFlushTime to be included in this tier
     */
    long getMaxAgeInDisk() {
      return maxAgeInDisk;
    }

    /**
     * @return Upper bound on storeFile's size to be included in this tier
     */
    long getMaxSize() {
      return maxSize;
    }

    /**
     * @return Compaction ratio for selections of this tier
     */
    double getCompactionRatio() {
      return tierCompactionRatio;
    }

    /**
     * @return lower bound on number of files in selections of this tier
     */
    int getMinFilesToCompact() {
      return tierMinFilesToCompact;
    }

    /**
     * @return upper bound on number of files in selections of this tier
     */
    int getMaxFilesToCompact() {
      return tierMaxFilesToCompact;
    }

    /**
     * @return the newest tier which will also be included in selections of this tier
     *  by default it is the index of this tier, must be between 0 and this tier
     */
    int getEndingIndexForTier() {
      return endingIndexForTier;
    }

    String getDescription() {
      String ageString = "INF";
      String sizeString = "INF";
      if (getMaxAgeInDisk() < Long.MAX_VALUE) {
        ageString = StringUtils.formatTime(getMaxAgeInDisk());
      }
      if (getMaxSize() < Long.MAX_VALUE) {
        ageString = StringUtils.humanReadableInt(getMaxSize());
      }
      String ret = "Has files upto age " + ageString
          + " and upto size " + sizeString + ". "
          + "Compaction ratio: " + (new DecimalFormat("#.##")).format(getCompactionRatio()) + ", "
          + "Compaction Selection with at least " + getMinFilesToCompact() + " and "
          + "at most " + getMaxFilesToCompact() + " files possible, "
          + "Selections in this tier includes files up to tier " + getEndingIndexForTier();
      return ret;
    }

  }

}