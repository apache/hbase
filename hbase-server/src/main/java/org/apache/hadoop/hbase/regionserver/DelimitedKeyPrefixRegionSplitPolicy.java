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

package org.apache.hadoop.hbase.regionserver;

import java.util.Arrays;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups
 * rows by a prefix of the row-key with a delimiter. Only the first delimiter
 * for the row key will define the prefix of the row key that is used for grouping.
 *
 * This ensures that a region is not split "inside" a prefix of a row key.
 * I.e. rows can be co-located in a region by their prefix.
 *
 * As an example, if you have row keys delimited with <code>_</code>, like
 * <code>userid_eventtype_eventid</code>, and use prefix delimiter _, this split policy
 * ensures that all rows starting with the same userid, belongs to the same region.
 * @see KeyPrefixRegionSplitPolicy
 */
@InterfaceAudience.Private
public class DelimitedKeyPrefixRegionSplitPolicy extends RegionSplitPolicy {

  private static final Logger LOG = LoggerFactory
      .getLogger(DelimitedKeyPrefixRegionSplitPolicy.class);
  public static final String DELIMITER_KEY = "DelimitedKeyPrefixRegionSplitPolicy.delimiter";
  public static final String BASE_REGION_SPLIT_POLICY_CLASS_KEY =
    "DelimitedKeyPrefixRegionSplitPolicy.base_region_split_policy_class";
  public static final String DEFAULT_BASE_REGION_SPLIT_POLICY_CLASS =
    "org.apache.hadoop.hbase.regionserver.SteppingSplitPolicy";

  private byte[] delimiter = null;
  private RegionSplitPolicy baseRegionSplitPolicy;

  @Override
  public String toString() {
    return "DelimitedKeyPrefixRegionSplitPolicy{" + "delimiter=" + Bytes.toStringBinary(delimiter) +
      ", " + super.toString() + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    // read the prefix length from the table descriptor
    String delimiterString = region.getTableDescriptor().getValue(DELIMITER_KEY);
    if (delimiterString == null || delimiterString.length() == 0) {
      LOG.error(DELIMITER_KEY + " not specified for table " +
        region.getTableDescriptor().getTableName() + ". Using default RegionSplitPolicy");
      return;
    }
    delimiter = Bytes.toBytes(delimiterString);

    // read the base region split policy class name from the table descriptor
    String baseRegionSplitPolicyClassName = region.getTableDescriptor().getValue(
      BASE_REGION_SPLIT_POLICY_CLASS_KEY);
    if (baseRegionSplitPolicyClassName == null) {
      baseRegionSplitPolicyClassName = DEFAULT_BASE_REGION_SPLIT_POLICY_CLASS;
    }

    try {
      baseRegionSplitPolicy = newBaseRegionSplitPolicy(baseRegionSplitPolicyClassName);
    } catch (Exception e) {
      LOG.error("Invalid class for " + BASE_REGION_SPLIT_POLICY_CLASS_KEY + " for table "
        + region.getTableDescriptor().getTableName() + ":"
        + baseRegionSplitPolicyClassName + ". Using default RegionSplitPolicy", e);
      try {
        baseRegionSplitPolicy = newBaseRegionSplitPolicy(DEFAULT_BASE_REGION_SPLIT_POLICY_CLASS);
      } catch (Exception ignored) {
      }
    }
    baseRegionSplitPolicy.configureForRegion(region);
  }

  private RegionSplitPolicy newBaseRegionSplitPolicy(String baseRegionSplitPolicyClassName)
    throws ClassNotFoundException {
    Class<? extends RegionSplitPolicy> baseRegionSplitPolicyClass =
      Class.forName(baseRegionSplitPolicyClassName).asSubclass(RegionSplitPolicy.class);
    return ReflectionUtils.newInstance(baseRegionSplitPolicyClass, getConf());
  }

  @Override
  protected boolean shouldSplit() {
    return baseRegionSplitPolicy.shouldSplit();
  }

  @Override
  protected boolean canSplit() {
    return baseRegionSplitPolicy.canSplit();
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = baseRegionSplitPolicy.getSplitPoint();
    if (splitPoint != null && delimiter != null) {

      //find the first occurrence of delimiter in split point
      int index =
        org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.indexOf(splitPoint, delimiter);
      if (index < 0) {
        LOG.warn("Delimiter " + Bytes.toString(delimiter) + "  not found for split key "
            + Bytes.toString(splitPoint));
        return splitPoint;
      }

      // group split keys by a prefix
      return Arrays.copyOf(splitPoint, Math.min(index, splitPoint.length));
    } else {
      return splitPoint;
    }
  }

  @Override
  protected boolean skipStoreFileRangeCheck(String familyName) {
    return baseRegionSplitPolicy.skipStoreFileRangeCheck(familyName);
  }
}
