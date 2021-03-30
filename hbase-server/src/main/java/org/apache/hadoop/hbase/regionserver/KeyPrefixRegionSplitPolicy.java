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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups
 * rows by a prefix of the row-key
 *
 * This ensures that a region is not split "inside" a prefix of a row key.
 * I.e. rows can be co-located in a region by their prefix.
 *
 * @deprecated since 3.0.0 and will be removed in 4.0.0. See HBASE-25706.
 */
@Deprecated
@InterfaceAudience.Private
public class KeyPrefixRegionSplitPolicy extends RegionSplitPolicy {
  private static final Logger LOG = LoggerFactory
      .getLogger(KeyPrefixRegionSplitPolicy.class);
  @Deprecated
  public static final String PREFIX_LENGTH_KEY_DEPRECATED = "prefix_split_key_policy.prefix_length";
  public static final String PREFIX_LENGTH_KEY = "KeyPrefixRegionSplitPolicy.prefix_length";
  public static final String BASE_REGION_SPLIT_POLICY_CLASS_KEY =
    "KeyPrefixRegionSplitPolicy.base_region_split_policy_class";

  private int prefixLength = 0;
  private RegionSplitPolicy baseRegionSplitPolicy;

  @Override
  public String toString() {
    return "KeyPrefixRegionSplitPolicy{" + "prefixLength=" + prefixLength + ", " +
      super.toString() + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    prefixLength = 0;

    // read the prefix length from the table descriptor
    String prefixLengthString = region.getTableDescriptor().getValue(
        PREFIX_LENGTH_KEY);
    if (prefixLengthString == null) {
      //read the deprecated value
      prefixLengthString = region.getTableDescriptor().getValue(PREFIX_LENGTH_KEY_DEPRECATED);
      if (prefixLengthString == null) {
        LOG.error(PREFIX_LENGTH_KEY + " not specified for table "
            + region.getTableDescriptor().getTableName()
            + ". Using default RegionSplitPolicy");
        return;
      }
    }
    try {
      prefixLength = Integer.parseInt(prefixLengthString);
    } catch (NumberFormatException nfe) {
      /* Differentiate NumberFormatException from an invalid value range reported below. */
      LOG.error("Number format exception when parsing " + PREFIX_LENGTH_KEY + " for table "
          + region.getTableDescriptor().getTableName() + ":"
          + prefixLengthString + ". " + nfe);
      return;
    }
    if (prefixLength <= 0) {
      LOG.error("Invalid value for " + PREFIX_LENGTH_KEY + " for table "
          + region.getTableDescriptor().getTableName() + ":"
          + prefixLengthString + ". Using default RegionSplitPolicy");
      return;
    }

    baseRegionSplitPolicy = createBaseRegionSplitPolicy(BASE_REGION_SPLIT_POLICY_CLASS_KEY,
      region, getConf());
  }

  static RegionSplitPolicy createBaseRegionSplitPolicy(String baseRegionSplitPolicyClassKey,
    HRegion region, Configuration conf) {
    // read the base region split policy class name from the table descriptor
    String baseRegionSplitPolicyClassName = region.getTableDescriptor().getValue(
      baseRegionSplitPolicyClassKey);
    if (baseRegionSplitPolicyClassName == null) {
      baseRegionSplitPolicyClassName = RegionSplitPolicy.DEFAULT_SPLIT_POLICY_CLASS.getName();
    }

    RegionSplitPolicy ret = null;
    try {
      ret = newBaseRegionSplitPolicyInstance(baseRegionSplitPolicyClassName, conf);
    } catch (Exception e) {
      LOG.error("Invalid class for " + baseRegionSplitPolicyClassKey + " for table "
        + region.getTableDescriptor().getTableName() + ":"
        + baseRegionSplitPolicyClassName + ". Using default RegionSplitPolicy", e);
      try {
        ret =
          newBaseRegionSplitPolicyInstance(RegionSplitPolicy.DEFAULT_SPLIT_POLICY_CLASS.getName(),
            conf);
      } catch (Exception ignored) {
        // Ignore it because creating a instance of the default RegionSplitPolicy should at least
        // succeed
      }
    }
    // ret should not be null because creating a instance of the default RegionSplitPolicy should
    // at least succeed
    assert ret != null;

    ret.configureForRegion(region);
    return ret;
  }

  private static RegionSplitPolicy newBaseRegionSplitPolicyInstance(
    String baseRegionSplitPolicyClassName, Configuration conf) throws ClassNotFoundException {
    Class<? extends RegionSplitPolicy> baseRegionSplitPolicyClass =
      Class.forName(baseRegionSplitPolicyClassName).asSubclass(RegionSplitPolicy.class);
    return ReflectionUtils.newInstance(baseRegionSplitPolicyClass, conf);
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
    if (prefixLength > 0 && splitPoint != null && splitPoint.length > 0) {
      // group split keys by a prefix
      return Arrays.copyOf(splitPoint,
          Math.min(prefixLength, splitPoint.length));
    } else {
      return splitPoint;
    }
  }

  @Override
  protected boolean skipStoreFileRangeCheck(String familyName) {
    return baseRegionSplitPolicy.skipStoreFileRangeCheck(familyName);
  }
}
