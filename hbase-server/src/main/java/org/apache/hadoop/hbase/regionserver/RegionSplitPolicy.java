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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

/**
 * A split policy determines when a region should be split.
 * @see IncreasingToUpperBoundRegionSplitPolicy Default split policy since
 *      0.94.0
 * @see ConstantSizeRegionSplitPolicy Default split policy before 0.94.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class RegionSplitPolicy extends Configured {
  private static final Class<? extends RegionSplitPolicy>
    DEFAULT_SPLIT_POLICY_CLASS = IncreasingToUpperBoundRegionSplitPolicy.class;

  /**
   * The region configured for this split policy.
   */
  protected HRegion region;

  /**
   * Upon construction, this method will be called with the region
   * to be governed. It will be called once and only once.
   */
  protected void configureForRegion(HRegion region) {
    Preconditions.checkState(
        this.region == null,
        "Policy already configured for region {}",
        this.region);

    this.region = region;
  }

  /**
   * @return true if the specified region should be split.
   */
  protected abstract boolean shouldSplit();

  /**
   * @return the key at which the region should be split, or null
   * if it cannot be split. This will only be called if shouldSplit
   * previously returned true.
   */
  protected byte[] getSplitPoint() {
    byte[] explicitSplitPoint = this.region.getExplicitSplitPoint();
    if (explicitSplitPoint != null) {
      return explicitSplitPoint;
    }
    Map<byte[], Store> stores = region.getStores();

    byte[] splitPointFromLargestStore = null;
    long largestStoreSize = 0;
    for (Store s : stores.values()) {
      byte[] splitPoint = s.getSplitPoint();
      long storeSize = s.getSize();
      if (splitPoint != null && largestStoreSize < storeSize) {
        splitPointFromLargestStore = splitPoint;
        largestStoreSize = storeSize;
      }
    }

    return splitPointFromLargestStore;
  }

  /**
   * Create the RegionSplitPolicy configured for the given table.
   * @param region
   * @param conf
   * @return a RegionSplitPolicy
   * @throws IOException
   */
  public static RegionSplitPolicy create(HRegion region,
      Configuration conf) throws IOException {
    Class<? extends RegionSplitPolicy> clazz = getSplitPolicyClass(
        region.getTableDesc(), conf);
    RegionSplitPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForRegion(region);
    return policy;
  }

  public static Class<? extends RegionSplitPolicy> getSplitPolicyClass(
      HTableDescriptor htd, Configuration conf) throws IOException {
    String className = htd.getRegionSplitPolicyClassName();
    if (className == null) {
      className = conf.get(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DEFAULT_SPLIT_POLICY_CLASS.getName());
    }

    try {
      Class<? extends RegionSplitPolicy> clazz =
        Class.forName(className).asSubclass(RegionSplitPolicy.class);
      return clazz;
    } catch (Exception  e) {
      throw new IOException(
          "Unable to load configured region split policy '" +
          className + "' for table '" + htd.getTableName() + "'",
          e);
    }
  }

  /**
   * In {@link HRegionFileSystem#splitStoreFile(org.apache.hadoop.hbase.HRegionInfo, String,
   * StoreFile, byte[], boolean, RegionSplitPolicy)} we are not creating the split reference
   * if split row not lies in the StoreFile range. But in some use cases we may need to create
   * the split reference even when the split row not lies in the range. This method can be used
   * to decide, whether to skip the the StoreFile range check or not.
   * @return whether to skip the StoreFile range check or not
   */
  protected boolean skipStoreFileRangeCheck() {
    return false;
  }
}
