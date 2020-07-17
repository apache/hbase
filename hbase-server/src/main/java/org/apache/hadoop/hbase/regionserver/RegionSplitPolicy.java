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
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;


/**
 * A split policy determines when a Region should be split.
 *
 * @see SteppingSplitPolicy Default split policy since 2.0.0
 * @see IncreasingToUpperBoundRegionSplitPolicy Default split policy since
 *      0.94.0
 * @see ConstantSizeRegionSplitPolicy Default split policy before 0.94.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class RegionSplitPolicy extends Configured {
  private static final Class<? extends RegionSplitPolicy>
    DEFAULT_SPLIT_POLICY_CLASS = SteppingSplitPolicy.class;

  /**
   * The region configured for this split policy.
   * As of hbase-2.0.0, RegionSplitPolicy can be instantiated on the Master-side so the
   * Phoenix local-indexer can block default hbase behavior. This is an exotic usage. Should not
   * trouble any other users of RegionSplitPolicy.
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
   * @return {@code true} if the specified region can be split.
   */
  protected boolean canSplit() {
    return !region.getRegionInfo().isMetaRegion() && region.isAvailable() &&
      !TableName.NAMESPACE_TABLE_NAME.equals(region.getRegionInfo().getTable()) &&
      region.getStores().stream().allMatch(HStore::canSplit);
  }

  /**
   * @return the key at which the region should be split, or null
   * if it cannot be split. This will only be called if shouldSplit
   * previously returned true.
   */
  protected byte[] getSplitPoint() {
    List<HStore> stores = region.getStores();

    byte[] splitPointFromLargestStore = null;
    long largestStoreSize = 0;
    for (HStore s : stores) {
      Optional<byte[]> splitPoint = s.getSplitPoint();
      // Store also returns null if it has references as way of indicating it is not splittable
      long storeSize = s.getSize();
      if (splitPoint.isPresent() && largestStoreSize < storeSize) {
        splitPointFromLargestStore = splitPoint.get();
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
    Preconditions.checkNotNull(region, "Region should not be null.");
    Class<? extends RegionSplitPolicy> clazz = getSplitPolicyClass(
        region.getTableDescriptor(), conf);
    RegionSplitPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForRegion(region);
    return policy;
  }

  public static Class<? extends RegionSplitPolicy> getSplitPolicyClass(
      TableDescriptor htd, Configuration conf) throws IOException {
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
   * In {@link HRegionFileSystem#splitStoreFile(org.apache.hadoop.hbase.client.RegionInfo, String,
   * HStoreFile, byte[], boolean, RegionSplitPolicy)} we are not creating the split reference
   * if split row does not lie inside the StoreFile range. But in some use cases we may need to
   * create the split reference even when the split row does not lie inside the StoreFile range.
   * This method can be used to decide, whether to skip the the StoreFile range check or not.
   *
   * <p>This method is not for general use. It is a mechanism put in place by Phoenix
   * local indexing to defeat standard hbase behaviors. Phoenix local indices are very likely
   * the only folks who would make use of this method. On the Master-side, we will instantiate
   * a RegionSplitPolicy instance and run this method ONLY... none of the others make sense
   * on the Master-side.</p>
   *
   * TODO: Shutdown this phoenix specialization or do it via some other means.
   * @return whether to skip the StoreFile range check or not
   */
  protected boolean skipStoreFileRangeCheck(String familyName) {
    return false;
  }
}
