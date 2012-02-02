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

/**
 * A custom RegionSplitPolicy for testing.
 * This class also demonstrates how to implement a SplitPolicy that groups
 * rows by a prefix of the row-key
 *
 * This ensures that a region is not split "inside"
 * a prefix of a row key. I.e. rows can be co-located by
 * their prefix.
 */
public class PrefixSplitKeyPolicy extends ConstantSizeRegionSplitPolicy {
  public static String PREFIX_LENGTH_KEY = "prefix_split_key_policy.prefix_length";

  private int prefix_length;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);

    if (region != null) {
      // this demonstrates how a RegionSplitPolicy can be configured
      // through HTableDescriptor values
      prefix_length = Integer.parseInt(region.getTableDesc().getValue(
          PREFIX_LENGTH_KEY));
    }
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = super.getSplitPoint();
    if (splitPoint != null && splitPoint.length > 0) {
      // group split keys by a prefix
      return Arrays.copyOf(splitPoint,
          Math.min(prefix_length, splitPoint.length));
    } else {
      return splitPoint;
    }
  }
}
