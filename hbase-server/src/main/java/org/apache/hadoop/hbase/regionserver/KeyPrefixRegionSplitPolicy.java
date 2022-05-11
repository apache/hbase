/*
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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups rows by a prefix of the row-key
 * This ensures that a region is not split "inside" a prefix of a row key. I.e. rows can be
 * co-located in a region by their prefix.
 * @deprecated since 2.4.3 and will be removed in 4.0.0. Use {@link RegionSplitRestriction},
 *             instead.
 */
@Deprecated
@InterfaceAudience.Private
public class KeyPrefixRegionSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(KeyPrefixRegionSplitPolicy.class);
  @Deprecated
  public static final String PREFIX_LENGTH_KEY_DEPRECATED = "prefix_split_key_policy.prefix_length";
  public static final String PREFIX_LENGTH_KEY = "KeyPrefixRegionSplitPolicy.prefix_length";

  private int prefixLength = 0;

  @Override
  public String toString() {
    return "KeyPrefixRegionSplitPolicy{" + "prefixLength=" + prefixLength + ", " + super.toString()
      + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    prefixLength = 0;

    // read the prefix length from the table descriptor
    String prefixLengthString = region.getTableDescriptor().getValue(PREFIX_LENGTH_KEY);
    if (prefixLengthString == null) {
      // read the deprecated value
      prefixLengthString = region.getTableDescriptor().getValue(PREFIX_LENGTH_KEY_DEPRECATED);
      if (prefixLengthString == null) {
        LOG.error(PREFIX_LENGTH_KEY + " not specified for table "
          + region.getTableDescriptor().getTableName() + ". Using default RegionSplitPolicy");
        return;
      }
    }
    try {
      prefixLength = Integer.parseInt(prefixLengthString);
    } catch (NumberFormatException nfe) {
      /* Differentiate NumberFormatException from an invalid value range reported below. */
      LOG.error("Number format exception when parsing " + PREFIX_LENGTH_KEY + " for table "
        + region.getTableDescriptor().getTableName() + ":" + prefixLengthString + ". " + nfe);
      return;
    }
    if (prefixLength <= 0) {
      LOG.error("Invalid value for " + PREFIX_LENGTH_KEY + " for table "
        + region.getTableDescriptor().getTableName() + ":" + prefixLengthString
        + ". Using default RegionSplitPolicy");
    }
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = super.getSplitPoint();
    if (prefixLength > 0 && splitPoint != null && splitPoint.length > 0) {
      // group split keys by a prefix
      return Arrays.copyOf(splitPoint, Math.min(prefixLength, splitPoint.length));
    } else {
      return splitPoint;
    }
  }
}
