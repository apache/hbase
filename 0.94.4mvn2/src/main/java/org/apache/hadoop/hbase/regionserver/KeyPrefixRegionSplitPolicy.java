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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups
 * rows by a prefix of the row-key
 *
 * This ensures that a region is not split "inside" a prefix of a row key.
 * I.e. rows can be co-located in a regionb by their prefix.
 */
public class KeyPrefixRegionSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {
  private static final Log LOG = LogFactory
      .getLog(KeyPrefixRegionSplitPolicy.class);
  public static String PREFIX_LENGTH_KEY = "prefix_split_key_policy.prefix_length";

  private int prefixLength = 0;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    if (region != null) {
      prefixLength = 0;

      // read the prefix length from the table descriptor
      String prefixLengthString = region.getTableDesc().getValue(
          PREFIX_LENGTH_KEY);
      if (prefixLengthString == null) {
        LOG.error(PREFIX_LENGTH_KEY + " not specified for table "
            + region.getTableDesc().getNameAsString()
            + ". Using default RegionSplitPolicy");
        return;
      }
      try {
        prefixLength = Integer.parseInt(prefixLengthString);
      } catch (NumberFormatException nfe) {
        // ignore
      }
      if (prefixLength <= 0) {
        LOG.error("Invalid value for " + PREFIX_LENGTH_KEY + " for table "
            + region.getTableDesc().getNameAsString() + ":"
            + prefixLengthString + ". Using default RegionSplitPolicy");
      }
    }
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = super.getSplitPoint();
    if (prefixLength > 0 && splitPoint != null && splitPoint.length > 0) {
      // group split keys by a prefix
      return Arrays.copyOf(splitPoint,
          Math.min(prefixLength, splitPoint.length));
    } else {
      return splitPoint;
    }
  }
}