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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RegionSplitRestriction} implementation that groups rows by a prefix of the row-key.
 * <p>
 * This ensures that a region is not split "inside" a prefix of a row key.
 * I.e. rows can be co-located in a region by their prefix.
 */
@InterfaceAudience.Private
public class KeyPrefixRegionSplitRestriction extends RegionSplitRestriction {
  private static final Logger LOG =
    LoggerFactory.getLogger(KeyPrefixRegionSplitRestriction.class);

  public static final String PREFIX_LENGTH_KEY =
    "hbase.regionserver.region.split_restriction.prefix_length";

  private int prefixLength;

  @Override
  public void initialize(TableDescriptor tableDescriptor, Configuration conf) throws IOException {
    String prefixLengthString = tableDescriptor.getValue(PREFIX_LENGTH_KEY);
    if (prefixLengthString == null) {
      prefixLengthString = conf.get(PREFIX_LENGTH_KEY);
      if (prefixLengthString == null) {
        LOG.error("{} not specified for table {}. "
          + "Using the default RegionSplitRestriction", PREFIX_LENGTH_KEY,
          tableDescriptor.getTableName());
        return;
      }
    }
    try {
      prefixLength = Integer.parseInt(prefixLengthString);
    } catch (NumberFormatException ignored) {
    }
    if (prefixLength <= 0) {
      LOG.error("Invalid value for {} for table {}:{}. "
        + "Using the default RegionSplitRestriction", PREFIX_LENGTH_KEY,
        tableDescriptor.getTableName(), prefixLengthString);
    }
  }

  @Override
  public byte[] getRestrictedSplitPoint(byte[] splitPoint) {
    if (prefixLength > 0) {
      // group split keys by a prefix
      return Arrays.copyOf(splitPoint, Math.min(prefixLength, splitPoint.length));
    } else {
      return splitPoint;
    }
  }
}
