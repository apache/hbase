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

import com.google.common.annotations.VisibleForTesting;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link RegionSplitPolicy} implementation which splits a region
 * as soon as any of its store files exceeds a maximum configurable
 * size.
 * <p>
 * This is the default split policy. From 0.94.0 on the default split policy has
 * changed to {@link IncreasingToUpperBoundRegionSplitPolicy}
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ConstantSizeRegionSplitPolicy extends RegionSplitPolicy {
  private static final Random RANDOM = new Random();
  private static final double EPSILON = 1E-6;

  private long desiredMaxFileSize;
  private double jitterRate;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    HTableDescriptor desc = region.getTableDesc();
    if (desc != null) {
      this.desiredMaxFileSize = desc.getMaxFileSize();
    }
    if (this.desiredMaxFileSize <= 0) {
      this.desiredMaxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    double jitter = conf.getDouble("hbase.hregion.max.filesize.jitter", 0.25D);
    this.jitterRate = (RANDOM.nextFloat() - 0.5D) * jitter;
    long jitterValue = (long) (this.desiredMaxFileSize * this.jitterRate);
    // make sure the long value won't overflow with jitter
    if (this.jitterRate > EPSILON && jitterValue > (Long.MAX_VALUE - this.desiredMaxFileSize)) {
      this.desiredMaxFileSize = Long.MAX_VALUE;
    } else {
      this.desiredMaxFileSize += jitterValue;
    }
  }

  @Override
  protected boolean shouldSplit() {
    boolean force = region.shouldForceSplit();
    boolean foundABigStore = false;

    for (Store store : region.getStores()) {
      // If any of the stores are unable to split (eg they contain reference files)
      // then don't split
      if ((!store.canSplit())) {
        return false;
      }

      // Mark if any store is big enough
      if (store.getSize() > desiredMaxFileSize) {
        foundABigStore = true;
      }
    }

    return foundABigStore || force;
  }

  long getDesiredMaxFileSize() {
    return desiredMaxFileSize;
  }

  @VisibleForTesting
  public boolean positiveJitterRate() {
    return this.jitterRate > EPSILON;
  }
}
