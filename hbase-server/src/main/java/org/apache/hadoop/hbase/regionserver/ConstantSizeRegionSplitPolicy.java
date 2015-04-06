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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.Random;

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

  private long desiredMaxFileSize;

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
    this.desiredMaxFileSize += (long)(desiredMaxFileSize * (RANDOM.nextFloat() - 0.5D) * jitter);
  }

  @Override
  protected boolean shouldSplit() {
    boolean force = region.shouldForceSplit();
    boolean foundABigStore = false;

    for (Store store : region.getStores().values()) {
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
}
