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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG =
    LoggerFactory.getLogger(ConstantSizeRegionSplitPolicy.class);
  private long desiredMaxFileSize;
  private double jitterRate;
  protected boolean overallHRegionFiles;

  @Override
  public String toString() {
    return "ConstantSizeRegionSplitPolicy{" + "desiredMaxFileSize=" + desiredMaxFileSize
      + ", jitterRate=" + jitterRate + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    TableDescriptor desc = region.getTableDescriptor();
    if (desc != null) {
      this.desiredMaxFileSize = desc.getMaxFileSize();
    }
    if (this.desiredMaxFileSize <= 0) {
      this.desiredMaxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.overallHRegionFiles = conf.getBoolean(HConstants.OVERALL_HREGION_FILES,
      HConstants.DEFAULT_OVERALL_HREGION_FILES);
    double jitter = conf.getDouble("hbase.hregion.max.filesize.jitter", 0.25D);
    this.jitterRate = (ThreadLocalRandom.current().nextFloat() - 0.5D) * jitter;
    long jitterValue = (long) (this.desiredMaxFileSize * this.jitterRate);
    // Default jitter is ~12% +/-. Make sure the long value won't overflow with jitter
    if (this.jitterRate > 0 && jitterValue > (Long.MAX_VALUE - this.desiredMaxFileSize)) {
      this.desiredMaxFileSize = Long.MAX_VALUE;
    } else {
      this.desiredMaxFileSize += jitterValue;
    }
  }

  @Override
  protected boolean shouldSplit() {
    if (!canSplit()) {
      return false;
    }
    return isExceedSize(desiredMaxFileSize);
  }

  long getDesiredMaxFileSize() {
    return desiredMaxFileSize;
  }

  @InterfaceAudience.Private
  public boolean positiveJitterRate() {
    return this.jitterRate > 0;
  }

  /**
   * @return true if region size exceed the sizeToCheck
   */
  protected final boolean isExceedSize(long sizeToCheck) {
    if (overallHRegionFiles) {
      long sumSize = 0;
      for (HStore store : region.getStores()) {
        sumSize += store.getSize();
      }
      if (sumSize > sizeToCheck) {
        LOG.debug("ShouldSplit because region size is big enough "
            + "sumSize={}, sizeToCheck={}", StringUtils.humanSize(sumSize),
          StringUtils.humanSize(sizeToCheck));
        return true;
      }
    } else {
      for (HStore store : region.getStores()) {
        long size = store.getSize();
        if (size > sizeToCheck) {
          LOG.debug("ShouldSplit because {} size={}, sizeToCheck={}{}",
            store.getColumnFamilyName(), StringUtils.humanSize(size),
            StringUtils.humanSize(sizeToCheck));
          return true;
        }
      }
    }
    return false;
  }
}
