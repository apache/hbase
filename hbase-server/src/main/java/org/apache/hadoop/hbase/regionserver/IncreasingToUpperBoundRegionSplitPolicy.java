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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * Split size is the number of regions that are on this server that all are
 * of the same table, cubed, times 2x the region flush size OR the maximum
 * region split size, whichever is smaller.
 * <p>
 * For example, if the flush size is 128MB, then after two flushes (256MB) we
 * will split which will make two regions that will split when their size is
 * {@code 2^3 * 128MB*2 = 2048MB}.
 * <p>
 * If one of these regions splits, then there are three regions and now the
 * split size is {@code 3^3 * 128MB*2 = 6912MB}, and so on until we reach the configured
 * maximum file size and then from there on out, we'll use that.
 */
@InterfaceAudience.Private
public class IncreasingToUpperBoundRegionSplitPolicy extends ConstantSizeRegionSplitPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(IncreasingToUpperBoundRegionSplitPolicy.class);

  protected long initialSize;

  @Override
  public String toString() {
    return "IncreasingToUpperBoundRegionSplitPolicy{" + "initialSize=" + initialSize +
      ", " + super.toString() + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    initialSize = conf.getLong("hbase.increasing.policy.initial.size", -1);
    if (initialSize > 0) {
      return;
    }
    TableDescriptor desc = region.getTableDescriptor();
    if (desc != null) {
      initialSize = 2 * desc.getMemStoreFlushSize();
    }
    if (initialSize <= 0) {
      initialSize = 2 * conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                                     TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
  }

  @Override
  protected boolean shouldSplit() {
    if (!canSplit()) {
      return false;
    }
    // Get count of regions that have the same common table as this.region
    int tableRegionsCount = getCountOfCommonTableRegions();
    // Get size to check
    long sizeToCheck = getSizeToCheck(tableRegionsCount);
    boolean shouldSplit = isExceedSize(sizeToCheck);
    if (shouldSplit) {
      LOG.debug("regionsWithCommonTable={}", tableRegionsCount);
    }
    return shouldSplit;
  }

  /**
   * @return Count of regions on this server that share the table this.region
   * belongs to
   */
  private int getCountOfCommonTableRegions() {
    RegionServerServices rss = region.getRegionServerServices();
    // Can be null in tests
    if (rss == null) {
      return 0;
    }
    TableName tablename = region.getTableDescriptor().getTableName();
    int tableRegionsCount = 0;
    try {
      List<? extends Region> hri = rss.getRegions(tablename);
      tableRegionsCount = hri == null || hri.isEmpty() ? 0 : hri.size();
    } catch (IOException e) {
      LOG.debug("Failed getOnlineRegions " + tablename, e);
    }
    return tableRegionsCount;
  }

  /**
   * @return Region max size or {@code count of regions cubed * 2 * flushsize},
   * which ever is smaller; guard against there being zero regions on this server.
   */
  protected long getSizeToCheck(final int tableRegionsCount) {
    // safety check for 100 to avoid numerical overflow in extreme cases
    return tableRegionsCount == 0 || tableRegionsCount > 100
               ? getDesiredMaxFileSize()
               : Math.min(getDesiredMaxFileSize(),
                          initialSize * tableRegionsCount * tableRegionsCount * tableRegionsCount);
  }

}
