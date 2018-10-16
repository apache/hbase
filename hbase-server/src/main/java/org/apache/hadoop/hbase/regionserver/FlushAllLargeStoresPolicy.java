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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FlushPolicy} that only flushes store larger a given threshold. If no store is large
 * enough, then all stores will be flushed.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FlushAllLargeStoresPolicy extends FlushLargeStoresPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(FlushAllLargeStoresPolicy.class);

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    int familyNumber = region.getTableDescriptor().getColumnFamilyCount();
    if (familyNumber <= 1) {
      // No need to parse and set flush size lower bound if only one family
      // Family number might also be zero in some of our unit test case
      return;
    }
    setFlushSizeLowerBounds(region);
  }

  @Override
  public Collection<HStore> selectStoresToFlush() {
    // no need to select stores if only one family
    if (region.getTableDescriptor().getColumnFamilyCount() == 1) {
      return region.stores.values();
    }
    // start selection
    Collection<HStore> stores = region.stores.values();
    Set<HStore> specificStoresToFlush = new HashSet<>();
    for (HStore store : stores) {
      if (shouldFlush(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if (!specificStoresToFlush.isEmpty()) {
      return specificStoresToFlush;
    }

    // Didn't find any CFs which were above the threshold for selection.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Since none of the CFs were above the size, flushing all.");
    }
    return stores;
  }

  @Override
  protected boolean shouldFlush(HStore store) {
    return super.shouldFlush(store) || region.shouldFlushStore(store);
  }

}
