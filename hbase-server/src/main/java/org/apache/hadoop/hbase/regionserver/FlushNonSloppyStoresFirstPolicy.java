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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link FlushPolicy} that only flushes store larger than a given threshold. If no store is large
 * enough, then all stores will be flushed.
 * Gives priority to selecting regular stores first, and only if no other
 * option, selects sloppy stores which normaly require more memory.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FlushNonSloppyStoresFirstPolicy extends FlushLargeStoresPolicy {

  private Collection<Store> regularStores = new HashSet<>();
  private Collection<Store> sloppyStores = new HashSet<>();

  /**
   * @return the stores need to be flushed.
   */
  @Override public Collection<Store> selectStoresToFlush() {
    Collection<Store> specificStoresToFlush = new HashSet<Store>();
    for(Store store : regularStores) {
      if(shouldFlush(store) || region.shouldFlushStore(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if(!specificStoresToFlush.isEmpty()) return specificStoresToFlush;
    for(Store store : sloppyStores) {
      if(shouldFlush(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if(!specificStoresToFlush.isEmpty()) return specificStoresToFlush;
    return region.stores.values();
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    this.flushSizeLowerBound = getFlushSizeLowerBound(region);
    for(Store store : region.stores.values()) {
      if(store.getMemStore().isSloppy()) {
        sloppyStores.add(store);
      } else {
        regularStores.add(store);
      }
    }
  }
}
