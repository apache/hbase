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

package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.RegionLoad;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Wrapper class for the few fields required by the {@link StochasticLoadBalancer}
 * from the full {@link RegionLoad}.
 */
@InterfaceStability.Evolving
class BalancerRegionLoad {
  private final long readRequestsCount;
  private final long writeRequestsCount;
  private final int memStoreSizeMB;
  private final int storefileSizeMB;

  BalancerRegionLoad(RegionLoad regionLoad) {
    readRequestsCount = regionLoad.getReadRequestsCount();
    writeRequestsCount = regionLoad.getWriteRequestsCount();
    memStoreSizeMB = regionLoad.getMemStoreSizeMB();
    storefileSizeMB = regionLoad.getStorefileSizeMB();
  }

  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  public long getWriteRequestsCount() {
    return writeRequestsCount;
  }

  public int getMemStoreSizeMB() {
    return memStoreSizeMB;
  }

  public int getStorefileSizeMB() {
    return storefileSizeMB;
  }
}
