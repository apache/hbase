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
package org.apache.hadoop.hbase.master.balancer;

import java.util.Collection;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class to be used for the subset of RegionLoad costs that should be treated as rates. We do not
 * compare about the actual rate in requests per second but rather the rate relative to the rest of
 * the regions.
 */
@InterfaceAudience.Private
abstract class CostFromRegionLoadAsRateFunction extends CostFromRegionLoadFunction {

  @Override
  protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
    double cost = 0;
    double previous = 0;
    boolean isFirst = true;
    for (BalancerRegionLoad rl : regionLoadList) {
      double current = getCostFromRl(rl);
      if (isFirst) {
        isFirst = false;
      } else {
        cost += current - previous;
      }
      previous = current;
    }
    return Math.max(0, cost / (regionLoadList.size() - 1));
  }
}