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
import java.util.Iterator;
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
    Iterator<BalancerRegionLoad> iter = regionLoadList.iterator();
    if (!iter.hasNext()) {
      return 0;
    }
    double previous = getCostFromRl(iter.next());
    if (!iter.hasNext()) {
      return 0;
    }
    double cost = 0;
    do {
      double current = getCostFromRl(iter.next());
      cost += current >= previous ? current - previous : current;
      previous = current;
    } while (iter.hasNext());
    return Math.max(0, cost / (regionLoadList.size() - 1));
  }
}
