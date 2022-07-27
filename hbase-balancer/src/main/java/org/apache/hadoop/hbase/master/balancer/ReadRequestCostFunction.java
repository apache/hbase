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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute the cost of total number of read requests The more unbalanced the higher the computed
 * cost will be. This uses a rolling average of regionload.
 */
@InterfaceAudience.Private
class ReadRequestCostFunction extends CostFromRegionLoadAsRateFunction {

  private static final String READ_REQUEST_COST_KEY =
    "hbase.master.balancer.stochastic.readRequestCost";
  private static final float DEFAULT_READ_REQUEST_COST = 5;

  ReadRequestCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(READ_REQUEST_COST_KEY, DEFAULT_READ_REQUEST_COST));
  }

  @Override
  protected double getCostFromRl(BalancerRegionLoad rl) {
    return rl.getReadRequestsCount();
  }
}
