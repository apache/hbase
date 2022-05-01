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
import org.apache.hadoop.hbase.master.balancer.BalancerClusterState.LocalityType;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class ServerLocalityCostFunction extends LocalityBasedCostFunction {

  private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
  private static final float DEFAULT_LOCALITY_COST = 25;

  ServerLocalityCostFunction(Configuration conf) {
    super(conf, LocalityType.SERVER, LOCALITY_COST_KEY, DEFAULT_LOCALITY_COST);
  }

  @Override
  int regionIndexToEntityIndex(int region) {
    return cluster.regionIndexToServerIndex[region];
  }
}
