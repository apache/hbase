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

import org.junit.After;
import org.junit.Before;

public class BalancerTestBase2 extends BalancerTestBase {

  @Before
  public void before() {
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 3 * 60 * 1000);
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    loadBalancer.setConf(conf);
  }

  @After
  public void after() {
    // reset config to make sure balancer run
    loadBalancer.setConf(conf);
  }
}
