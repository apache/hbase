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

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RegionPlanConditional {
  public RegionPlanConditional(Configuration conf, BalancerClusterState cluster) {
  }

  /**
   * Get the candidate generator for this conditional. This can be useful to provide the balancer
   * with hints that will appease your conditional.
   * @return the candidate generator for this conditional
   */
  abstract Optional<RegionPlanConditionalCandidateGenerator> getCandidateGenerator();

  /**
   * Check if the conditional is violated by the given region plan.
   * @param regionPlan the region plan to check
   * @return true if the conditional is violated
   */
  abstract boolean isViolating(RegionPlan regionPlan);
}
