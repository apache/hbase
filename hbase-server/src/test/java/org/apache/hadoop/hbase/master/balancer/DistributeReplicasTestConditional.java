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

public class DistributeReplicasTestConditional extends DistributeReplicasConditional {

  static void enableConditionalReplicaDistributionForTest(Configuration conf) {
    conf.set(BalancerConditionals.ADDITIONAL_CONDITIONALS_KEY,
      DistributeReplicasTestConditional.class.getCanonicalName());
  }

  public DistributeReplicasTestConditional(BalancerConditionals balancerConditionals,
    BalancerClusterState cluster) {
    super(balancerConditionals, cluster);
  }

  @Override
  public ValidationLevel getValidationLevel() {
    // Mini-cluster tests can't validate at host/rack levels
    return ValidationLevel.SERVER;
  }
}
