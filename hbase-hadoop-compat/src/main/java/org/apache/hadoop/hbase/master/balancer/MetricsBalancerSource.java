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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsBalancerSource extends BaseSource  {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Balancer";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  String BALANCE_CLUSTER = "balancerCluster";
  String MISC_INVOATION_COUNT = "miscInvocationCount";
  String BALANCER_STATUS = "isBalancerActive";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master balancer";

  void updateBalanceCluster(long time);

  void incrMiscInvocations();

  void updateBalancerStatus(boolean status);
}
