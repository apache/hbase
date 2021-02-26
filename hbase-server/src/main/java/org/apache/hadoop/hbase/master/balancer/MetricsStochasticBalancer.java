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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This metrics balancer uses extended source for stochastic load balancer
 * to report its related metrics to JMX. For details, refer to HBASE-13965
 */
@InterfaceAudience.Private
public class MetricsStochasticBalancer extends MetricsBalancer {
  /**
   * Use the stochastic source instead of the default source.
   */
  private MetricsStochasticBalancerSource stochasticSource = null;

  public MetricsStochasticBalancer() {
    initSource();
  }

  /**
   * This function overrides the initSource in the MetricsBalancer, use
   * MetricsStochasticBalancerSource instead of the MetricsBalancerSource.
   */
  @Override
  protected void initSource() {
    stochasticSource =
        CompatibilitySingletonFactory.getInstance(MetricsStochasticBalancerSource.class);
  }

  @Override
  public void balanceCluster(long time) {
    stochasticSource.updateBalanceCluster(time);
  }

  @Override
  public void incrMiscInvocations() {
    stochasticSource.incrMiscInvocations();
  }

  /**
   * Updates the balancer status tag reported to JMX
   */
  @Override
  public void balancerStatus(boolean status) {
    stochasticSource.updateBalancerStatus(status);
  }

  /**
   * Updates the number of metrics reported to JMX
   */
  public void updateMetricsSize(int size) {
    stochasticSource.updateMetricsSize(size);
  }

  /**
   * Reports stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    stochasticSource.updateStochasticCost(tableName, costFunctionName, costFunctionDesc, value);
  }
}
