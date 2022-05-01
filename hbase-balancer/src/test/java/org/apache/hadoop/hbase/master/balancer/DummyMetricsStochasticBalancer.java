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

import java.util.HashMap;
import java.util.Map;

public class DummyMetricsStochasticBalancer extends MetricsStochasticBalancer {
  // We use a map to record those metrics that were updated to MetricsStochasticBalancer when
  // running
  // unit tests.
  private Map<String, Double> costsMap;

  public DummyMetricsStochasticBalancer() {
    // noop
  }

  @Override
  protected void initSource() {
    costsMap = new HashMap<>();
  }

  @Override
  public void balanceCluster(long time) {
    // noop
  }

  @Override
  public void incrMiscInvocations() {
    // noop
  }

  @Override
  public void balancerStatus(boolean status) {
    // noop
  }

  @Override
  public void updateMetricsSize(int size) {
    // noop
  }

  @Override
  public void updateStochasticCost(String tableName, String costFunctionName,
    String costFunctionDesc, Double value) {
    String key = tableName + "#" + costFunctionName;
    costsMap.put(key, value);
  }

  public Map<String, Double> getDummyCostsMap() {
    return this.costsMap;
  }

  /**
   * Clear all metrics in the cache map then prepare to run the next test
   */
  public void clearDummyMetrics() {
    this.costsMap.clear();
  }

}
