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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

@InterfaceAudience.Private
public class MetricsStochasticBalancerSourceImpl extends MetricsBalancerSourceImpl implements
    MetricsStochasticBalancerSource {
  private static final String TABLE_FUNCTION_SEP = "_";

  // Most Recently Used(MRU) cache
  private static final float MRU_LOAD_FACTOR = 0.75f;
  private int metricsSize = 1000;
  private int mruCap = calcMruCap(metricsSize);

  private Map<String, Map<String, Double>> stochasticCosts =
      new LinkedHashMap<String, Map<String, Double>>(mruCap, MRU_LOAD_FACTOR, true) {
        private static final long serialVersionUID = 8204713453436906599L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Map<String, Double>> eldest) {
          return size() > mruCap;
        }
      };
  private Map<String, String> costFunctionDescs = new ConcurrentHashMap<String, String>();

  /**
   * Calculates the mru cache capacity from the metrics size
   */
  private static int calcMruCap(int metricsSize) {
    return (int) Math.ceil(metricsSize / MRU_LOAD_FACTOR) + 1;
  }

  @Override
  public void updateMetricsSize(int size) {
    if (size > 0) {
      metricsSize = size;
      mruCap = calcMruCap(size);
    }
  }

  /**
   * Reports stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName, String functionDesc,
      Double cost) {
    if (tableName == null || costFunctionName == null || cost == null) {
      return;
    }

    if (functionDesc != null) {
      costFunctionDescs.put(costFunctionName, functionDesc);
    }

    synchronized (stochasticCosts) {
      Map<String, Double> costs = stochasticCosts.get(tableName);
      if (costs == null) {
        costs = new ConcurrentHashMap<String, Double>();
      }

      costs.put(costFunctionName, cost);
      stochasticCosts.put(tableName, costs);
    }
  }

  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsBuilder.addRecord(metricsName);

    if (stochasticCosts != null) {
      synchronized (stochasticCosts) {
        for (Map.Entry<String, Map<String, Double>> tableEntry : stochasticCosts.entrySet()) {
          for (Map.Entry<String, Double> costEntry : tableEntry.getValue().entrySet()) {
            String attrName = tableEntry.getKey() + TABLE_FUNCTION_SEP + costEntry.getKey();
            Double cost = costEntry.getValue();
            String functionDesc = costFunctionDescs.get(costEntry.getKey());
            if (functionDesc == null) functionDesc = costEntry.getKey();
            metricsRecordBuilder.addGauge(attrName, functionDesc, cost);
          }
        }
      }
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }

}
