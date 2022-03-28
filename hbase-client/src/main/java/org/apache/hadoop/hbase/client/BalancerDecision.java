/*
 *
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

package org.apache.hadoop.hbase.client;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;

/**
 * History of balancer decisions taken for region movements.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
final public class BalancerDecision extends LogEntry {

  private final String initialFunctionCosts;
  private final String finalFunctionCosts;
  private final double initTotalCost;
  private final double computedTotalCost;
  private final long computedSteps;
  private final List<String> regionPlans;

  // used to convert object to pretty printed format
  // used by toJsonPrettyPrint()
  private static final Gson GSON = GsonUtil.createGson()
    .setPrettyPrinting()
    .registerTypeAdapter(BalancerDecision.class, (JsonSerializer<BalancerDecision>)
      (balancerDecision, type, jsonSerializationContext) -> {
        Gson gson = new Gson();
        return gson.toJsonTree(balancerDecision);
      }).create();

  private BalancerDecision(String initialFunctionCosts, String finalFunctionCosts,
      double initTotalCost, double computedTotalCost, List<String> regionPlans,
      long computedSteps) {
    this.initialFunctionCosts = initialFunctionCosts;
    this.finalFunctionCosts = finalFunctionCosts;
    this.initTotalCost = initTotalCost;
    this.computedTotalCost = computedTotalCost;
    this.regionPlans = regionPlans;
    this.computedSteps = computedSteps;
  }

  public String getInitialFunctionCosts() {
    return initialFunctionCosts;
  }

  public String getFinalFunctionCosts() {
    return finalFunctionCosts;
  }

  public double getInitTotalCost() {
    return initTotalCost;
  }

  public double getComputedTotalCost() {
    return computedTotalCost;
  }

  public List<String> getRegionPlans() {
    return regionPlans;
  }

  public long getComputedSteps() {
    return computedSteps;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("initialFunctionCosts", initialFunctionCosts)
      .append("finalFunctionCosts", finalFunctionCosts)
      .append("initTotalCost", initTotalCost)
      .append("computedTotalCost", computedTotalCost)
      .append("computedSteps", computedSteps)
      .append("regionPlans", regionPlans)
      .toString();
  }

  @Override
  public String toJsonPrettyPrint() {
    return GSON.toJson(this);
  }

  public static class Builder {
    private String initialFunctionCosts;
    private String finalFunctionCosts;
    private double initTotalCost;
    private double computedTotalCost;
    private long computedSteps;
    private List<String> regionPlans;

    public Builder setInitialFunctionCosts(String initialFunctionCosts) {
      this.initialFunctionCosts = initialFunctionCosts;
      return this;
    }

    public Builder setFinalFunctionCosts(String finalFunctionCosts) {
      this.finalFunctionCosts = finalFunctionCosts;
      return this;
    }

    public Builder setInitTotalCost(double initTotalCost) {
      this.initTotalCost = initTotalCost;
      return this;
    }

    public Builder setComputedTotalCost(double computedTotalCost) {
      this.computedTotalCost = computedTotalCost;
      return this;
    }

    public Builder setRegionPlans(List<String> regionPlans) {
      this.regionPlans = regionPlans;
      return this;
    }

    public Builder setComputedSteps(long computedSteps) {
      this.computedSteps = computedSteps;
      return this;
    }

    public BalancerDecision build() {
      return new BalancerDecision(initialFunctionCosts, finalFunctionCosts,
        initTotalCost, computedTotalCost, regionPlans, computedSteps);
    }
  }

}
