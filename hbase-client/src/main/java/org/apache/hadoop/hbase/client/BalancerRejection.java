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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;

/**
 * History of detail information that balancer movements was rejected
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
final public class BalancerRejection extends LogEntry {
  //The reason why balancer was rejected
  private final String reason;
  private final List<String> costFuncInfoList;

  // used to convert object to pretty printed format
  // used by toJsonPrettyPrint()
  private static final Gson GSON = GsonUtil.createGson()
    .setPrettyPrinting()
    .disableHtmlEscaping()
    .registerTypeAdapter(BalancerRejection.class, (JsonSerializer<BalancerRejection>)
      (balancerRejection, type, jsonSerializationContext) -> {
        Gson gson = new Gson();
        return gson.toJsonTree(balancerRejection);
      }).create();

  private BalancerRejection(String reason, List<String> costFuncInfoList) {
    this.reason = reason;
    if(costFuncInfoList == null){
      this.costFuncInfoList = Collections.emptyList();
    }
    else {
      this.costFuncInfoList = costFuncInfoList;
    }
  }

  public String getReason() {
    return reason;
  }

  public List<String> getCostFuncInfoList() {
    return costFuncInfoList;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("reason", reason)
      .append("costFuncInfoList", costFuncInfoList.toString())
      .toString();
  }

  @Override
  public String toJsonPrettyPrint() {
    return GSON.toJson(this);
  }

  public static class Builder {
    private String reason;
    private List<String> costFuncInfoList;

    public Builder setReason(String reason) {
      this.reason = reason;
      return this;
    }

    public void addCostFuncInfo(String funcName, double cost, float multiplier){
      if(costFuncInfoList == null){
        costFuncInfoList = new ArrayList<>();
      }
      costFuncInfoList.add(
        new StringBuilder()
          .append(funcName)
          .append(" cost:").append(cost)
          .append(" multiplier:").append(multiplier)
          .toString());
    }

    public Builder setCostFuncInfoList(List<String> costFuncInfoList){
      this.costFuncInfoList = costFuncInfoList;
      return this;
    }

    public BalancerRejection build() {
      return new BalancerRejection(reason, costFuncInfoList);
    }
  }
}
