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

import java.util.List;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

@InterfaceAudience.Private
class MoveRegionAction extends BalanceAction {
  private final int region;
  private final int fromServer;
  private final int toServer;

  public MoveRegionAction(int region, int fromServer, int toServer) {
    super(Type.MOVE_REGION);
    this.fromServer = fromServer;
    this.region = region;
    this.toServer = toServer;
  }

  public int getRegion() {
    return region;
  }

  public int getFromServer() {
    return fromServer;
  }

  public int getToServer() {
    return toServer;
  }

  @Override
  public BalanceAction undoAction() {
    return new MoveRegionAction(region, toServer, fromServer);
  }

  @Override
  List<RegionPlan> toRegionPlans(BalancerClusterState cluster) {
    return ImmutableList.of(new RegionPlan(cluster.regions[getRegion()],
      cluster.servers[getFromServer()], cluster.servers[getToServer()]));
  }

  @Override
  public String toString() {
    return getType() + ": " + region + ":" + fromServer + " -> " + toServer;
  }
}
