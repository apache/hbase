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

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An action to move or swap a region
 */
@InterfaceAudience.Private
abstract class BalanceAction {
  enum Type {
    ASSIGN_REGION,
    MOVE_REGION,
    SWAP_REGIONS,
    MOVE_BATCH,
    NULL,
  }

  static final BalanceAction NULL_ACTION = new NullBalanceAction();

  private final Type type;

  BalanceAction(Type type) {
    this.type = type;
  }

  /**
   * Returns an Action which would undo this action
   */
  abstract BalanceAction undoAction();

  /**
   * Returns the Action represented as RegionPlans
   */
  abstract List<RegionPlan> toRegionPlans(BalancerClusterState cluster);

  Type getType() {
    return type;
  }

  long getStepCount() {
    return 1;
  }

  @Override
  public String toString() {
    return type + ":";
  }

  private static final class NullBalanceAction extends BalanceAction {
    private NullBalanceAction() {
      super(Type.NULL);
    }

    @Override
    BalanceAction undoAction() {
      return this;
    }

    @Override
    List<RegionPlan> toRegionPlans(BalancerClusterState cluster) {
      return Collections.emptyList();
    }
  }
}
