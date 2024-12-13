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
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimaps;

@InterfaceAudience.Private
public class MoveBatchAction extends BalanceAction {
  private final List<MoveRegionAction> moveActions;

  MoveBatchAction(List<MoveRegionAction> moveActions) {
    super(Type.MOVE_BATCH);
    this.moveActions = moveActions;
  }

  @Override
  long getStepCount() {
    return moveActions.size();
  }

  public HashMultimap<Integer, Integer> getServerToRegionsToRemove() {
    return moveActions.stream().collect(Multimaps.toMultimap(MoveRegionAction::getFromServer,
      MoveRegionAction::getRegion, HashMultimap::create));
  }

  public HashMultimap<Integer, Integer> getServerToRegionsToAdd() {
    return moveActions.stream().collect(Multimaps.toMultimap(MoveRegionAction::getToServer,
      MoveRegionAction::getRegion, HashMultimap::create));
  }

  List<MoveRegionAction> getMoveActions() {
    return moveActions;
  }
}
