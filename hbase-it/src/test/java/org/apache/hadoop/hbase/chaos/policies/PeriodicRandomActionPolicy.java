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

package org.apache.hadoop.hbase.chaos.policies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

/**
 * A policy, which picks a random action according to the given weights,
 * and performs it every configurable period.
 */
public class PeriodicRandomActionPolicy extends PeriodicPolicy {
  private List<Pair<Action, Integer>> actions;

  public PeriodicRandomActionPolicy(long periodMs, List<Pair<Action, Integer>> actions) {
    super(periodMs);
    this.actions = actions;
  }

  public PeriodicRandomActionPolicy(long periodMs, Pair<Action, Integer>... actions) {
    // We don't expect it to be modified.
    this(periodMs, Arrays.asList(actions));
  }

  public PeriodicRandomActionPolicy(long periodMs, Action... actions) {
    super(periodMs);
    this.actions = new ArrayList<>(actions.length);
    for (Action action : actions) {
      this.actions.add(new Pair<>(action, 1));
    }
  }

  @Override
  protected void runOneIteration() {
    Action action = PolicyBasedChaosMonkey.selectWeightedRandomItem(actions);
    try {
      action.perform();
    } catch (Exception ex) {
      LOG.warn("Exception performing action: " + StringUtils.stringifyException(ex));
    }
  }

  @Override
  public void init(PolicyContext context) throws Exception {
    super.init(context);
    for (Pair<Action, Integer> action : actions) {
      action.getFirst().init(this.context);
    }
  }
}
