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
import org.apache.hadoop.util.StringUtils;

/** A policy which performs a sequence of actions deterministically. */
public class DoActionsOncePolicy extends PeriodicPolicy {
  private List<Action> actions;

  public DoActionsOncePolicy(long periodMs, List<Action> actions) {
    super(periodMs);
    this.actions = new ArrayList<>(actions);
  }

  public DoActionsOncePolicy(long periodMs, Action... actions) {
    this(periodMs, Arrays.asList(actions));
  }

  @Override
  protected void runOneIteration() {
    if (actions.isEmpty()) {
      this.stop("done");
      return;
    }
    Action action = actions.remove(0);

    try {
      action.perform();
    } catch (Exception ex) {
      LOG.warn("Exception occurred during performing action: "
          + StringUtils.stringifyException(ex));
    }
  }

  @Override
  public void init(PolicyContext context) throws Exception {
    super.init(context);
    for (Action action : actions) {
      action.init(this.context);
    }
  }
}
