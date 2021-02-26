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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Chaos Monkey policy that will run two different actions at the same time.
 * A random action from each array of actions will be chosen and then run in parallel.
 */
public class TwoConcurrentActionPolicy extends PeriodicPolicy {
  private final Action[] actionsOne;
  private final Action[] actionsTwo;
  private final ExecutorService executor;

  public TwoConcurrentActionPolicy(long sleepTime, Action[] actionsOne, Action[] actionsTwo) {
    super(sleepTime);
    this.actionsOne = actionsOne;
    this.actionsTwo = actionsTwo;
    executor = Executors.newFixedThreadPool(2,
      new ThreadFactoryBuilder().setNameFormat("TwoConcurrentAction-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  @Override
  protected void runOneIteration() {
    Action actionOne = PolicyBasedChaosMonkey.selectRandomItem(actionsOne);
    Action actionTwo = PolicyBasedChaosMonkey.selectRandomItem(actionsTwo);

    Future fOne = executor.submit(new ActionRunner(actionOne));
    Future fTwo = executor.submit(new ActionRunner(actionTwo));

    try {
      fOne.get();
      fTwo.get();
    } catch (InterruptedException e) {
      LOG.warn("Exception occurred during performing action: "
          + StringUtils.stringifyException(e));
    } catch (ExecutionException ex) {
      LOG.warn("Exception occurred during performing action: "
          + StringUtils.stringifyException(ex));
    }
  }

  @Override
  public void init(PolicyContext context) throws Exception {
    super.init(context);
    for (Action a : actionsOne) {
      a.init(context);
    }
    for (Action a : actionsTwo) {
      a.init(context);
    }
  }

  private static class ActionRunner implements Runnable {

    private final Action action;

    public ActionRunner(Action action) {

      this.action = action;
    }

    @Override public void run() {
      try {
        action.perform();
      } catch (Exception ex) {
        LOG.warn("Exception occurred during performing action: "
            + StringUtils.stringifyException(ex));
      }
    }
  }
}
