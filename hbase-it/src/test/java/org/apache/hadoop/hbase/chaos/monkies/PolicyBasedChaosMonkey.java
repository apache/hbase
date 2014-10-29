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

package org.apache.hadoop.hbase.chaos.monkies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.policies.Policy;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Chaos monkey that given multiple policies will run actions against the cluster.
 */
public class PolicyBasedChaosMonkey extends ChaosMonkey {

  private static final Log LOG = LogFactory.getLog(PolicyBasedChaosMonkey.class);
  private static final long ONE_SEC = 1000;
  private static final long FIVE_SEC = 5 * ONE_SEC;
  private static final long ONE_MIN = 60 * ONE_SEC;

  public static final long TIMEOUT = ONE_MIN;

  final IntegrationTestingUtility util;

  /**
   * Construct a new ChaosMonkey
   * @param util the HBaseIntegrationTestingUtility already configured
   * @param policies custom policies to use
   */
  public PolicyBasedChaosMonkey(IntegrationTestingUtility util, Policy... policies) {
    this.util = util;
    this.policies = policies;
  }

  public PolicyBasedChaosMonkey(IntegrationTestingUtility util, Collection<Policy> policies) {
    this.util = util;
    this.policies = policies.toArray(new Policy[policies.size()]);
  }


  /** Selects a random item from the given items */
  public static <T> T selectRandomItem(T[] items) {
    return items[RandomUtils.nextInt(items.length)];
  }

  /** Selects a random item from the given items with weights*/
  public static <T> T selectWeightedRandomItem(List<Pair<T, Integer>> items) {
    int totalWeight = 0;
    for (Pair<T, Integer> pair : items) {
      totalWeight += pair.getSecond();
    }

    int cutoff = RandomUtils.nextInt(totalWeight);
    int cummulative = 0;
    T item = null;

    //warn: O(n)
    for (int i=0; i<items.size(); i++) {
      int curWeight = items.get(i).getSecond();
      if ( cutoff < cummulative + curWeight) {
        item = items.get(i).getFirst();
        break;
      }
      cummulative += curWeight;
    }

    return item;
  }

  /** Selects and returns ceil(ratio * items.length) random items from the given array */
  public static <T> List<T> selectRandomItems(T[] items, float ratio) {
    int remaining = (int)Math.ceil(items.length * ratio);

    List<T> selectedItems = new ArrayList<T>(remaining);

    for (int i=0; i<items.length && remaining > 0; i++) {
      if (RandomUtils.nextFloat() < ((float)remaining/(items.length-i))) {
        selectedItems.add(items[i]);
        remaining--;
      }
    }

    return selectedItems;
  }

  private Policy[] policies;
  private Thread[] monkeyThreads;

  @Override
  public void start() throws Exception {
    monkeyThreads = new Thread[policies.length];

    for (int i=0; i<policies.length; i++) {
      policies[i].init(new Policy.PolicyContext(this.util));
      Thread monkeyThread = new Thread(policies[i]);
      monkeyThread.start();
      monkeyThreads[i] = monkeyThread;
    }
  }

  @Override
  public void stop(String why) {
    if (policies == null) {
      return;
    }

    for (Policy policy : policies) {
      policy.stop(why);
    }
  }

  @Override
  public boolean isStopped() {
    return policies[0].isStopped();
  }

  /**
   * Wait for ChaosMonkey to stop.
   * @throws InterruptedException
   */
  @Override
  public void waitForStop() throws InterruptedException {
    if (monkeyThreads == null) {
      return;
    }
    for (Thread monkeyThread : monkeyThreads) {
      // TODO: bound the wait time per policy
      monkeyThread.join();
    }
  }

  @Override
  public boolean isDestructive() {
    // TODO: we can look at the actions, and decide to do the restore cluster or not based on them.
    return true;
  }
}
