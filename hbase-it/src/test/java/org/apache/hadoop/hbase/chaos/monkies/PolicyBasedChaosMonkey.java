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

package org.apache.hadoop.hbase.chaos.monkies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.policies.Policy;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Chaos monkey that given multiple policies will run actions against the cluster.
 */
public class PolicyBasedChaosMonkey extends ChaosMonkey {

  private static final long ONE_SEC = 1000;
  private static final long ONE_MIN = 60 * ONE_SEC;

  public static final long TIMEOUT = ONE_MIN;

  final IntegrationTestingUtility util;
  final Properties monkeyProps;

  private final Policy[] policies;
  private final ExecutorService monkeyThreadPool;

  /**
   * Construct a new ChaosMonkey
   * @param util the HBaseIntegrationTestingUtility already configured
   * @param policies custom policies to use
   */
  public PolicyBasedChaosMonkey(IntegrationTestingUtility util, Policy... policies) {
    this(null, util, policies);
  }

  public PolicyBasedChaosMonkey(IntegrationTestingUtility util, Collection<Policy> policies) {
    this(null, util, policies);
  }

  public PolicyBasedChaosMonkey(Properties monkeyProps, IntegrationTestingUtility util,
    Collection<Policy> policies) {
    this(monkeyProps, util, policies.toArray(new Policy[0]));
  }

  public PolicyBasedChaosMonkey(Properties monkeyProps, IntegrationTestingUtility util,
    Policy... policies) {
    this.monkeyProps = monkeyProps;
    this.util = Objects.requireNonNull(util);
    this.policies = Objects.requireNonNull(policies);
    if (policies.length == 0) {
      throw new IllegalArgumentException("policies may not be empty");
    }
    this.monkeyThreadPool = buildMonkeyThreadPool(policies.length);
  }

  private static ExecutorService buildMonkeyThreadPool(final int size) {
    return Executors.newFixedThreadPool(size, new ThreadFactoryBuilder()
      .setDaemon(false)
      .setNameFormat("ChaosMonkey-%d")
      .setUncaughtExceptionHandler((t, e) -> {
        throw new RuntimeException(e);
      })
      .build());
  }

  /** Selects a random item from the given items */
  public static <T> T selectRandomItem(T[] items) {
    return items[ThreadLocalRandom.current().nextInt(items.length)];
  }

  /** Selects a random item from the given items with weights*/
  public static <T> T selectWeightedRandomItem(List<Pair<T, Integer>> items) {
    int totalWeight = 0;
    for (Pair<T, Integer> pair : items) {
      totalWeight += pair.getSecond();
    }

    int cutoff = ThreadLocalRandom.current().nextInt(totalWeight);
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
    int selectedNumber = (int)Math.ceil(items.length * ratio);

    List<T> originalItems = Arrays.asList(items);
    Collections.shuffle(originalItems);

    int startIndex = ThreadLocalRandom.current().nextInt(items.length - selectedNumber);
    return originalItems.subList(startIndex, startIndex + selectedNumber);
  }

  @Override
  public void start() throws Exception {
    final Policy.PolicyContext context = new Policy.PolicyContext(monkeyProps, util);
    for (final Policy policy : policies) {
      policy.init(context);
      monkeyThreadPool.execute(policy);
    }
  }

  @Override
  public void stop(String why) {
    // stop accepting new work (shouldn't be any with a fixed-size pool)
    monkeyThreadPool.shutdown();
    // notify all executing policies that it's time to halt.
    for (Policy policy : policies) {
      policy.stop(why);
    }
  }

  @Override
  public boolean isStopped() {
    return monkeyThreadPool.isTerminated();
  }

  @Override
  public void waitForStop() throws InterruptedException {
    monkeyThreadPool.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Override
  public boolean isDestructive() {
    // TODO: we can look at the actions, and decide to do the restore cluster or not based on them.
    return true;
  }
}
