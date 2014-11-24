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

import org.apache.hadoop.hbase.Stoppable;

/**
 * A utility to injects faults in a running cluster.
 * <p>
 * ChaosMonkey defines Action's and Policy's. Actions are sequences of events, like
 *  - Select a random server to kill
 *  - Sleep for 5 sec
 *  - Start the server on the same host
 * Actions can also be complex events, like rolling restart of all of the servers.
 * <p>
 * Policies on the other hand are responsible for executing the actions based on a strategy.
 * The default policy is to execute a random action every minute based on predefined action
 * weights. ChaosMonkey executes predefined named policies until it is stopped. More than one
 * policy can be active at any time.
 * <p>
 * Chaos monkey can be run from the command line, or can be invoked from integration tests.
 * See {@link org.apache.hadoop.hbase.IntegrationTestIngest} or other integration tests that use
 * chaos monkey for code examples.
 * <p>
 * ChaosMonkey class is indeed inspired by the Netflix's same-named tool:
 * http://techblog.netflix.com/2012/07/chaos-monkey-released-into-wild.html
 */
public abstract class ChaosMonkey implements Stoppable {
  public abstract void start() throws Exception;

  @Override
  public abstract void stop(String why);

  @Override
  public abstract boolean isStopped();

  public abstract void waitForStop() throws InterruptedException;
}
