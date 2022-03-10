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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;

/** A policy which does stuff every time interval. */
public abstract class PeriodicPolicy extends Policy {
  private long periodMs;

  public PeriodicPolicy(long periodMs) {
    this.periodMs = periodMs;
  }

  @Override
  public void run() {
    // Add some jitter.
    int jitter = ThreadLocalRandom.current().nextInt((int)periodMs);
    LOG.info("Sleeping for {} ms to add jitter", jitter);
    Threads.sleep(jitter);

    while (!isStopped()) {
      long start = EnvironmentEdgeManager.currentTime();
      runOneIteration();

      if (isStopped()) return;
      long sleepTime = periodMs - (EnvironmentEdgeManager.currentTime() - start);
      if (sleepTime > 0) {
        LOG.info("Sleeping for {} ms", sleepTime);
        Threads.sleep(sleepTime);
      }
    }
  }

  protected abstract void runOneIteration();

  @Override
  public void init(PolicyContext context) throws Exception {
    super.init(context);
    LOG.info("Using ChaosMonkey Policy {}, period={} ms", this.getClass(), periodMs);
  }
}
