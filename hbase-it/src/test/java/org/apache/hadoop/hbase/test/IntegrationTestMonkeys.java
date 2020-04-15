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

package org.apache.hadoop.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.util.ChaosMonkeyRunner;
import org.apache.hadoop.hbase.chaos.util.Monkeys;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This is an integration test for showing a simple usage of how to use {@link Monkeys}
 * to control {@link ChaosMonkeyRunner}.
 */
@Category(IntegrationTests.class)
public class IntegrationTestMonkeys extends ChaosMonkeyRunner {
  private static final int RUN_SECS = 15 * 1000;
  private static final int WAIT_SECS = 10 * 1000;

  @Override
  protected int doWork() throws Exception {
    super.setUpCluster();
    runMonkeys();
    return 0;
  }

  @Test
  public void runMonkeys() throws Exception {
    try (Monkeys monkeys = new Monkeys()) {
      for (int i = 0; i < 2; i++) {
        monkeys.startChaos();
        Thread.sleep(RUN_SECS);
        monkeys.stopChaos();
        Thread.sleep(WAIT_SECS);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // Run chaos monkeys 15 seconds, then stop them.
    // After 10 seconds, run chaos monkeys again.
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int exitCode = ToolRunner.run(conf, new IntegrationTestMonkeys(), args);
    System.exit(exitCode);
  }
}
