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

package org.apache.hadoop.hbase;

import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.factories.MonkeyFactory;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for HBase integration tests that want to use the Chaos Monkey.
 */
public abstract class IntegrationTestBase extends AbstractHBaseTool {
  public static final String LONG_OPT = "monkey";
  private static final Log LOG = LogFactory.getLog(IntegrationTestBase.class);

  protected IntegrationTestingUtility util;
  protected ChaosMonkey monkey;
  protected String monkeyToUse;

  public IntegrationTestBase() {
    this(null);
  }

  public IntegrationTestBase(String monkeyToUse) {
    this.monkeyToUse = monkeyToUse;
  }

  @Override
  protected void addOptions() {
    addOptWithArg("m", LONG_OPT, "Which chaos monkey to run");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.hasOption(LONG_OPT)) {
      monkeyToUse = cmd.getOptionValue(LONG_OPT);
    }
  }

  @Override
  public Configuration getConf() {
    Configuration c = super.getConf();
    if (c == null && util != null) {
      conf = util.getConfiguration();
      c = conf;
    }
    return c;
  }

  @Override
  protected int doWork() throws Exception {
    setUp();
    int result = -1;
    try {
      result = runTestFromCommandLine();
    } finally {
      cleanUp();
    }

    return result;
  }

  @Before
  public void setUp() throws Exception {
    setUpCluster();
    setUpMonkey();
  }

  @After
  public void cleanUp() throws Exception {
    cleanUpMonkey();
    cleanUpCluster();
  }

  public void setUpMonkey() throws Exception {
    util = getTestingUtil(getConf());
    MonkeyFactory fact = MonkeyFactory.getFactory(monkeyToUse);
    if (fact == null) {
      // Run with no monkey in distributed context, with real monkey in local test context.
      fact = MonkeyFactory.getFactory(
          util.isDistributedCluster() ? MonkeyFactory.CALM : MonkeyFactory.SLOW_DETERMINISTIC);
    }
    monkey = fact.setUtil(util)
                 .setTableName(getTablename())
                 .setColumnFamilies(getColumnFamilies()).build();
    monkey.start();
  }

  public void cleanUpMonkey() throws Exception {
    cleanUpMonkey("Ending test");
  }

  protected void cleanUpMonkey(String why) throws Exception {
    if (monkey != null && !monkey.isStopped()) {
      monkey.stop(why);
      monkey.waitForStop();
    }
  }

  protected IntegrationTestingUtility getTestingUtil(Configuration conf) {
    if (this.util == null) {
      if (conf == null) {
        this.util = new IntegrationTestingUtility();
      } else {
        this.util = new IntegrationTestingUtility(conf);
      }
    }
    return util;
  }

  public abstract void setUpCluster() throws Exception;

  public void cleanUpCluster() throws Exception {
    LOG.debug("Restoring the cluster");
    util.restoreCluster();
    LOG.debug("Done restoring the cluster");
  }

  public abstract int runTestFromCommandLine() throws Exception;

  public abstract String getTablename();

  protected abstract Set<String> getColumnFamilies();
}
