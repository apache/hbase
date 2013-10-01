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

  protected IntegrationTestingUtility util;
  protected ChaosMonkey monkey;
  protected String monkeyToUse;

  public IntegrationTestBase() {
    this(MonkeyFactory.CALM);
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
    setUpMonkey();
    int result = -1;
    try {
      result = runTestFromCommandLine();
    } finally {
      cleanUpMonkey();
      cleanUp();
    }

    return result;
  }

  @Before
  public void setUpMonkey() throws Exception {
    util = getTestingUtil(getConf());
    MonkeyFactory fact = MonkeyFactory.getFactory(monkeyToUse);
    monkey = fact.setUtil(util)
                 .setTableName(getTablename())
                 .setColumnFamilies(getColumnFamilies()).build();
    monkey.start();
  }

  @After
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

  public abstract void setUp() throws Exception;

  public abstract void cleanUp()  throws Exception;

  public abstract int runTestFromCommandLine() throws Exception;

  public abstract String getTablename();

  protected abstract Set<String> getColumnFamilies();
}
