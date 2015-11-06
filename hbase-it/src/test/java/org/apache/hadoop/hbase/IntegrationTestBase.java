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

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
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
 * Usage: bin/hbase <sub_class_of_IntegrationTestBase> <options>
 * Options: -h,--help Show usage
 *          -m,--monkey <arg> Which chaos monkey to run
 *          -monkeyProps <arg> The properties file for specifying chaos monkey properties.
 *          -ncc Option to not clean up the cluster at the end.
 */
public abstract class IntegrationTestBase extends AbstractHBaseTool {

  public static final String NO_CLUSTER_CLEANUP_LONG_OPT = "noClusterCleanUp";
  public static final String MONKEY_LONG_OPT = "monkey";
  public static final String CHAOS_MONKEY_PROPS = "monkeyProps";
  private static final Log LOG = LogFactory.getLog(IntegrationTestBase.class);

  protected IntegrationTestingUtility util;
  protected ChaosMonkey monkey;
  protected String monkeyToUse;
  protected Properties monkeyProps;
  protected boolean noClusterCleanUp = false;

  public IntegrationTestBase() {
    this(null);
  }

  public IntegrationTestBase(String monkeyToUse) {
    this.monkeyToUse = monkeyToUse;
  }

  @Override
  protected void addOptions() {
    addOptWithArg("m", MONKEY_LONG_OPT, "Which chaos monkey to run");
    addOptNoArg("ncc", NO_CLUSTER_CLEANUP_LONG_OPT,
      "Don't clean up the cluster at the end");
    addOptWithArg(CHAOS_MONKEY_PROPS, "The properties file for specifying chaos "
        + "monkey properties.");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.hasOption(MONKEY_LONG_OPT)) {
      monkeyToUse = cmd.getOptionValue(MONKEY_LONG_OPT);
    }
    if (cmd.hasOption(NO_CLUSTER_CLEANUP_LONG_OPT)) {
      noClusterCleanUp = true;
    }
    monkeyProps = new Properties();
    if (cmd.hasOption(CHAOS_MONKEY_PROPS)) {
      String chaosMonkeyPropsFile = cmd.getOptionValue(CHAOS_MONKEY_PROPS);
      if (StringUtils.isNotEmpty(chaosMonkeyPropsFile)) {
        try {
          monkeyProps.load(this.getClass().getClassLoader()
              .getResourceAsStream(chaosMonkeyPropsFile));
        } catch (IOException e) {
          LOG.warn(e);
          System.exit(EXIT_FAILURE);
        }
      }
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
      fact = getDefaultMonkeyFactory();
    }
    monkey = fact.setUtil(util)
                 .setTableName(getTablename())
                 .setProperties(monkeyProps)
                 .setColumnFamilies(getColumnFamilies()).build();
    startMonkey();
  }

  protected MonkeyFactory getDefaultMonkeyFactory() {
    // Run with no monkey in distributed context, with real monkey in local test context.
    return MonkeyFactory.getFactory(
      util.isDistributedCluster() ? MonkeyFactory.CALM : MonkeyFactory.SLOW_DETERMINISTIC);
  }

  protected void startMonkey() throws Exception {
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
        this.setConf(util.getConfiguration());
      } else {
        this.util = new IntegrationTestingUtility(conf);
      }
    }
    return util;
  }

  public abstract void setUpCluster() throws Exception;

  public void cleanUpCluster() throws Exception {
    if (util.isDistributedCluster() &&  (monkey == null || !monkey.isDestructive())) {
      noClusterCleanUp = true;
    }
    if (noClusterCleanUp) {
      LOG.debug("noClusterCleanUp is set, skip restoring the cluster");
      return;
    }
    LOG.debug("Restoring the cluster");
    util.restoreCluster();
    LOG.debug("Done restoring the cluster");
  }

  public abstract int runTestFromCommandLine() throws Exception;

  /**
   * Provides the name of the table that is protected from random Chaos monkey activity
   * @return table to not delete.
   */
  public abstract TableName getTablename();

  /**
   * Provides the name of the CFs that are protected from random Chaos monkey activity (alter)
   * @return set of cf names to protect.
   */
  protected abstract Set<String> getColumnFamilies();
}
