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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.chaos.factories.MonkeyFactory;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * Base class for HBase integration tests that want to use the Chaos Monkey.
 * Usage: bin/hbase &lt;sub_class_of_IntegrationTestBase> &lt;options>
 * Options: -h,--help Show usage
 *          -m,--monkey &lt;arg> Which chaos monkey to run
 *          -monkeyProps &lt;arg> The properties file for specifying chaos monkey properties.
 *          -ncc Option to not clean up the cluster at the end.
 */
public abstract class IntegrationTestBase extends AbstractHBaseTool {

  public static final String NO_CLUSTER_CLEANUP_LONG_OPT = "noClusterCleanUp";
  public static final String MONKEY_LONG_OPT = "monkey";
  public static final String CHAOS_MONKEY_PROPS = "monkeyProps";
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);

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

  /**
   * This allows tests that subclass children of this base class such as
   * {@link org.apache.hadoop.hbase.test.IntegrationTestReplication} to
   * include the base options without having to also include the options from the test.
   *
   * @param cmd the command line
   */
  protected void processBaseOptions(CommandLine cmd) {
    if (cmd.hasOption(MONKEY_LONG_OPT)) {
      monkeyToUse = cmd.getOptionValue(MONKEY_LONG_OPT);
    }
    if (cmd.hasOption(NO_CLUSTER_CLEANUP_LONG_OPT)) {
      noClusterCleanUp = true;
    }
    monkeyProps = new Properties();
    // Add entries for the CM from hbase-site.xml as a convenience.
    // Do this prior to loading from the properties file to make sure those in the properties
    // file are given precedence to those in hbase-site.xml (backwards compatibility).
    loadMonkeyProperties(monkeyProps, conf);
    if (cmd.hasOption(CHAOS_MONKEY_PROPS)) {
      String chaosMonkeyPropsFile = cmd.getOptionValue(CHAOS_MONKEY_PROPS);
      if (StringUtils.isNotEmpty(chaosMonkeyPropsFile)) {
        try {
          monkeyProps.load(this.getClass().getClassLoader()
              .getResourceAsStream(chaosMonkeyPropsFile));
        } catch (IOException e) {
          LOG.warn(e.toString(), e);
          System.exit(EXIT_FAILURE);
        }
      }
    }
  }

  /**
   * Loads entries from the provided {@code conf} into {@code props} when the configuration key
   * is one that may be configuring ChaosMonkey actions.
   */
  public static void loadMonkeyProperties(Properties props, Configuration conf) {
    for (Entry<String,String> entry : conf) {
      for (String prefix : MonkeyConstants.MONKEY_CONFIGURATION_KEY_PREFIXES) {
        if (entry.getKey().startsWith(prefix)) {
          props.put(entry.getKey(), entry.getValue());
          break;
        }
      }
    }
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    processBaseOptions(cmd);
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
    ChoreService choreService = null;

    // Launches chore for refreshing kerberos credentials if security is enabled.
    // Please see http://hbase.apache.org/book.html#_running_canary_in_a_kerberos_enabled_cluster
    // for more details.
    final ScheduledChore authChore = AuthUtil.getAuthChore(conf);
    if (authChore != null) {
      choreService = new ChoreService("INTEGRATION_TEST");
      choreService.scheduleChore(authChore);
    }

    setUp();
    int result = -1;
    try {
      result = runTestFromCommandLine();
    } finally {
      cleanUp();
    }

    if (choreService != null) {
      choreService.shutdown();
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
    LOG.info("Using chaos monkey factory: {}", fact.getClass());
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
