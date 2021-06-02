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
package org.apache.hadoop.hbase.chaos.util;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.factories.MonkeyFactory;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

public class ChaosMonkeyRunner extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(ChaosMonkeyRunner.class);
  public static final String MONKEY_LONG_OPT = "monkey";
  public static final String CHAOS_MONKEY_PROPS = "monkeyProps";
  public static final String TABLE_NAME_OPT = "tableName";
  public static final String FAMILY_NAME_OPT = "familyName";
  protected IntegrationTestingUtility util;
  protected ChaosMonkey monkey;
  protected String monkeyToUse;
  protected Properties monkeyProps;
  protected boolean noClusterCleanUp = false;
  private String tableName = "ChaosMonkeyRunner.tableName";
  private String familyName = "ChaosMonkeyRunner.familyName";

  @Override
  public void addOptions() {
    // The -c option is processed down in the main, not along w/ other options. Added here so shows
    // in the usage output.
    addOptWithArg("c", "Name of extra configurations file to find on CLASSPATH");
    addOptWithArg("m", MONKEY_LONG_OPT, "Which chaos monkey to run");
    addOptWithArg(CHAOS_MONKEY_PROPS, "The properties file for specifying chaos "
        + "monkey properties.");
    addOptWithArg(TABLE_NAME_OPT, "Table name in the test to run chaos monkey against");
    addOptWithArg(FAMILY_NAME_OPT, "Family name in the test to run chaos monkey against");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.hasOption(MONKEY_LONG_OPT)) {
      monkeyToUse = cmd.getOptionValue(MONKEY_LONG_OPT);
    }
    monkeyProps = new Properties();
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
    if (cmd.hasOption(TABLE_NAME_OPT)) {
      this.tableName = cmd.getOptionValue(TABLE_NAME_OPT);
    }
    if (cmd.hasOption(FAMILY_NAME_OPT)) {
      this.familyName = cmd.getOptionValue(FAMILY_NAME_OPT);
    }
  }

  @Override
  protected int doWork() throws Exception {
    setUpCluster();
    getAndStartMonkey();
    while (!monkey.isStopped()) {
      // loop here until got killed
      try {
        // TODO: make sleep time configurable
        Thread.sleep(5000); // 5 seconds
      } catch (InterruptedException ite) {
        // Chaos monkeys got interrupted.
        // It is ok to stop monkeys and exit.
        monkey.stop("Interruption occurred.");
        break;
      }
    }
    monkey.waitForStop();
    return 0;
  }

  public void stopRunner() {
    if (monkey != null) {
      monkey.stop("Program Control");
    }
  }

  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    boolean isDistributed = isDistributedCluster(getConf());
    if (isDistributed) {
      util.createDistributedHBaseCluster();
      util.checkNodeCount(1);// make sure there's at least 1 alive rs
    } else {
      throw new RuntimeException("ChaosMonkeyRunner must run against a distributed cluster,"
          + " please check and point to the right configuration dir");
    }
    this.setConf(util.getConfiguration());
  }

  private boolean isDistributedCluster(Configuration conf) {
    return conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false);
  }

  public void getAndStartMonkey() throws Exception {
    util = getTestingUtil(getConf());
    MonkeyFactory fact = MonkeyFactory.getFactory(monkeyToUse);
    if (fact == null) {
      fact = getDefaultMonkeyFactory();
    }
    monkey =
        fact.setUtil(util).setTableName(getTablename()).setProperties(monkeyProps)
            .setColumnFamilies(getColumnFamilies()).build();
    monkey.start();
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

  protected MonkeyFactory getDefaultMonkeyFactory() {
    // Run with slow deterministic monkey by default
    return MonkeyFactory.getFactory(MonkeyFactory.SLOW_DETERMINISTIC);
  }

  public TableName getTablename() {
    return TableName.valueOf(tableName);
  }

  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(familyName);
  }

  /*
   * If caller wants to add config parameters from a file, the path to the conf file
   * can be passed like this: -c <path-to-conf>. The file is presumed to have the Configuration
   * file xml format and is added as a new Resource to the current Configuration.
   * Use this mechanism to set Configuration such as what ClusterManager to use, etc.
   * Here is an example file you might references that sets an alternate ClusterManager:
   * {code}
   * <?xml version="1.0" encoding="UTF-8"?>
   * <configuration>
   *   <property>
   *     <name>hbase.it.clustermanager.class</name>
   *     <value>org.apache.hadoop.hbase.MyCustomClusterManager</value>
   *   </property>
   * </configuration>
   * {code}
   * NOTE: The code searches for the file name passed on the CLASSPATH! Passing the path to a file
   * will not work! Add the file to the CLASSPATH and then pass the filename as the '-c' arg.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String [] actualArgs = args;
    if (args.length > 0 && "-c".equals(args[0])) {
      int argCount = args.length - 2;
      if (argCount < 0) {
        throw new IllegalArgumentException("Missing path for -c parameter");
      }
      // Load the resource specified by the second parameter. We load from the classpath, not
      // from filesystem path.
      conf.addResource(args[1]);
      actualArgs = new String[argCount];
      System.arraycopy(args, 2, actualArgs, 0, argCount);
    }
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new ChaosMonkeyRunner(), actualArgs);
    System.exit(ret);
  }
}
