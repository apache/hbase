/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.manual;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.manual.utils.HBaseUtils;
import org.apache.hadoop.hbase.manual.utils.KillProcessesAndVerify;
import org.apache.hadoop.hbase.manual.utils.MultiThreadedReader;
import org.apache.hadoop.hbase.manual.utils.MultiThreadedWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBaseTest
{
  static {
      // make the root logger display only errors
      Logger.getRootLogger().setLevel(Level.ERROR);
      // enable debugging for our package
      Logger.getLogger("org.apache.hadoop.hbase.manual").setLevel(Level.DEBUG);
      Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.INFO);
  }

  // global HBase configuration for the JVM - referenced by all classes.
  public static List<HBaseConfiguration> configList_ = new ArrayList<HBaseConfiguration>();
  // startup options
  public static Options options_ = new Options();
  // command line options object
  public static CommandLine cmd_;

  // table name for the test
  public static byte[] tableName_;
  // column families used by the test
  public static byte[][] columnFamilies_ = { Bytes.toBytes("actions") };
  private static final Log LOG = LogFactory.getLog(HBaseTest.class);
  static Random random_ = new Random();

  // usage string for loading data
  static final String OPT_USAGE_LOAD = " <num keys>:<average cols per key>:<avg data size>[:<num threads = 20>]";
  /**
   * Reads the following params from the command line:
   *  <Number of keys to load>:<Average columns per key>:<Average data size per column>[:<num threads = 20>]
   */
  public void loadData() {
    // parse command line data
    String[] cols = cmd_.getOptionValue(OPT_LOAD).split(":");
    long startKey = 0;
    long endKey = Long.parseLong(cols[0]);
    long minColsPerKey = 1;
    long maxColsPerKey = 2 * Long.parseLong(cols[1]);
    int minColDataSize = Integer.parseInt(cols[2])/2;
    int maxColDataSize = Integer.parseInt(cols[2]) * 3 / 2;
    int numThreads = (endKey - startKey > 1000)? 20 : 1;
    if (cols.length > 3) {
      numThreads = Integer.parseInt(cols[3]);
    }

    // print out the args
    System.out.printf("Key range %d .. %d\n", startKey, endKey);
    System.out.printf("Number of Columns/Key: %d..%d\n", minColsPerKey, maxColsPerKey);
    System.out.printf("Data Size/Column: %d..%d bytes\n", minColDataSize, maxColDataSize);
    System.out.printf("Client Threads: %d\n", numThreads);

    // start the writers
    MultiThreadedWriter writer = new MultiThreadedWriter(configList_.get(0), tableName_, columnFamilies_[0]);
    writer.setBulkLoad(true);
    writer.setColumnsPerKey(minColsPerKey, maxColsPerKey);
    writer.setDataSize(minColDataSize, maxColDataSize);
    writer.start(startKey, endKey, numThreads);
    System.out.printf("Started loading data...");
//    writer.waitForFinish();
  }

  static final String OPT_USAGE_READ = " <start key>:<end key>[:<verify percent = 0>:<num threads = 20>]";
  /**
   * Reads the following params from the command line:
   *  <Starting key to read>:<End key to read>:<Percent of keys to verify (data is stamped with key)>[:<num threads = 20>]
   */
  public void readData() {
    // parse command line data
    String[] cols = cmd_.getOptionValue(OPT_READ).split(":");
    long startKey = Long.parseLong(cols[0]);
    long endKey = Long.parseLong(cols[1]);
    int verifyPercent = Integer.parseInt(cols[2]);
    int numThreads = (endKey - startKey > 1000)? 20 : 1;
    if (cols.length > 3) {
      numThreads = Integer.parseInt(cols[3]);
    }

    // if the read test is running at the same time as the load,
    // ignore the start/endKey options
    if (cmd_.hasOption(OPT_LOAD)) {
      startKey = -1;
      endKey = -1;
    } else {
      System.out.printf("Key range %d .. %d\n", startKey, endKey);
    }

    System.out.printf("Verify percent of keys: %d\n", verifyPercent);
    System.out.printf("Client Threads: %d\n", numThreads);

    // start the readers
    MultiThreadedReader reader = new MultiThreadedReader(configList_.get(0), tableName_, columnFamilies_[0]);
    reader.setVerficationPercent(verifyPercent);
    reader.start(startKey, endKey, numThreads);
  }

 static final String OPT_USAGE_KILL = " <HBase root path>:<HDFS root path>:<minutes between kills>:<RS kill %>:<#keys to verify>";

  /**
   * Reads the following params from the command line:
   *  <Path to HBase root on deployed cluster>:<Path to HDFS root on deployed cluster>:
   *  <Minutes between server kills>:<RS kill % (versus DN)>:<# keys to verify after kill (# keys loaded in 4 minutes is a good number)>
   */
  public void killAndVerify() {
    // parse command line data
    String[] cols = cmd_.getOptionValue(OPT_KILL).split(":");
    String clusterHBasePath = cols[0];
    String clusterHDFSPath = cols[1];
    int timeIntervalBetweenKills = Integer.parseInt(cols[2]);
    int rSKillPercent = Integer.parseInt(cols[3]);
    int numKeysToVerify = Integer.parseInt(cols[4]);

    System.out.printf("Time between kills:  %d minutes\n", timeIntervalBetweenKills);
    System.out.printf("RegionServer (rest is DataNode) kill percent: %d\n", rSKillPercent);
    System.out.printf("Num keys to verify after killing: %d\n", numKeysToVerify);

    (new KillProcessesAndVerify(clusterHBasePath, clusterHDFSPath, timeIntervalBetweenKills, rSKillPercent, numKeysToVerify)).start();
    System.out.printf("Started kill test...");
  }

  public static void main(String[] args) {
    try {
      // parse the command line args
      initAndParseArgs(args);

      HBaseTest hBaseTest = new HBaseTest();
      // create the HBase configuration from ZooKeeper node
      String[] zkNodes = cmd_.getOptionValue(OPT_ZKNODE).split(":");
      for(String zkNode : zkNodes) {
        HBaseConfiguration conf = HBaseUtils.getHBaseConfFromZkNode(zkNode);
        LOG.info("Adding hbase.zookeeper.quorum = " + conf.get("hbase.zookeeper.quorum"));
        configList_.add(conf);
      }

      String tn = cmd_.getOptionValue(OPT_TABLE_NAME, "test1");
      tableName_ = Bytes.toBytes(tn);

      // create tables if needed
      for(HBaseConfiguration conf : configList_) {
        HBaseUtils.createTableIfNotExists(conf, tableName_, columnFamilies_);
      }

      // write some test data in an infinite loop if needed
      if(cmd_.hasOption(OPT_LOAD)) {
        hBaseTest.loadData();
      }
      // kill servers and verify the data integrity
      if(cmd_.hasOption(OPT_KILL)) {
        hBaseTest.killAndVerify();
      }
      // read the test data in an infinite loop
      if(cmd_.hasOption(OPT_READ)) {
        hBaseTest.readData();
      }
    } catch(Exception e) {
      e.printStackTrace();
      printUsage();
    }
  }

  private static String USAGE;
  private static final String HEADER = "HBaseTest";
  private static final String FOOTER = "";
  private static final String OPT_ZKNODE = "zk";
  private static final String OPT_LOAD = "load";
  private static final String OPT_READ = "read";
  private static final String OPT_KILL = "kill";
  private static final String OPT_TABLE_NAME = "tn";
  static void initAndParseArgs(String[] args) throws ParseException {
    // set the usage object
    USAGE =  "bin/hbase org.apache.hadoop.hbase.manual.HBaseTest "
            + "  -" + OPT_ZKNODE   + " <Zookeeper node>"
            + "  _" + OPT_TABLE_NAME + " <Table name>"
            + "  -" + OPT_LOAD     + OPT_USAGE_LOAD
            + "  -" + OPT_READ     + OPT_USAGE_READ
            + "  -" + OPT_KILL     + OPT_USAGE_KILL;
    // add options
    options_.addOption(OPT_ZKNODE    , true, "Zookeeper node in the HBase cluster");
    options_.addOption(OPT_TABLE_NAME, true, "The name of the table to be read or write");
    options_.addOption(OPT_LOAD      , true, OPT_USAGE_LOAD);
    options_.addOption(OPT_READ      , true, OPT_USAGE_READ);
    options_.addOption(OPT_KILL      , true, OPT_USAGE_KILL);
    // parse the passed in options
    CommandLineParser parser = new BasicParser();
    cmd_ = parser.parse(options_, args);
  }

  private static void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter( );
    helpFormatter.setWidth( 80 );
    helpFormatter.printHelp( USAGE, HEADER, options_, FOOTER );
  }
}
