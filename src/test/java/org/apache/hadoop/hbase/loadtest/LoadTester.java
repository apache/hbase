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
package org.apache.hadoop.hbase.loadtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class LoadTester {

  public static String defaultTestClusterName = "VerificationTest_DummyTable";
  public String inputFilename = "LoadTest.properties";

  private boolean verbose = false;

  int clusterTestTime = 60;
  int regionsPerServer = 5;

  public static final String dashedLine =
      "-----------------------------------------------------------------------";

  // global HBase configuration for the JVM - referenced by all classes.
  private Configuration config;

  // for admin operations use a config with higher timeouts
  private Configuration adminConfig;

  // startup options
  public static Options options = new Options();

  // table name for the test
  public byte[] tableName;
  // column families used by the test
  private static final Log LOG = LogFactory.getLog(LoadTester.class);

  MultiThreadedReader reader;
  MultiThreadedWriter writer;

  ColumnFamilyProperties[] familyProperties;

  public LoadTester(String inputFilename, String zkNodeName, String tableNameString) {
    if (inputFilename != null) {
      this.inputFilename = inputFilename;
    }
    this.config = HBaseUtils.getHBaseConfFromZkNode(zkNodeName);

    // for admin operations create a similar config, except 
    // set the RPC timeout much higher (5 mins).
    this.adminConfig = HBaseUtils.getHBaseConfFromZkNode(zkNodeName);
    this.adminConfig.setInt("hbase.rpc.timeout", 5 * 60 * 1000);

    LOG.info("Adding hbase.zookeeper.quorum = "
        + config.get("hbase.zookeeper.quorum"));
    if (tableNameString == null) {
      tableNameString = defaultTestClusterName;
    } else {
      tableNameString = "VerificationTest_" + tableNameString;
    }
    this.tableName = Bytes.toBytes(tableNameString);
    this.writer = new MultiThreadedWriter(this.config, tableName);
    this.reader = new MultiThreadedReader(this.config, tableName);
  }

  long startKey;
  long endKey;
  int readerThreads;
  int writerThreads;

  public void readPropertiesFile() {
    Properties properties = new Properties();
    try {
      File inputFile = new File(inputFilename);
      if (inputFile.exists()) {
        LOG.info("Found properties file at " + inputFile.getAbsolutePath()
            + " so loading it..... ");
        properties.load(new FileInputStream(inputFile));
      } else {
        LOG.info("Did not find properties file " + inputFilename
            + " so using default properties..... ");
        LOG.info("Properties is : \n"
            + ColumnFamilyProperties.defaultColumnProperties);
        properties.load(new StringReader(
            ColumnFamilyProperties.defaultColumnProperties));
      }

      verbose =
          Boolean.parseBoolean(properties.getProperty("Verbose", "False"));
      readerThreads =
          Integer.parseInt(properties.getProperty("ReaderThreads", "1"));
      writerThreads =
          Integer.parseInt(properties.getProperty("WriterThreads", "1"));
      startKey = Long.parseLong(properties.getProperty("StartKey", "1"));
      endKey = Long.parseLong(properties.getProperty("EndKey", "10"));
      int numberOfFamilies =
          Integer.parseInt(properties.getProperty("NumberOfFamilies", "1"));
      int verifyPercent =
          Integer.parseInt(properties.getProperty("VerifyPercent", "10"));
      clusterTestTime =
          Integer.parseInt(properties.getProperty("ClusterTestTime", "60"));
      writer.setBulkLoad(Boolean.parseBoolean(properties.getProperty(
          "BulkLoad", "False")));
      regionsPerServer =
          Integer.parseInt(properties.getProperty("RegionsPerServer", "5"));

      if (verbose == true) {
        LOG.info("Reader Threads: " + readerThreads);
        LOG.info("Writer Threads: " + writerThreads);
        LOG.info("Number of Column Families: " + numberOfFamilies);
        LOG.info("Key range: " + startKey + "..." + endKey);
        LOG.info("VerifyPercent: " + verifyPercent);
        LOG.info("ClusterTestTime: " + clusterTestTime);
        LOG.info("BulkLoad: " + writer.getBulkLoad());
        LOG.info("RegionsPerServer: " + regionsPerServer);
      }

      this.familyProperties = new ColumnFamilyProperties[numberOfFamilies];
      for (int i = 0; i < numberOfFamilies; i++) {
        familyProperties[i] = new ColumnFamilyProperties();
        String columnPrefix = "CF" + (i + 1) + "_";
        familyProperties[i].familyName =
            properties.getProperty(columnPrefix + "Name", "Dummy " + (i + 1));
        familyProperties[i].startTimestamp =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "StartTimestamp", "1"));
        familyProperties[i].endTimestamp =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "EndTimestamp", "10"));
        familyProperties[i].minColDataSize =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "MinDataSize", "1"));
        familyProperties[i].maxColDataSize =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "MaxDataSize", "10"));
        familyProperties[i].minColsPerKey =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "MinColsPerKey", "1"));
        familyProperties[i].maxColsPerKey =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "MaxColsPerKey", "10"));
        familyProperties[i].maxVersions =
            Integer.parseInt(properties.getProperty(columnPrefix
                + "MaxVersions", "10"));
        familyProperties[i].filterType =
            properties.getProperty(columnPrefix + "FilterType", "None");
        familyProperties[i].bloomType =
            properties.getProperty(columnPrefix + "BloomType", "None");
        familyProperties[i].compressionType =
            properties.getProperty(columnPrefix + "CompressionType", "None");
        if (verbose == true) {
          familyProperties[i].print();
        }
      }
      writer.setColumnFamilyProperties(familyProperties);
      reader.setColumnFamilyProperties(familyProperties);
      reader.setVerficationPercent(verifyPercent);
      reader.setVerbose(verbose);
      writer.setVerbose(verbose);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.info("Error reading properties file... Aborting!!!");
      System.exit(0);
    }
  }

  public void loadData() {
    createTableIfNotExists();
    writer.setStopOnError(true);
    reader.setWriteHappeningInParallel();
    writer.start(startKey, endKey, writerThreads);
    LOG.info("Started writing data...");
  }

  public void readData() {
    createTableIfNotExists();
    reader.setStopOnError(true);
    reader.start(startKey, endKey, readerThreads);
    LOG.info("Started reading data...");
  }

  public boolean createTableIfNotExists() {
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info("Creating table if not exists................................");
    }
    return HBaseUtils.createTableIfNotExists(adminConfig, tableName,
        familyProperties, regionsPerServer);
  }

  public boolean deleteTable() {
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info("Deleting table if it exists......");
    }
    return HBaseUtils.deleteTable(adminConfig, tableName);
  }

  public void startAction(MultiThreadedAction action, String actionType,
      int numberOfThreads) {
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info("Starting " + actionType + " thread..........................");
    }
    action.start(startKey, endKey, numberOfThreads);
  }

  public boolean flushTable() {
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info("Flushing table....");
    }
    return HBaseUtils.flushTable(config, tableName);
  }

  public boolean testCluster() {
    int flushAfterStartInSeconds = clusterTestTime / 4;
    int stopAfterFlushInSeconds = clusterTestTime / 4;
    int checkAfterStopInSeconds = clusterTestTime / 4;
    int deleteAfterCheckInSeconds = clusterTestTime / 16;

    boolean tableCreated = createTableIfNotExists();

    reader.setWriteHappeningInParallel();
    startAction(writer, "Writer", writerThreads);
    startAction(reader, "Reader", readerThreads);

    sleep(flushAfterStartInSeconds);
    boolean tableFlushed = flushTable();

    sleep(stopAfterFlushInSeconds);
    stopAction(writer, "Writer");
    stopAction(reader, "Reader");

    sleep(checkAfterStopInSeconds);
    boolean writerStatus = checkActionStatus(writer, "Writer");
    boolean readerStatus = checkActionStatus(reader, "Reader");

    sleep(deleteAfterCheckInSeconds);
    boolean tableDeleted = deleteTable();

    boolean overall =
        tableCreated && tableFlushed && writerStatus && readerStatus
            && tableDeleted;

    String passed = "Passed! :)";
    String failed = "Failed  :(";
    LOG.info(dashedLine);
    LOG.info("Summary of cluster test");
    LOG.info(dashedLine);
    LOG.info("Table creating: " + (tableCreated ? passed : failed));
    LOG.info("Table flushing: " + (tableFlushed ? passed : failed));
    LOG.info("Table Deleting: " + (tableDeleted ? passed : failed));
    LOG.info("Writer status: " + (writerStatus ? passed : failed));
    LOG.info("Reader status: " + (readerStatus ? passed : failed));
    LOG.info(dashedLine);
    LOG.info("Cluster test: " + (overall ? passed : failed));
    LOG.info(dashedLine);

    return overall;
  }

  public void sleep(int seconds) {
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info("Sleeping for " + seconds + " seconds.... zzz");
    }
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void stopAction(MultiThreadedAction action, String actionType) {
    action.pleaseStopRunning();
    if (verbose) {
      LOG.info(dashedLine);
      LOG.info(actionType + " was running with "
          + action.numThreadsWorking.get() + " threads.");
      LOG.info("Stopping" + actionType + "thread............................");
    }
  }

  public boolean checkActionStatus(MultiThreadedAction action, String actionType) {
    boolean workingProperly = true;
    if (action.numErrors_.get() > 0) {
      LOG.info(dashedLine);
      LOG.info("PROBLEM: " + actionType + " has " + action.numErrors_.get()
          + " errors");
      workingProperly = false;
    } else if (verbose) {
      LOG.info(dashedLine);
      LOG.info(actionType + " has no errors");
    }

    if (action.numOpFailures_.get() > 0) {
      LOG.info(dashedLine);
      LOG.info("PROBLEM: " + actionType + " has " + action.numOpFailures_.get()
          + " op failures");
      workingProperly = false;
    } else if (verbose) {
      LOG.info(dashedLine);
      LOG.info(actionType + " has no op failures");
    }

    if (action.numRows_.get() <= 0) {
      LOG.info(dashedLine);
      LOG.info("PROBLEM: " + actionType + " has not processed any keys.");
      workingProperly = false;
    } else if (verbose) {
      LOG.info(dashedLine);
      LOG.info(actionType + " has processed keys.");
    }

    if (action.numThreadsWorking.get() != 0) {
      LOG.info(dashedLine);
      LOG.info("PROBLEM: " + actionType + " has not stopped yet. "
          + action.numThreadsWorking.get() + " threads were still running"
          + " so will kill them.");
      workingProperly = false;
      action.killAllThreads();
    } else if (verbose) {
      LOG.info(dashedLine);
      LOG.info(actionType + " has stopped running");
    }
    return workingProperly;
  }

  public static void main(String[] args) {
    try {
      CommandLine cmd = initAndParseArgs(args);
      if (cmd.hasOption(OPT_HELP)) {
        printUsage();
        return;
      }
      String inputFilename = cmd.getOptionValue(OPT_INPUT_FILENAME);
      String zkNodeName = cmd.getOptionValue(OPT_ZKNODE);
      String tableName = cmd.getOptionValue(OPT_TABLE_NAME);
      LoadTester hBaseTest = new LoadTester(inputFilename, zkNodeName, tableName);
      hBaseTest.readPropertiesFile();

      if (cmd.hasOption(OPT_DELETE_TABLE)) {
        hBaseTest.deleteTable();
      }

      if (cmd.hasOption(OPT_LOAD)) {
        hBaseTest.loadData();
      } else if (cmd.hasOption(OPT_READ)) {
        hBaseTest.readData();
      } else if (cmd.hasOption(OPT_LOADREAD)) {
        hBaseTest.loadData();
        hBaseTest.readData();
      } else if (cmd.hasOption(OPT_TEST_CLUSTER)
          || !cmd.hasOption(OPT_DELETE_TABLE)) {
        hBaseTest.deleteTable();
        boolean clusterStatus = hBaseTest.testCluster();
        if (clusterStatus) {
          System.exit(1);
        } else {
          System.exit(-1);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
    }
  }

  private static String USAGE;
  private static final String HEADER = "HBaseTest";
  private static final String FOOTER =
      "\nCalling this wihout any arguments"
          + " will start a short verification test with default table name and"
          + " current machine as zookeeper. "
          + "\n To specify a different zk use the -zk option.\n";
  private static final String OPT_ZKNODE = "zk";
  private static final String OPT_LOAD = "load";
  private static final String OPT_READ = "read";
  private static final String OPT_LOADREAD = "loadread";
  private static final String OPT_DELETE_TABLE = "deletetable";
  private static final String OPT_TEST_CLUSTER = "testcluster";
  private static final String OPT_TABLE_NAME = "tn";
  private static final String OPT_INPUT_FILENAME = "inputfile";
  private static final String OPT_HELP = "help";

  static CommandLine initAndParseArgs(String[] args) throws ParseException {
    // set the usage object
    USAGE =
        "bin/hbase org.apache.hadoop.hbase.loadtest.Tester " + "  -"
            + OPT_ZKNODE + " <Zookeeper node>" + "  -" + OPT_TABLE_NAME
            + " <Table name>" + "  -" + OPT_LOAD + "  -" + OPT_READ + "  -"
            + OPT_TEST_CLUSTER + "  -" + OPT_HELP + "  -" + OPT_INPUT_FILENAME;
    // add options
    options.addOption(OPT_HELP, false, "Help");
    options.addOption(OPT_ZKNODE, true, "Zookeeper node in the HBase cluster"
        + " (optional)");
    options.addOption(OPT_TABLE_NAME, true,
        "The name of the table to be read or write (optional)");
    options.addOption(OPT_INPUT_FILENAME, true,
        "Path to input configuration file (optional)");
    options.addOption(OPT_LOAD, false, "Command to load Data");
    options.addOption(OPT_READ, false, "Command to read Data assuming all"
        + " required data had been previously loaded to the table");
    options.addOption(OPT_LOADREAD, false, "Command to load and read Data");
    options.addOption(OPT_DELETE_TABLE, false, "Command to delete table before"
        + " testing it");
    options.addOption(OPT_TEST_CLUSTER, false, "Command to run a short"
        + " verification test on cluster."
        + " This also deletes the table if it exists before running the test");
    // parse the passed in options
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    return cmd;
  }

  private static void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(80);
    helpFormatter.printHelp(USAGE, HEADER, options, FOOTER);
  }
}