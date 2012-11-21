/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 * A command-line tool that spins up a local process-based cluster, loads
 * some data, restarts the regionserver holding .META., and verifies that the
 * cluster recovers.
 */
public class RestartMetaTest extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(RestartMetaTest.class);

  /** The number of region servers used if not specified */
  private static final int DEFAULT_NUM_RS = 2;

  /** Table name for the test */
  private static byte[] TABLE_NAME = Bytes.toBytes("load_test");

  /** The number of seconds to sleep after loading the data */
  private static final int SLEEP_SEC_AFTER_DATA_LOAD = 5;

  /** The actual number of region servers */
  private int numRegionServers;

  /** HBase home source tree home directory */
  private String hbaseHome;

  private static final String OPT_HBASE_HOME = "hbase_home";
  private static final String OPT_NUM_RS = "num_rs";

  /** Loads data into the table using the multi-threaded writer. */
  private void loadData() throws IOException {
    long startKey = 0;
    long endKey = 100000;
    long minColsPerKey = 5;
    long maxColsPerKey = 15;
    int minColDataSize = 256;
    int maxColDataSize = 256 * 3;
    int numThreads = 10;

    // print out the arguments
    System.out.printf("Key range %d .. %d\n", startKey, endKey);
    System.out.printf("Number of Columns/Key: %d..%d\n", minColsPerKey,
        maxColsPerKey);
    System.out.printf("Data Size/Column: %d..%d bytes\n", minColDataSize,
        maxColDataSize);
    System.out.printf("Client Threads: %d\n", numThreads);

    // start the writers
    MultiThreadedWriter writer = new MultiThreadedWriter(conf, TABLE_NAME,
        LoadTestTool.COLUMN_FAMILY);
    writer.setMultiPut(true);
    writer.setColumnsPerKey(minColsPerKey, maxColsPerKey);
    writer.setDataSize(minColDataSize, maxColDataSize);
    writer.start(startKey, endKey, numThreads);
    System.out.printf("Started loading data...");
    writer.waitForFinish();
    System.out.printf("Finished loading data...");
  }

  @Override
  protected int doWork() throws Exception {
    ProcessBasedLocalHBaseCluster hbaseCluster =
        new ProcessBasedLocalHBaseCluster(conf, hbaseHome, numRegionServers);

    // start the process based HBase cluster
    hbaseCluster.start();

    // create tables if needed
    HBaseTestingUtility.createPreSplitLoadTestTable(conf, TABLE_NAME,
        LoadTestTool.COLUMN_FAMILY, Compression.Algorithm.NONE,
        DataBlockEncoding.NONE);

    LOG.debug("Loading data....\n\n");
    loadData();

    LOG.debug("Sleeping for " + SLEEP_SEC_AFTER_DATA_LOAD +
        " seconds....\n\n");
    Threads.sleep(5 * SLEEP_SEC_AFTER_DATA_LOAD);

    int metaRSPort = HBaseTestingUtility.getMetaRSPort(conf);

    LOG.debug("Killing META region server running on port " + metaRSPort);
    hbaseCluster.killRegionServer(metaRSPort);
    Threads.sleep(2000);

    LOG.debug("Restarting region server running on port metaRSPort");
    hbaseCluster.startRegionServer(metaRSPort);
    Threads.sleep(2000);

    LOG.debug("Trying to scan meta");

    HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
    ResultScanner scanner = metaTable.getScanner(new Scan());
    Result result;
    while ((result = scanner.next()) != null) {
      LOG.info("Region assignment from META: "
          + Bytes.toStringBinary(result.getRow())
          + " => "
          + Bytes.toStringBinary(result.getFamilyMap(HConstants.CATALOG_FAMILY)
              .get(HConstants.SERVER_QUALIFIER)));
    }
    return 0;
  }

  @Override
  protected void addOptions() {
    addRequiredOptWithArg(OPT_HBASE_HOME, "HBase home directory");
    addOptWithArg(OPT_NUM_RS, "Number of Region Servers");
    addOptWithArg(LoadTestTool.OPT_DATA_BLOCK_ENCODING,
        LoadTestTool.OPT_DATA_BLOCK_ENCODING_USAGE);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    hbaseHome = cmd.getOptionValue(OPT_HBASE_HOME);
    if (hbaseHome == null || !new File(hbaseHome).isDirectory()) {
      throw new IllegalArgumentException("Invalid HBase home directory: " +
          hbaseHome);
    }

    LOG.info("Using HBase home directory " + hbaseHome);
    numRegionServers = Integer.parseInt(cmd.getOptionValue(OPT_NUM_RS,
        String.valueOf(DEFAULT_NUM_RS)));
  }

  public static void main(String[] args) {
    new RestartMetaTest().doStaticMain(args);
  }

}
