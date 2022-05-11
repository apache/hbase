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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * A base class for tests that do something with the cluster while running {@link LoadTestTool} to
 * write and verify some data.
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngest extends IntegrationTestBase {
  public static final char HIPHEN = '-';
  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster
  protected static final long DEFAULT_RUN_TIME = 20 * 60 * 1000;
  protected static final long JUNIT_RUN_TIME = 10 * 60 * 1000;

  /** A soft limit on how long we should run */
  protected static final String RUN_TIME_KEY = "hbase.%s.runtime";

  protected static final String NUM_KEYS_PER_SERVER_KEY = "num_keys_per_server";
  protected static final long DEFAULT_NUM_KEYS_PER_SERVER = 2500;

  protected static final String NUM_WRITE_THREADS_KEY = "num_write_threads";
  protected static final int DEFAULT_NUM_WRITE_THREADS = 20;

  protected static final String NUM_READ_THREADS_KEY = "num_read_threads";
  protected static final int DEFAULT_NUM_READ_THREADS = 20;

  // Log is being used in IntegrationTestIngestWithEncryption, hence it is protected
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestIngest.class);
  protected IntegrationTestingUtility util;
  protected HBaseCluster cluster;
  protected LoadTestTool loadTool;

  protected String[] LOAD_TEST_TOOL_INIT_ARGS =
    { LoadTestTool.OPT_COLUMN_FAMILIES, LoadTestTool.OPT_COMPRESSION,
      HFileTestUtil.OPT_DATA_BLOCK_ENCODING, LoadTestTool.OPT_INMEMORY, LoadTestTool.OPT_ENCRYPTION,
      LoadTestTool.OPT_NUM_REGIONS_PER_SERVER, LoadTestTool.OPT_REGION_REPLICATION, };

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    LOG.debug("Initializing/checking cluster has " + SERVER_COUNT + " servers");
    util.initializeCluster(getMinServerCount());
    LOG.debug("Done initializing/checking cluster");
    cluster = util.getHBaseClusterInterface();
    deleteTableIfNecessary();
    loadTool = new LoadTestTool();
    loadTool.setConf(util.getConfiguration());
    // Initialize load test tool before we start breaking things;
    // LoadTestTool init, even when it is a no-op, is very fragile.
    initTable();
  }

  protected int getMinServerCount() {
    return SERVER_COUNT;
  }

  protected void initTable() throws IOException {
    int ret = loadTool.run(getArgsForLoadTestToolInitTable());
    Assert.assertEquals("Failed to initialize LoadTestTool", 0, ret);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    internalRunIngestTest(DEFAULT_RUN_TIME);
    return 0;
  }

  @Test
  public void testIngest() throws Exception {
    runIngestTest(JUNIT_RUN_TIME, 2500, 10, 1024, 10, 20);
  }

  protected void internalRunIngestTest(long runTime) throws Exception {
    String clazz = this.getClass().getSimpleName();
    long numKeysPerServer = conf.getLong(String.format("%s.%s", clazz, NUM_KEYS_PER_SERVER_KEY),
      DEFAULT_NUM_KEYS_PER_SERVER);
    int numWriteThreads =
      conf.getInt(String.format("%s.%s", clazz, NUM_WRITE_THREADS_KEY), DEFAULT_NUM_WRITE_THREADS);
    int numReadThreads =
      conf.getInt(String.format("%s.%s", clazz, NUM_READ_THREADS_KEY), DEFAULT_NUM_READ_THREADS);
    runIngestTest(runTime, numKeysPerServer, 10, 1024, numWriteThreads, numReadThreads);
  }

  @Override
  public TableName getTablename() {
    String clazz = this.getClass().getSimpleName();
    return TableName
      .valueOf(conf.get(String.format("%s.%s", clazz, LoadTestTool.OPT_TABLE_NAME), clazz));
  }

  @Override
  protected Set<String> getColumnFamilies() {
    Set<String> families = Sets.newHashSet();
    String clazz = this.getClass().getSimpleName();
    // parse conf for getting the column famly names because LTT is not initialized yet.
    String familiesString =
      getConf().get(String.format("%s.%s", clazz, LoadTestTool.OPT_COLUMN_FAMILIES));
    if (familiesString == null) {
      for (byte[] family : HFileTestUtil.DEFAULT_COLUMN_FAMILIES) {
        families.add(Bytes.toString(family));
      }
    } else {
      for (String family : familiesString.split(",")) {
        families.add(family);
      }
    }

    return families;
  }

  private void deleteTableIfNecessary() throws IOException {
    if (util.getAdmin().tableExists(getTablename())) {
      util.deleteTable(getTablename());
    }
  }

  protected void runIngestTest(long defaultRunTime, long keysPerServerPerIter, int colsPerKey,
    int recordSize, int writeThreads, int readThreads) throws Exception {

    LOG.info("Running ingest");
    LOG.info("Cluster size:"
      + util.getHBaseClusterInterface().getClusterMetrics().getLiveServerMetrics().size());

    long start = System.currentTimeMillis();
    String runtimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
    long runtime = util.getConfiguration().getLong(runtimeKey, defaultRunTime);
    long startKey = 0;

    long numKeys = getNumKeys(keysPerServerPerIter);
    while (System.currentTimeMillis() - start < 0.9 * runtime) {
      LOG.info("Intended run time: " + (runtime / 60000) + " min, left:"
        + ((runtime - (System.currentTimeMillis() - start)) / 60000) + " min");

      int ret = -1;
      ret = loadTool.run(getArgsForLoadTestTool("-write",
        String.format("%d:%d:%d", colsPerKey, recordSize, writeThreads), startKey, numKeys));
      if (0 != ret) {
        String errorMsg = "Load failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }

      ret = loadTool.run(getArgsForLoadTestTool("-update", String.format("60:%d:1", writeThreads),
        startKey, numKeys));
      if (0 != ret) {
        String errorMsg = "Update failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }

      ret = loadTool.run(
        getArgsForLoadTestTool("-read", String.format("100:%d", readThreads), startKey, numKeys));
      if (0 != ret) {
        String errorMsg = "Verification failed with error code " + ret;
        LOG.error(errorMsg + " Rerunning verification after 1 minute for debugging");
        Threads.sleep(1000 * 60);
        ret = loadTool.run(
          getArgsForLoadTestTool("-read", String.format("100:%d", readThreads), startKey, numKeys));
        if (0 != ret) {
          LOG.error("Rerun of Verification failed with error code " + ret);
        }
        Assert.fail(errorMsg);
      }
      startKey += numKeys;
    }
  }

  protected String[] getArgsForLoadTestToolInitTable() {
    List<String> args = new ArrayList<>();
    args.add("-tn");
    args.add(getTablename().getNameAsString());
    // pass all remaining args from conf with keys <test class name>.<load test tool arg>
    String clazz = this.getClass().getSimpleName();
    for (String arg : LOAD_TEST_TOOL_INIT_ARGS) {
      String val = conf.get(String.format("%s.%s", clazz, arg));
      if (val != null) {
        args.add("-" + arg);
        args.add(val);
      }
    }
    args.add("-init_only");
    return args.toArray(new String[args.size()]);
  }

  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
    long numKeys) {
    List<String> args = new ArrayList<>(11);
    args.add("-tn");
    args.add(getTablename().getNameAsString());
    args.add("-families");
    args.add(getColumnFamiliesAsString());
    args.add(mode);
    args.add(modeSpecificArg);
    args.add("-start_key");
    args.add(String.valueOf(startKey));
    args.add("-num_keys");
    args.add(String.valueOf(numKeys));
    args.add("-skip_init");

    return args.toArray(new String[args.size()]);
  }

  private String getColumnFamiliesAsString() {
    return StringUtils.join(",", getColumnFamilies());
  }

  /** Estimates a data size based on the cluster size */
  protected long getNumKeys(long keysPerServer) throws IOException {
    int numRegionServers = cluster.getClusterMetrics().getLiveServerMetrics().size();
    return keysPerServer * numRegionServers;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngest(), args);
    System.exit(ret);
  }
}
