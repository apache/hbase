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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * A base class for tests that do something with the cluster while running
 * {@link LoadTestTool} to write and verify some data.
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngest extends IntegrationTestBase {
  public static final char HIPHEN = '-';
  private static final int SERVER_COUNT = 4; // number of slaves for the smallest cluster
  private static final long DEFAULT_RUN_TIME = 20 * 60 * 1000;
  private static final long JUNIT_RUN_TIME = 10 * 60 * 1000;

  /** A soft limit on how long we should run */
  private static final String RUN_TIME_KEY = "hbase.%s.runtime";

  protected static final Log LOG = LogFactory.getLog(IntegrationTestIngest.class);
  protected IntegrationTestingUtility util;
  protected HBaseCluster cluster;
  protected LoadTestTool loadTool;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    LOG.debug("Initializing/checking cluster has " + SERVER_COUNT + " servers");
    util.initializeCluster(SERVER_COUNT);
    LOG.debug("Done initializing/checking cluster");
    cluster = util.getHBaseClusterInterface();
    deleteTableIfNecessary();
    loadTool = new LoadTestTool();
    loadTool.setConf(util.getConfiguration());
    // Initialize load test tool before we start breaking things;
    // LoadTestTool init, even when it is a no-op, is very fragile.
    initTable();
  }

  protected void initTable() throws IOException {
    int ret = loadTool.run(new String[] { "-tn", getTablename(), "-init_only" });
    Assert.assertEquals("Failed to initialize LoadTestTool", 0, ret);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    internalRunIngestTest(DEFAULT_RUN_TIME);
    return 0;
  }

  @Test
  public void testIngest() throws Exception {
    runIngestTest(JUNIT_RUN_TIME, 2500, 10, 1024, 10);
  }

  private void internalRunIngestTest(long runTime) throws Exception {
    runIngestTest(runTime, 2500, 10, 1024, 10);
  }

  @Override
  public String getTablename() {
    return this.getClass().getSimpleName();
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(LoadTestTool.COLUMN_FAMILY));
  }

  private void deleteTableIfNecessary() throws IOException {
    if (util.getHBaseAdmin().tableExists(getTablename())) {
      util.deleteTable(Bytes.toBytes(getTablename()));
    }
  }

  protected void runIngestTest(long defaultRunTime, int keysPerServerPerIter, int colsPerKey,
      int recordSize, int writeThreads) throws Exception {
    LOG.info("Running ingest");
    LOG.info("Cluster size:" + util.getHBaseClusterInterface().getClusterStatus().getServersSize());

    long start = System.currentTimeMillis();
    String runtimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
    long runtime = util.getConfiguration().getLong(runtimeKey, defaultRunTime);
    long startKey = 0;

    long numKeys = getNumKeys(keysPerServerPerIter);
    while (System.currentTimeMillis() - start < 0.9 * runtime) {
      LOG.info("Intended run time: " + (runtime/60000) + " min, left:" +
          ((runtime - (System.currentTimeMillis() - start))/60000) + " min");

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

      ret = loadTool.run(getArgsForLoadTestTool("-read", "100:20", startKey, numKeys));
      if (0 != ret) {
        String errorMsg = "Verification failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }
      startKey += numKeys;
    }
  }

  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    List<String> args = new ArrayList<String>();
    args.add("-tn");
    args.add(getTablename());
    args.add(mode);
    args.add(modeSpecificArg);
    args.add("-start_key");
    args.add(String.valueOf(startKey));
    args.add("-num_keys");
    args.add(String.valueOf(numKeys));
    args.add("-skip_init");
    return args.toArray(new String[args.size()]);
  }

  /** Estimates a data size based on the cluster size */
  private long getNumKeys(int keysPerServer)
      throws IOException {
    int numRegionServers = cluster.getClusterStatus().getServersSize();
    return keysPerServer * numRegionServers;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngest(), args);
    System.exit(ret);
  }
}
