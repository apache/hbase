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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;

/**
 * A base class for tests that do something with the cluster while running
 * {@link LoadTestTool} to write and verify some data.
 */
public abstract class IngestIntegrationTestBase {
  private static String tableName = null;

  /** A soft limit on how long we should run */
  private static final String RUN_TIME_KEY = "hbase.%s.runtime";

  protected static final Log LOG = LogFactory.getLog(IngestIntegrationTestBase.class);
  protected IntegrationTestingUtility util;
  protected HBaseCluster cluster;
  private LoadTestTool loadTool;

  protected void setUp(int numSlavesBase) throws Exception {
    tableName = this.getClass().getSimpleName();
    util = new IntegrationTestingUtility();
    LOG.info("Initializing cluster with " + numSlavesBase + " servers");
    util.initializeCluster(numSlavesBase);
    LOG.info("Done initializing cluster");
    cluster = util.getHBaseClusterInterface();
    deleteTableIfNecessary();
    loadTool = new LoadTestTool();
    loadTool.setConf(util.getConfiguration());
    // Initialize load test tool before we start breaking things;
    // LoadTestTool init, even when it is a no-op, is very fragile.
    int ret = loadTool.run(new String[] { "-tn", tableName, "-init_only" });
    Assert.assertEquals("Failed to initialize LoadTestTool", 0, ret);
  }

  protected void tearDown() throws Exception {
    LOG.info("Restoring the cluster");
    util.restoreCluster();
    LOG.info("Done restoring the cluster");
  }

  private void deleteTableIfNecessary() throws IOException {
    if (util.getHBaseAdmin().tableExists(tableName)) {
      util.deleteTable(Bytes.toBytes(tableName));
    }
  }

  protected void runIngestTest(long defaultRunTime, int keysPerServerPerIter,
      int colsPerKey, int recordSize, int writeThreads) throws Exception {
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

      int ret = loadTool.run(new String[] {
          "-tn", tableName,
          "-write", String.format("%d:%d:%d", colsPerKey, recordSize, writeThreads),
          "-start_key", String.valueOf(startKey),
          "-num_keys", String.valueOf(numKeys),
          "-skip_init"
      });
      if (0 != ret) {
        String errorMsg = "Load failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }

      ret = loadTool.run(new String[] {
          "-tn", tableName,
          "-read", "100:20",
          "-start_key", String.valueOf(startKey),
          "-num_keys", String.valueOf(numKeys),
          "-skip_init"
      });
      if (0 != ret) {
        String errorMsg = "Verification failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }
      startKey += numKeys;
    }
  }

  /** Estimates a data size based on the cluster size */
  private long getNumKeys(int keysPerServer)
      throws IOException {
    int numRegionServers = cluster.getClusterStatus().getServersSize();
    return keysPerServer * numRegionServers;
  }
}
