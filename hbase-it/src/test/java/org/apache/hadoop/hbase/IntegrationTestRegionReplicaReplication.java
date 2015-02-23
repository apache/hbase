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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.ConstantDelayQueue;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.MultiThreadedUpdater;
import org.apache.hadoop.hbase.util.MultiThreadedWriter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Integration test for testing async wal replication to secondary region replicas. Sets up a table
 * with given region replication (default 2), and uses LoadTestTool client writer, updater and
 * reader threads for writes and reads and verification. It uses a delay queue with a given delay
 * ("read_delay_ms", default 5000ms) between the writer/updater and reader threads to make the
 * written items available to readers. This means that a reader will only start reading from a row
 * written by the writer / updater after 5secs has passed. The reader thread performs the reads from
 * the given region replica id (default 1) to perform the reads. Async wal replication has to finish
 * with the replication of the edits before read_delay_ms to the given region replica id so that
 * the read and verify will not fail.
 *
 * The job will run for <b>at least<b> given runtime (default 10min) by running a concurrent
 * writer and reader workload followed by a concurrent updater and reader workload for
 * num_keys_per_server.
 *<p>
 * Example usage:
 * <pre>
 * hbase org.apache.hadoop.hbase.IntegrationTestRegionReplicaReplication
 * -DIntegrationTestRegionReplicaReplication.num_keys_per_server=10000
 * -Dhbase.IntegrationTestRegionReplicaReplication.runtime=600000
 * -DIntegrationTestRegionReplicaReplication.read_delay_ms=5000
 * -DIntegrationTestRegionReplicaReplication.region_replication=3
 * -DIntegrationTestRegionReplicaReplication.region_replica_id=2
 * -DIntegrationTestRegionReplicaReplication.num_read_threads=100
 * -DIntegrationTestRegionReplicaReplication.num_write_threads=100
 * </pre>
 */
@Category(IntegrationTests.class)
public class IntegrationTestRegionReplicaReplication extends IntegrationTestIngest {

  private static final String TEST_NAME
    = IntegrationTestRegionReplicaReplication.class.getSimpleName();

  private static final String OPT_READ_DELAY_MS = "read_delay_ms";

  private static final int DEFAULT_REGION_REPLICATION = 2;
  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster
  private static final String[] DEFAULT_COLUMN_FAMILIES = new String[] {"f1", "f2", "f3"};

  @Override
  protected int getMinServerCount() {
    return SERVER_COUNT;
  }

  @Override
  public void setConf(Configuration conf) {
    conf.setIfUnset(
      String.format("%s.%s", TEST_NAME, LoadTestTool.OPT_REGION_REPLICATION),
      String.valueOf(DEFAULT_REGION_REPLICATION));

    conf.setIfUnset(
      String.format("%s.%s", TEST_NAME, LoadTestTool.OPT_COLUMN_FAMILIES),
      StringUtils.join(",", DEFAULT_COLUMN_FAMILIES));

    conf.setBoolean("hbase.table.sanity.checks", true);

    // enable async wal replication to region replicas for unit tests
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024L * 1024 * 4); // flush every 4 MB
    conf.setInt("hbase.hstore.blockingStoreFiles", 100);

    super.setConf(conf);
  }

  @Override
  @Test
  public void testIngest() throws Exception {
    runIngestTest(JUNIT_RUN_TIME, 25000, 10, 1024, 10, 20);
  }

  @Override
  protected void startMonkey() throws Exception {
    // TODO: disabled for now
  }

  /**
   * This extends MultiThreadedWriter to add a configurable delay to the keys written by the writer
   * threads to become available to the MultiThradedReader threads. We add this delay because of
   * the async nature of the wal replication to region replicas.
   */
  public static class DelayingMultiThreadedWriter extends MultiThreadedWriter {
    private long delayMs;
    public DelayingMultiThreadedWriter(LoadTestDataGenerator dataGen, Configuration conf,
        TableName tableName) throws IOException {
      super(dataGen, conf, tableName);
    }
    @Override
    protected BlockingQueue<Long> createWriteKeysQueue(Configuration conf) {
      this.delayMs = conf.getLong(String.format("%s.%s",
        IntegrationTestRegionReplicaReplication.class.getSimpleName(), OPT_READ_DELAY_MS), 5000);
      return new ConstantDelayQueue<Long>(TimeUnit.MILLISECONDS, delayMs);
    }
  }

  /**
   * This extends MultiThreadedWriter to add a configurable delay to the keys written by the writer
   * threads to become available to the MultiThradedReader threads. We add this delay because of
   * the async nature of the wal replication to region replicas.
   */
  public static class DelayingMultiThreadedUpdater extends MultiThreadedUpdater {
    private long delayMs;
    public DelayingMultiThreadedUpdater(LoadTestDataGenerator dataGen, Configuration conf,
        TableName tableName, double updatePercent) throws IOException {
      super(dataGen, conf, tableName, updatePercent);
    }
    @Override
    protected BlockingQueue<Long> createWriteKeysQueue(Configuration conf) {
      this.delayMs = conf.getLong(String.format("%s.%s",
        IntegrationTestRegionReplicaReplication.class.getSimpleName(), OPT_READ_DELAY_MS), 5000);
      return new ConstantDelayQueue<Long>(TimeUnit.MILLISECONDS, delayMs);
    }
  }

  @Override
  protected void runIngestTest(long defaultRunTime, long keysPerServerPerIter, int colsPerKey,
      int recordSize, int writeThreads, int readThreads) throws Exception {

    LOG.info("Running ingest");
    LOG.info("Cluster size:" + util.getHBaseClusterInterface().getClusterStatus().getServersSize());

    // sleep for some time so that the cache for disabled tables does not interfere.
    Threads.sleep(
      getConf().getInt("hbase.region.replica.replication.cache.disabledAndDroppedTables.expiryMs",
        5000) + 1000);

    long start = System.currentTimeMillis();
    String runtimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
    long runtime = util.getConfiguration().getLong(runtimeKey, defaultRunTime);
    long startKey = 0;

    long numKeys = getNumKeys(keysPerServerPerIter);
    while (System.currentTimeMillis() - start < 0.9 * runtime) {
      LOG.info("Intended run time: " + (runtime/60000) + " min, left:" +
          ((runtime - (System.currentTimeMillis() - start))/60000) + " min");

      int verifyPercent = 100;
      int updatePercent = 20;
      int ret = -1;
      int regionReplicaId = conf.getInt(String.format("%s.%s"
        , TEST_NAME, LoadTestTool.OPT_REGION_REPLICA_ID), 1);

      // we will run writers and readers at the same time.
      List<String> args = Lists.newArrayList(getArgsForLoadTestTool("", "", startKey, numKeys));
      args.add("-write");
      args.add(String.format("%d:%d:%d", colsPerKey, recordSize, writeThreads));
      args.add("-" + LoadTestTool.OPT_MULTIPUT);
      args.add("-writer");
      args.add(DelayingMultiThreadedWriter.class.getName()); // inject writer class
      args.add("-read");
      args.add(String.format("%d:%d", verifyPercent, readThreads));
      args.add("-" + LoadTestTool.OPT_REGION_REPLICA_ID);
      args.add(String.valueOf(regionReplicaId));

      ret = loadTool.run(args.toArray(new String[args.size()]));
      if (0 != ret) {
        String errorMsg = "Load failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }

      args = Lists.newArrayList(getArgsForLoadTestTool("", "", startKey, numKeys));
      args.add("-update");
      args.add(String.format("%s:%s:1", updatePercent, writeThreads));
      args.add("-updater");
      args.add(DelayingMultiThreadedUpdater.class.getName()); // inject updater class
      args.add("-read");
      args.add(String.format("%d:%d", verifyPercent, readThreads));
      args.add("-" + LoadTestTool.OPT_REGION_REPLICA_ID);
      args.add(String.valueOf(regionReplicaId));

      ret = loadTool.run(args.toArray(new String[args.size()]));
      if (0 != ret) {
        String errorMsg = "Load failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }
      startKey += numKeys;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestRegionReplicaReplication(), args);
    System.exit(ret);
  }
}
