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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.BoundedGroupingStrategy.DEFAULT_NUM_REGION_GROUPS;
import static org.apache.hadoop.hbase.wal.BoundedGroupingStrategy.NUM_REGION_GROUPS;
import static org.apache.hadoop.hbase.wal.RegionGroupingProvider.DELEGATE_PROVIDER;
import static org.apache.hadoop.hbase.wal.RegionGroupingProvider.REGION_GROUPING_STRATEGY;
import static org.apache.hadoop.hbase.wal.WALFactory.WAL_PROVIDER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: delegate-provider={0}")
public class TestBoundedRegionGroupingStrategy {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBoundedRegionGroupingStrategy.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration CONF;
  private static DistributedFileSystem FS;

  public String walProvider;

  public TestBoundedRegionGroupingStrategy(String walProvider) {
    this.walProvider = walProvider;
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of("defaultProvider"), Arguments.of("asyncfs"));
  }

  @BeforeEach
  public void setUp() throws Exception {
    CONF.set(DELEGATE_PROVIDER, walProvider);
  }

  @AfterEach
  public void tearDown() throws Exception {
    FileStatus[] entries = FS.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      FS.delete(dir.getPath(), true);
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    // Make block sizes small.
    CONF.setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    CONF.setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    CONF.setInt("dfs.heartbeat.interval", 1);
    CONF.setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    CONF.setInt("hbase.ipc.client.connect.max.retries", 1);
    CONF.setInt("dfs.client.block.recovery.retries", 1);
    CONF.setInt("hbase.ipc.client.connection.maxidletime", 500);

    CONF.setClass(WAL_PROVIDER, RegionGroupingProvider.class, WALProvider.class);
    CONF.set(REGION_GROUPING_STRATEGY, RegionGroupingProvider.Strategies.bounded.name());

    TEST_UTIL.startMiniDFSCluster(3);

    FS = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   */
  @TestTemplate
  public void testConcurrentWrites() throws Exception {
    // Run the WPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = WALPerformanceEvaluation.innerMain(new Configuration(CONF),
      new String[] { "-threads", "3", "-verify", "-noclosefs", "-iterations", "3000" });
    assertEquals(0, errCode);
  }

  /**
   * Make sure we can successfully run with more regions then our bound.
   */
  @TestTemplate
  public void testMoreRegionsThanBound() throws Exception {
    final String parallelism = Integer.toString(DEFAULT_NUM_REGION_GROUPS * 2);
    int errCode =
      WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
        parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
    assertEquals(0, errCode);
  }

  @TestTemplate
  public void testBoundsGreaterThanDefault() throws Exception {
    final int temp = CONF.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    try {
      CONF.setInt(NUM_REGION_GROUPS, temp * 4);
      final String parallelism = Integer.toString(temp * 4);
      int errCode =
        WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
          parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
      assertEquals(0, errCode);
    } finally {
      CONF.setInt(NUM_REGION_GROUPS, temp);
    }
  }

  @TestTemplate
  public void testMoreRegionsThanBoundWithBoundsGreaterThanDefault() throws Exception {
    final int temp = CONF.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    try {
      CONF.setInt(NUM_REGION_GROUPS, temp * 4);
      final String parallelism = Integer.toString(temp * 4 * 2);
      int errCode =
        WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
          parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
      assertEquals(0, errCode);
    } finally {
      CONF.setInt(NUM_REGION_GROUPS, temp);
    }
  }

  /**
   * Ensure that we can use Set.add to deduplicate WALs
   */
  @TestTemplate
  public void setMembershipDedups() throws IOException {
    final int temp = CONF.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    WALFactory wals = null;
    try {
      CONF.setInt(NUM_REGION_GROUPS, temp * 4);
      // Set HDFS root directory for storing WAL
      CommonFSUtils.setRootDir(CONF, TEST_UTIL.getDataTestDirOnTestFS());

      wals = new WALFactory(CONF, "setMembershipDedups");
      Set<WAL> seen = new HashSet<>(temp * 4);
      int count = 0;
      // we know that this should see one of the wals more than once
      for (int i = 0; i < temp * 8; i++) {
        WAL maybeNewWAL = wals.getWAL(RegionInfoBuilder
          .newBuilder(TableName.valueOf("Table-" + ThreadLocalRandom.current().nextInt())).build());
        LOG.info("Iteration " + i + ", checking wal " + maybeNewWAL);
        if (seen.add(maybeNewWAL)) {
          count++;
        }
      }
      assertEquals(temp * 4, count,
        "received back a different number of WALs that are not equal() to each other "
          + "than the bound we placed.");
    } finally {
      if (wals != null) {
        wals.close();
      }
      CONF.setInt(NUM_REGION_GROUPS, temp);
    }
  }
}
