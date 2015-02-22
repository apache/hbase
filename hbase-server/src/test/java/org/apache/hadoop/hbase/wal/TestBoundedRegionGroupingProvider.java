/**
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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.hbase.wal.BoundedRegionGroupingProvider.NUM_REGION_GROUPS;
import static org.apache.hadoop.hbase.wal.BoundedRegionGroupingProvider.DEFAULT_NUM_REGION_GROUPS;
import static org.apache.hadoop.hbase.wal.WALFactory.WAL_PROVIDER;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, LargeTests.class})
public class TestBoundedRegionGroupingProvider {
  protected static final Log LOG = LogFactory.getLog(TestBoundedRegionGroupingProvider.class);

  @Rule
  public TestName currentTest = new TestName();
  protected static Configuration conf;
  protected static FileSystem fs;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Make block sizes small.
    conf.setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    conf.setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    conf.setInt("hbase.ipc.client.connect.max.retries", 1);
    conf.setInt("dfs.client.block.recovery.retries", 1);
    conf.setInt("hbase.ipc.client.connection.maxidletime", 500);

    conf.setClass(WAL_PROVIDER, BoundedRegionGroupingProvider.class, WALProvider.class);

    TEST_UTIL.startMiniDFSCluster(3);

    fs = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    // Run the WPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = WALPerformanceEvaluation.innerMain(new Configuration(conf),
        new String [] {"-threads", "3", "-verify", "-noclosefs", "-iterations", "3000"});
    assertEquals(0, errCode);
  }

  /**
   * Make sure we can successfully run with more regions then our bound.
   */
  @Test
  public void testMoreRegionsThanBound() throws Exception {
    final String parallelism = Integer.toString(DEFAULT_NUM_REGION_GROUPS * 2);
    int errCode = WALPerformanceEvaluation.innerMain(new Configuration(conf),
        new String [] {"-threads", parallelism, "-verify", "-noclosefs", "-iterations", "3000",
            "-regions", parallelism});
    assertEquals(0, errCode);
  }

  @Test
  public void testBoundsGreaterThanDefault() throws Exception {
    final int temp = conf.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    try {
      conf.setInt(NUM_REGION_GROUPS, temp*4);
      final String parallelism = Integer.toString(temp*4);
      int errCode = WALPerformanceEvaluation.innerMain(new Configuration(conf),
          new String [] {"-threads", parallelism, "-verify", "-noclosefs", "-iterations", "3000",
              "-regions", parallelism});
      assertEquals(0, errCode);
    } finally {
      conf.setInt(NUM_REGION_GROUPS, temp);
    }
  }

  @Test
  public void testMoreRegionsThanBoundWithBoundsGreaterThanDefault() throws Exception {
    final int temp = conf.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    try {
      conf.setInt(NUM_REGION_GROUPS, temp*4);
      final String parallelism = Integer.toString(temp*4*2);
      int errCode = WALPerformanceEvaluation.innerMain(new Configuration(conf),
          new String [] {"-threads", parallelism, "-verify", "-noclosefs", "-iterations", "3000",
              "-regions", parallelism});
      assertEquals(0, errCode);
    } finally {
      conf.setInt(NUM_REGION_GROUPS, temp);
    }
  }

  /**
   * Ensure that we can use Set.add to deduplicate WALs
   */
  @Test
  public void setMembershipDedups() throws IOException {
    final int temp = conf.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    WALFactory wals = null;
    try {
      conf.setInt(NUM_REGION_GROUPS, temp*4);
      wals = new WALFactory(conf, null, currentTest.getMethodName());
      final Set<WAL> seen = new HashSet<WAL>(temp*4);
      final Random random = new Random();
      int count = 0;
      // we know that this should see one of the wals more than once
      for (int i = 0; i < temp*8; i++) {
        final WAL maybeNewWAL = wals.getWAL(Bytes.toBytes(random.nextInt()));
        LOG.info("Iteration " + i + ", checking wal " + maybeNewWAL);
        if (seen.add(maybeNewWAL)) {
          count++;
        }
      }
      assertEquals("received back a different number of WALs that are not equal() to each other " +
          "than the bound we placed.", temp*4, count);
    } finally {
      if (wals != null) {
        wals.close();
      }
      conf.setInt(NUM_REGION_GROUPS, temp);
    }
  }
}
