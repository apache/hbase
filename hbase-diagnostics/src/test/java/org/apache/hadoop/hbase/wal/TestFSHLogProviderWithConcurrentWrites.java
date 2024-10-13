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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for TestFSHLogProvider which use WALPerformanceEvaluation for WAL data creation. This class
 * was created as part of refactoring for hbase-diagnostics module creation in HBASE-28432 to break
 * cyclic dependency.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLogProviderWithConcurrentWrites {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFSHLogProviderWithConcurrentWrites.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestFSHLogProviderWithConcurrentWrites.class);

  private static FileSystem fs;
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.startMiniDFSCluster(3);

    // Set up a working space for our tests.
    TEST_UTIL.createRootDir();
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
    int errCode =
      WALPerformanceEvaluation.innerMain(new Configuration(TEST_UTIL.getConfiguration()),
        new String[] { "-threads", "3", "-verify", "-noclosefs", "-iterations", "3000" });
    assertEquals(0, errCode);
  }
}
