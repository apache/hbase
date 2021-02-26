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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, LargeTests.class })
public class TestWALOpenAfterDNRollingStart {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALOpenAfterDNRollingStart.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Sleep time before restart next dn, we need to wait the current dn to finish start up
  private static long DN_RESTART_INTERVAL = 15000;

  // interval of checking low replication. The sleep time must smaller than
  // DataNodeRestartInterval
  // so a low replication case will be detected and the wal will be rolled
  private static long CHECK_LOW_REPLICATION_INTERVAL = 10000;

  @Parameter
  public String walProvider;

  @Parameters(name = "{index}: wal={0}")
  public static List<Object[]> data() {
    return Arrays.asList(new Object[] { "asyncfs" }, new Object[] { "filesystem" });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // don't let hdfs client to choose a new replica when dn down
    TEST_UTIL.getConfiguration()
        .setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", false);
    TEST_UTIL.getConfiguration().setLong("hbase.regionserver.hlog.check.lowreplication.interval",
      CHECK_LOW_REPLICATION_INTERVAL);
    TEST_UTIL.startMiniDFSCluster(3);
    TEST_UTIL.startMiniZKCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    TEST_UTIL.getConfiguration().set("hbase.wal.provider", walProvider);
    TEST_UTIL.startMiniHBaseCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniHBaseCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * see HBASE-18132 This is a test case of failing open a wal(for replication for example) after
   * all datanode restarted (rolling upgrade, for example). Before this patch, low replication
   * detection is only used when syncing wal. But if the wal haven't had any entry whiten, it will
   * never know all the replica of the wal is broken(because of dn restarting). And this wal can
   * never be open
   * @throws Exception
   */
  @Test
  public void test() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    AbstractFSWAL<?> wal = (AbstractFSWAL<?>) server.getWAL(null);
    Path currentFile = wal.getCurrentFileName();
    // restart every dn to simulate a dn rolling upgrade
    for (int i = 0, n = TEST_UTIL.getDFSCluster().getDataNodes().size(); i < n; i++) {
      // This is NOT a bug, when restart dn in miniDFSCluster, it will remove the stopped dn from
      // the dn list and then add to the tail of this list, we need to always restart the first one
      // to simulate rolling upgrade of every dn.
      TEST_UTIL.getDFSCluster().restartDataNode(0);
      // sleep enough time so log roller can detect the pipeline break and roll log
      Thread.sleep(DN_RESTART_INTERVAL);
    }

    if (!server.getFileSystem().exists(currentFile)) {
      Path walRootDir = CommonFSUtils.getWALRootDir(TEST_UTIL.getConfiguration());
      final Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      currentFile = new Path(oldLogDir, currentFile.getName());
    }
    // if the log is not rolled, then we can never open this wal forever.
    try (WAL.Reader reader = WALFactory.createReader(TEST_UTIL.getTestFileSystem(), currentFile,
      TEST_UTIL.getConfiguration())) {
      reader.next();
    }
  }
}
