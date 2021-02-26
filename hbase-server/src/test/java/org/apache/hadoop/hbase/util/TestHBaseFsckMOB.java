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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.hfile.TestHFile;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestHBaseFsckMOB extends BaseTestHBaseFsck {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseFsckMOB.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncCoprocessor.class.getName());

    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 30);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(1);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
      new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat("testhbck-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

    hbfsckExecutorService = new ScheduledThreadPoolExecutor(POOL_SIZE);

    AssignmentManager assignmentManager =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    regionStates = assignmentManager.getRegionStates();

    connection = (ClusterConnection) TEST_UTIL.getConnection();

    admin = connection.getAdmin();
    admin.setBalancerRunning(false, true);

    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.NAMESPACE_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    tableExecutorService.shutdown();
    hbfsckExecutorService.shutdown();
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    EnvironmentEdgeManager.reset();
  }


  /**
   * This creates a table and then corrupts a mob file.  Hbck should quarantine the file.
   */
  @Test
  public void testQuarantineCorruptMobFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    try {
      setupMobTable(table);
      assertEquals(ROWKEYS.length, countRows());
      admin.flush(table);

      FileSystem fs = FileSystem.get(conf);
      Path mobFile = getFlushedMobFile(fs, table);
      admin.disableTable(table);
      // create new corrupt mob file.
      String corruptMobFile = createMobFileName(mobFile.getName());
      Path corrupt = new Path(mobFile.getParent(), corruptMobFile);
      TestHFile.truncateFile(fs, mobFile, corrupt);
      LOG.info("Created corrupted mob file " + corrupt);
      HBaseFsck.debugLsr(conf, CommonFSUtils.getRootDir(conf));
      HBaseFsck.debugLsr(conf, MobUtils.getMobHome(conf));

      // A corrupt mob file doesn't abort the start of regions, so we can enable the table.
      admin.enableTable(table);
      HBaseFsck res = HbckTestingUtil.doHFileQuarantine(conf, table);
      assertEquals(0, res.getRetCode());
      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(4, hfcc.getHFilesChecked());
      assertEquals(0, hfcc.getCorrupted().size());
      assertEquals(0, hfcc.getFailures().size());
      assertEquals(0, hfcc.getQuarantined().size());
      assertEquals(0, hfcc.getMissing().size());
      assertEquals(5, hfcc.getMobFilesChecked());
      assertEquals(1, hfcc.getCorruptedMobFiles().size());
      assertEquals(0, hfcc.getFailureMobFiles().size());
      assertEquals(1, hfcc.getQuarantinedMobFiles().size());
      assertEquals(0, hfcc.getMissedMobFiles().size());
      String quarantinedMobFile = hfcc.getQuarantinedMobFiles().iterator().next().getName();
      assertEquals(corruptMobFile, quarantinedMobFile);
    } finally {
      cleanupTable(table);
    }
  }
}
