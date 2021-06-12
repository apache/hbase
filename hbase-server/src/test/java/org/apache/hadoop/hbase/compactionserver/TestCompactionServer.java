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
package org.apache.hadoop.hbase.compactionserver;

import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.compaction.CompactionOffloadManager;
import org.apache.hadoop.hbase.testclassification.CompactionServerTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({CompactionServerTests.class, MediumTests.class})
public class TestCompactionServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactionServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionServer.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration CONF = TEST_UTIL.getConfiguration();
  private static HMaster MASTER;
  private static HCompactionServer COMPACTION_SERVER;
  private static ServerName COMPACTION_SERVER_NAME;
  private static TableName TABLENAME = TableName.valueOf("t");
  private static String FAMILY = "C";
  private static String COL ="c0";

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numCompactionServers(1).build());
    TEST_UTIL.getAdmin().switchCompactionOffload(true);
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    COMPACTION_SERVER = TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0)
      .getCompactionServer();
    COMPACTION_SERVER_NAME = COMPACTION_SERVER.getServerName();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME);
  }

  @After
  public void after() throws IOException {
    TEST_UTIL.deleteTableIfAny(TABLENAME);
  }


  @Test
  public void testCompactionServerReport() throws Exception {
    CompactionOffloadManager compactionOffloadManager = MASTER.getCompactionOffloadManager();
    TEST_UTIL.waitFor(60000, () -> !compactionOffloadManager.getOnlineServers().isEmpty()
      && null != compactionOffloadManager.getOnlineServers().get(COMPACTION_SERVER_NAME));
    // invoke compact
    TEST_UTIL.compact(TABLENAME, false);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.rpcServices.requestCount.sum() > 0
        && COMPACTION_SERVER.rpcServices.requestCount.sum() == compactionOffloadManager
        .getOnlineServers().get(COMPACTION_SERVER_NAME).getTotalNumberOfRequests());
  }

  @Test
  public void testCompactionServerExpire() throws Exception {
    int initialNum = TEST_UTIL.getMiniHBaseCluster().getNumLiveCompactionServers();
    CONF.setInt(HConstants.COMPACTION_SERVER_PORT, HConstants.DEFAULT_COMPACTION_SERVER_PORT + 1);
    CONF.setInt(HConstants.COMPACTION_SERVER_INFO_PORT,
      HConstants.DEFAULT_COMPACTION_SERVER_INFOPORT + 1);
    HCompactionServer compactionServer = new HCompactionServer(CONF);
    compactionServer.start();
    ServerName compactionServerName = compactionServer.getServerName();

    CompactionOffloadManager compactionOffloadManager = MASTER.getCompactionOffloadManager();
    TEST_UTIL.waitFor(60000,
      () -> initialNum + 1 == compactionOffloadManager.getOnlineServersList().size()
          && null != compactionOffloadManager.getLoad(compactionServerName));

    compactionServer.stop("test");

    TEST_UTIL.waitFor(60000,
      () -> initialNum == compactionOffloadManager.getOnlineServersList().size());
    assertNull(compactionOffloadManager.getLoad(compactionServerName));
  }
}
