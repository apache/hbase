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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore.MASTER_PROCEDURE_LOGDIR;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, MediumTests.class})
public class TestMetaInitIfAllProceduresLost {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaInitIfAllProceduresLost.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMetaInitIfAllProceduresLost.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void test() throws Exception {
    for (JVMClusterUtil.RegionServerThread rst : UTIL.getMiniHBaseCluster()
        .getRegionServerThreads()) {
      rst.getRegionServer().abort("killAll");
    }
    //wait for a while, until all dirs are changed to '-splitting'
    UTIL.waitFor(30000, () ->
        UTIL.getMiniHBaseCluster().getMaster().getMasterWalManager()
          .getLiveServersFromWALDir().size() == 0);
    Thread.sleep(1000);
    Path procedureWals = new Path(
        UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem()
            .getRootDir(), MASTER_PROCEDURE_LOGDIR);
    //Kill the master
    UTIL.getMiniHBaseCluster().killAll();
    //Delte all procedure log to create an anomaly
    for (FileStatus file : UTIL.getTestFileSystem().listStatus(procedureWals)) {
      LOG.info("removing " + file);
      UTIL.getTestFileSystem().delete(file.getPath());
    }
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.getMiniHBaseCluster().startRegionServer();
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // Schedule an assign of meta after ten seconds. Then a few seconds later, do namespace assign.
    // The meta table needs to be online before the namespace can succeed.
    final HMaster master = UTIL.getHBaseCluster().getMaster();
    final AssignmentManager am = master.getAssignmentManager();
    final AssignProcedure ap = am.createAssignProcedure(RegionInfoBuilder.FIRST_META_REGIONINFO);
    scheduler.schedule(() -> master.getMasterProcedureExecutor().submitProcedure(ap), 10,
        TimeUnit.SECONDS);
    scheduler.schedule(() -> {
      // hbase:meta should be online by the time this runs. That means we should have read the
      // regions that make up the namespace table so below query should return results.
      List<RegionInfo> ris = am.getRegionStates().getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME);
      if (ris.isEmpty()) {
        throw new RuntimeException("No namespace regions found!");
      }
      for (RegionInfo ri: ris) {
        AssignProcedure riap = am.createAssignProcedure(ri);
        master.getMasterProcedureExecutor().submitProcedure(riap);
      }
    }, 20 /*Must run AFTER meta is online*/, TimeUnit.SECONDS);
    // Master should able to finish init even if all procedures are lost
    UTIL.waitFor(180000, () -> UTIL.getMiniHBaseCluster().getMaster() != null && UTIL
      .getMiniHBaseCluster().getMaster().isInitialized());
  }
}
