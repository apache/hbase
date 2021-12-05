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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotVerifyProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotVerifyProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotVerifyProcedure.class);

  private HBaseTestingUtil TEST_UTIL;
  private HMaster master;
  private List<RegionInfo> regions;
  private SnapshotProtos.SnapshotDescription snapshotProto;
  private ProcedureExecutor<MasterProcedureEnv> procExec;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.MASTER_INFO_PORT, 8080);
    // delay dispatch so that we can do something, for example kill a target server
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 20000);
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);

    // prepare runtime environment
    master = TEST_UTIL.getHBaseCluster().getMaster();
    procExec = master.getMasterProcedureExecutor();
    TableName tableName = TableName.valueOf(Bytes.toBytes("SVPTestTable"));
    byte[] cf = Bytes.toBytes("cf");
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(tableName, cf, splitKeys);
    regions = master.getAssignmentManager().getTableRegions(tableName, true);
    TEST_UTIL.loadTable(table, cf, false);

    // prepare snapshot info
    String snapshotName = "SnapshotVerifyProcedureTest";
    SnapshotDescription snapshot =
      new SnapshotDescription(snapshotName, tableName, SnapshotType.FLUSH);
    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());

    // prepare unverified data manifest
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotProto, rootDir, conf);
    FileSystem workingDirFs = workingDir.getFileSystem(conf);
    if (!workingDirFs.exists(workingDir)) {
      workingDirFs.mkdirs(workingDir);
    }
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());
    SnapshotManifest manifest = SnapshotManifest
      .create(conf, workingDirFs, workingDir, snapshotProto, monitor);
    manifest.addTableDescriptor(master.getTableDescriptors().get(tableName));
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshotProto, workingDir, workingDirFs);
    TEST_UTIL.getHBaseCluster()
      .getRegions(tableName).forEach(r -> {
      try {
        r.addRegionToSnapshot(snapshotProto, monitor);
      } catch (IOException e) {
        LOG.warn("Failed snapshot region {}", r.getRegionInfo());
      }
    });
    manifest.consolidate();
  }

  @Test
  public void testSimpleVerifySnapshot() throws Exception {
    ServerName targetServer = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    SnapshotVerifyProcedure svp1 = new SnapshotVerifyProcedure(snapshotProto,
      regions, targetServer, regions.size());
    long procId1 = procExec.submitProcedure(svp1);
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    Assert.assertFalse(svp1.isFailed());
    SnapshotVerifyProcedure svp2 = new SnapshotVerifyProcedure(snapshotProto,
      regions, targetServer, regions.size()+1);
    long procId2 = procExec.submitProcedure(svp2);
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    Assert.assertTrue(svp2.isFailed());
  }

  @Test
  public void testRegionServerCrashWhileVerifyingSnapshot() throws Exception {
    ServerName targetServer = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    SnapshotVerifyProcedure svp = new SnapshotVerifyProcedure(snapshotProto,
      regions, targetServer, regions.size());
    long procId = procExec.submitProcedure(svp);
    TEST_UTIL.getHBaseCluster().killRegionServer(svp.getServerName());
    TEST_UTIL.waitFor(180000, () -> !svp.getServerName().equals(targetServer));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Assert.assertFalse(svp.isFailed());
  }

  @After
  public void teardown() throws Exception {
    if (this.master != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        master.getMasterProcedureExecutor(), false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}
