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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
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

@Category({ RegionServerTests.class, MediumTests.class })
public class TestSnapshotVerifyProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotVerifyProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotVerifyProcedure.class);

  private HBaseTestingUtil TEST_UTIL;
  private final TableName tableName = TableName.valueOf("TestRSSnapshotVerifier");
  private final byte[] cf = Bytes.toBytes("cf");
  private final SnapshotDescription snapshot =
    new SnapshotDescription("test-snapshot", tableName, SnapshotType.FLUSH);
  private SnapshotProtos.SnapshotDescription snapshotProto =
    ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration conf = TEST_UTIL.getConfiguration();
    // delay procedure dispatch
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(tableName, cf, splitKeys);
    TEST_UTIL.loadTable(table, cf, false);
    TEST_UTIL.getAdmin().flush(tableName);

    // prepare unverified snapshot
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotProto, rootDir, conf);
    FileSystem workingDirFs = workingDir.getFileSystem(conf);
    if (!workingDirFs.exists(workingDir)) {
      workingDirFs.mkdirs(workingDir);
    }
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());
    SnapshotManifest manifest = SnapshotManifest
      .create(conf, workingDirFs, workingDir, snapshotProto, monitor);
    manifest.addTableDescriptor(TEST_UTIL.getHBaseCluster()
      .getMaster().getTableDescriptors().get(tableName));
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
  public void testSimpleVerify() throws Exception {
    Optional<HRegion> regionOpt = TEST_UTIL.getHBaseCluster().getRegions(tableName)
      .stream().filter(r -> !r.getStore(cf).getStorefiles().isEmpty()).findFirst();
    Assert.assertTrue(regionOpt.isPresent());
    HRegion region = regionOpt.get();
    SnapshotVerifyProcedure p1 = new SnapshotVerifyProcedure(snapshotProto, region.getRegionInfo());
    ProcedureExecutor<MasterProcedureEnv> procExec =
      TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    long procId = procExec.submitProcedure(p1);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Assert.assertTrue(p1.isSuccess());

    // delete store file to trigger a CorruptedSnapshotException
    for (HStoreFile file : region.getStore(cf).getStorefiles()) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(file.getPath(), true);
      LOG.info("delete store file {}", file.getPath());
    }
    SnapshotVerifyProcedure p2 = new SnapshotVerifyProcedure(snapshotProto, region.getRegionInfo());
    long newProcId = procExec.submitProcedure(p2);
    ProcedureTestingUtility.waitProcedure(procExec, newProcId);
    Assert.assertTrue(p2.isSuccess());
  }

  @Test
  public void testRestartMaster() throws Exception {
    RegionInfo region = TEST_UTIL.getHBaseCluster().getRegions(tableName).get(0).getRegionInfo();
    SnapshotVerifyProcedure svp = new SnapshotVerifyProcedure(snapshotProto, region);
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long procId = master.getMasterProcedureExecutor().submitProcedure(svp);
    TEST_UTIL.waitFor(10000, () -> svp.getServerName() != null);
    ServerName worker = svp.getServerName();
    int availableWorker = master.getSnapshotManager().getAvailableWorker(worker);

    // restart master
    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 30000);
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();

    // restore used worker
    master = TEST_UTIL.getHBaseCluster().getMaster();
    SnapshotVerifyProcedure svp2 = master.getMasterProcedureExecutor()
      .getProcedure(SnapshotVerifyProcedure.class, procId);
    Assert.assertNotNull(svp2);
    Assert.assertFalse(svp2.isFinished());
    Assert.assertNotNull(svp2.getServerName());
    Assert.assertEquals(worker, svp.getServerName());
    Assert.assertEquals((int) master.getSnapshotManager().getAvailableWorker(worker),
      availableWorker);

    // release worker
    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), svp2);
    Assert.assertEquals((int) master.getSnapshotManager().getAvailableWorker(worker),
      availableWorker + 1);
  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
