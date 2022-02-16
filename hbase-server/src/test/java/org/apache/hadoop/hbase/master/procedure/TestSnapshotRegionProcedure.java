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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.List;
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
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifestV2;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotRegionProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotRegionProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotRegionProcedure.class);

  private static HBaseTestingUtil TEST_UTIL;
  private HMaster master;
  private TableName tableName;
  private SnapshotProtos.SnapshotDescription snapshotProto;
  private Path workingDir;
  private FileSystem workingDirFs;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration conf = TEST_UTIL.getConfiguration();
    // disable info server. Info server is useful when we run unit tests locally, but it will
    // fails integration testing of jenkins.
    // conf.setInt(HConstants.MASTER_INFO_PORT, 8080);

    // delay dispatch so that we can do something, for example kill a target server
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    conf.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    tableName = TableName.valueOf(Bytes.toBytes("SRPTestTable"));
    byte[] cf = Bytes.toBytes("cf");
    String SNAPSHOT_NAME = "SnapshotRegionProcedureTest";
    SnapshotDescription snapshot =
      new SnapshotDescription(SNAPSHOT_NAME, tableName, SnapshotType.FLUSH);
    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(tableName, cf, splitKeys);
    TEST_UTIL.loadTable(table, cf, false);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotProto, rootDir, conf);
    this.workingDirFs = workingDir.getFileSystem(conf);
    if (!workingDirFs.exists(workingDir)) {
      workingDirFs.mkdirs(workingDir);
    }
  }

  private boolean assertRegionManifestGenerated(RegionInfo region) throws Exception {
    // path: /<root dir>/<snapshot dir>/<working dir>/<snapshot name>/region-manifest.<encode name>
    String regionManifest = SnapshotManifestV2.SNAPSHOT_MANIFEST_PREFIX + region.getEncodedName();
    Path targetPath = new Path(workingDir, regionManifest);
    return workingDirFs.exists(targetPath);
  }

  @Test
  public void testSimpleSnapshotRegion() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    List<Pair<RegionInfo, ServerName>> regions =
      master.getAssignmentManager().getTableRegionsAndLocations(tableName, true);
    assertEquals(10, regions.size());
    Pair<RegionInfo, ServerName> region = regions.get(0);
    SnapshotRegionProcedure srp = new SnapshotRegionProcedure(snapshotProto, region.getFirst());
    long procId = procExec.submitProcedure(srp);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertTrue(assertRegionManifestGenerated(region.getFirst()));
  }

  @Test
  public void testRegionServerCrashWhileTakingSnapshotRegion() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    List<Pair<RegionInfo, ServerName>> regions =
      master.getAssignmentManager().getTableRegionsAndLocations(tableName, true);
    assertEquals(10, regions.size());
    Pair<RegionInfo, ServerName> pair = regions.get(0);
    SnapshotRegionProcedure srp = new SnapshotRegionProcedure(snapshotProto, pair.getFirst());
    long procId = procExec.submitProcedure(srp);
    TEST_UTIL.getHBaseCluster().killRegionServer(pair.getSecond());
    TEST_UTIL.waitFor(60000, () -> !pair.getSecond().equals(master.getAssignmentManager()
      .getRegionStates().getRegionStateNode(pair.getFirst()).getRegionLocation()));
    TEST_UTIL.waitFor(60000, () -> srp.inRetrying());
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertTrue(assertRegionManifestGenerated(pair.getFirst()));
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
