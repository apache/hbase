/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.snapshot;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotVerifyUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotVerifyUtil.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotVerifyUtil.class);

  private static final int MOCK_REGION_ID = 0;

  private static HBaseTestingUtil TEST_UTIL;
  private HMaster master;
  private TableName tableName;
  private Configuration conf;
  private String snapshotName;
  private byte[] CF;
  private SnapshotDescription snapshot;
  private SnapshotProtos.SnapshotDescription snapshotProto;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    tableName = TableName.valueOf(Bytes.toBytes("VerifyTestTable"));
    snapshotName = "SnapshotVerifyTest";
    CF = Bytes.toBytes("cf");
    snapshot = new SnapshotDescription(snapshotName, tableName,
      SnapshotType.FLUSH, null, -1, SnapshotManifestV2.DESCRIPTOR_VERSION, null);
    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());
  }

  @Test
  public void testVerifySnapshot() throws Exception {
    Table table = TEST_UTIL.createTable(tableName, CF);
    TEST_UTIL.loadTable(table, CF, false);
    TEST_UTIL.flush(tableName);

    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotProto, rootDir, conf);
    FileSystem workingDirFs = workingDir.getFileSystem(conf);
    if (!workingDirFs.exists(workingDir)) {
      workingDirFs.mkdirs(workingDir);
      LOG.info("create working dir {}", workingDir);
    }
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());
    SnapshotManifest manifest = SnapshotManifest
      .create(conf, workingDirFs, workingDir, snapshotProto, monitor);
    manifest.addTableDescriptor(master.getTableDescriptors().get(tableName));
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshotProto, workingDir, workingDirFs);
    TEST_UTIL.getHBaseCluster()
      .getRegions(tableName).get(0).addRegionToSnapshot(snapshotProto, monitor);
    manifest.consolidate();

    // make sure current is fine
    List<RegionInfo> regions = master
      .getAssignmentManager().getTableRegions(tableName, true);
    try {
      SnapshotVerifyUtil.verifySnapshot(conf, snapshotProto, tableName, regions, 1);
    } catch (CorruptedSnapshotException e) {
      Assert.fail("verify normal snapshots as false");
    }

    // test verify snapshot description
    SnapshotProtos.SnapshotDescription newSnapshotDescWithNewType = ProtobufUtil
      .createHBaseProtosSnapshotDesc(
        new SnapshotDescription(snapshotName, tableName, SnapshotType.SKIPFLUSH,
          null, -1, SnapshotManifestV2.DESCRIPTOR_VERSION, null));
    Assert.assertThrows(CorruptedSnapshotException.class, () -> {
      SnapshotVerifyUtil.verifySnapshot(conf, newSnapshotDescWithNewType, tableName, regions, 1);
    });

    // test verify table description
    TableName newTableName = TableName.valueOf("newTable");
    SnapshotProtos.SnapshotDescription snapshotDescWithNewTableName = ProtobufUtil
      .createHBaseProtosSnapshotDesc(
        new SnapshotDescription(snapshotName, newTableName, SnapshotType.FLUSH,
          null, -1, SnapshotManifestV2.DESCRIPTOR_VERSION, null));
    Assert.assertThrows(CorruptedSnapshotException.class, () -> {
      SnapshotVerifyUtil.verifySnapshot(conf, snapshotDescWithNewTableName, tableName, regions, 1);
    });

    // test verify num regions
    Assert.assertThrows(CorruptedSnapshotException.class, () -> {
      SnapshotVerifyUtil.verifySnapshot(conf, snapshotProto, tableName, regions, 2);
    });

    // test verify region info
    List<RegionInfo> newRegions = regions.stream().map(r -> RegionInfoBuilder.newBuilder(r).
      setRegionId(MOCK_REGION_ID).build()).collect(Collectors.toList());
    try {
      SnapshotVerifyUtil.verifySnapshot(conf, snapshotProto, tableName, newRegions, 1);
    } catch (CorruptedSnapshotException e) {
      // here may be a little confusing, even if we change the region info, it will not trigger the
      // CorruptedSnapshotException, just log the missing region info. see code for more details.
      Assert.fail("verify normal snapshots as false");
    }

    // test verify store files
    DistributedFileSystem dfs = TEST_UTIL.getDFSCluster().getFileSystem();
    TEST_UTIL.getHBaseCluster().getRegions(tableName).get(0)
      .getStoreFileList(new byte[][] { CF }).forEach(s -> {
      try {
        // delete real data file to trigger the CorruptedSnapshotException
        dfs.delete(new Path(s), true);
      } catch (Exception e) {
        LOG.warn("Failed delete {} to make snapshot corrupt", s, e);
      }
    });
    Assert.assertThrows(CorruptedSnapshotException.class, () -> {
      SnapshotVerifyUtil.verifySnapshot(conf, snapshotProto, tableName, regions, 1);
    });
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
