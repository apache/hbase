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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
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

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRSSnapshotVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(TestRSSnapshotVerifier.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSSnapshotVerifier.class);

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
    TEST_UTIL.startMiniCluster(3);
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(tableName, cf, splitKeys);
    TEST_UTIL.loadTable(table, cf, false);
    TEST_UTIL.getAdmin().flush(tableName);

    // prepare unverified snapshot
    Configuration conf = TEST_UTIL.getConfiguration();
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

  @Test(expected = org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException.class)
  public void testVerifyStoreFile() throws Exception {
    RSSnapshotVerifier verifier = TEST_UTIL
      .getHBaseCluster().getRegionServer(0).getRsSnapshotVerifier();
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(tableName).stream()
      .filter(r -> !r.getStore(cf).getStorefiles().isEmpty()).findFirst().get();
    Path filePath = new ArrayList<>(region.getStore(cf).getStorefiles()).get(0).getPath();
    TEST_UTIL.getDFSCluster().getFileSystem().delete(filePath, true);
    LOG.info("delete store file {}", filePath);
    verifier.verifyRegion(snapshotProto, region.getRegionInfo());
  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
