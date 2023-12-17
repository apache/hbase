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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.CompactedHFilesDischarger;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestManageTableErasureCodingPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManageTableErasureCodingPolicy.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("a");
  private static final TableName NON_EC_TABLE = TableName.valueOf("foo");
  private static final TableDescriptor NON_EC_TABLE_DESC = TableDescriptorBuilder
    .newBuilder(NON_EC_TABLE).setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
  private static final TableName EC_TABLE = TableName.valueOf("bar");
  private static final TableDescriptor EC_TABLE_DESC =
    TableDescriptorBuilder.newBuilder(EC_TABLE).setErasureCodingPolicy("RS-6-3-1024k")
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniDFSCluster(6); // 6 necessary for RS-6-3-1024k
    UTIL.startMiniCluster(1);
    Table table = UTIL.createTable(NON_EC_TABLE_DESC, null);
    UTIL.loadTable(table, FAMILY);
    UTIL.flush();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void itValidatesPolicyNameForCreate() {
    HBaseIOException thrown = assertThrows(HBaseIOException.class, () -> {
      try (Admin admin = UTIL.getAdmin()) {
        admin.createTable(
          TableDescriptorBuilder.newBuilder(EC_TABLE_DESC).setErasureCodingPolicy("foo").build());
      }
    });
    assertThat(thrown.getMessage(),
      containsString("Cannot set Erasure Coding policy: foo. Policy not found"));

    thrown = assertThrows(HBaseIOException.class, () -> {
      try (Admin admin = UTIL.getAdmin()) {
        admin.createTable(TableDescriptorBuilder.newBuilder(EC_TABLE_DESC)
          .setErasureCodingPolicy("RS-10-4-1024k").build());
      }
    });
    assertThat(thrown.getMessage(), containsString(
      "Cannot set Erasure Coding policy: RS-10-4-1024k. The policy must be enabled"));
  }

  @Test
  public void itValidatesPolicyNameForAlter() {
    HBaseIOException thrown = assertThrows(HBaseIOException.class, () -> {
      try (Admin admin = UTIL.getAdmin()) {
        TableDescriptor desc = UTIL.getAdmin().getDescriptor(NON_EC_TABLE);
        admin.modifyTable(
          TableDescriptorBuilder.newBuilder(desc).setErasureCodingPolicy("foo").build());
      }
    });
    assertThat(thrown.getMessage(),
      containsString("Cannot set Erasure Coding policy: foo. Policy not found"));

    thrown = assertThrows(HBaseIOException.class, () -> {
      try (Admin admin = UTIL.getAdmin()) {
        TableDescriptor desc = UTIL.getAdmin().getDescriptor(NON_EC_TABLE);
        admin.modifyTable(
          TableDescriptorBuilder.newBuilder(desc).setErasureCodingPolicy("RS-10-4-1024k").build());
      }
    });
    assertThat(thrown.getMessage(), containsString(
      "Cannot set Erasure Coding policy: RS-10-4-1024k. The policy must be enabled"));
  }

  @Test
  public void testCreateTableErasureCodingSync() throws IOException {
    try (Admin admin = UTIL.getAdmin(); Table table = UTIL.getConnection().getTable(EC_TABLE)) {
      admin.createTable(EC_TABLE_DESC);
      UTIL.loadTable(table, FAMILY);
      UTIL.flush(EC_TABLE);
      Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(UTIL.getConfiguration());
      checkRegionDirAndFilePolicies(dfs, rootDir, EC_TABLE, "RS-6-3-1024k", "RS-6-3-1024k");
    }
  }

  @Test
  public void testModifyTableErasureCodingSync() throws IOException, InterruptedException {
    try (Admin admin = UTIL.getAdmin()) {
      Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(UTIL.getConfiguration());

      // start off without EC
      checkRegionDirAndFilePolicies(dfs, rootDir, NON_EC_TABLE, null, null);

      // add EC
      TableDescriptor desc = UTIL.getAdmin().getDescriptor(NON_EC_TABLE);
      TableDescriptor newDesc =
        TableDescriptorBuilder.newBuilder(desc).setErasureCodingPolicy("RS-6-3-1024k").build();
      admin.modifyTable(newDesc);

      // check dirs, but files should not be changed yet
      checkRegionDirAndFilePolicies(dfs, rootDir, NON_EC_TABLE, "RS-6-3-1024k", null);

      // compact to rewrite files with EC, then run discharger to get rid of the old non-EC files
      UTIL.compact(NON_EC_TABLE, true);
      for (JVMClusterUtil.RegionServerThread regionserver : UTIL.getHBaseCluster()
        .getLiveRegionServerThreads()) {
        CompactedHFilesDischarger chore =
          regionserver.getRegionServer().getCompactedHFilesDischarger();
        chore.setUseExecutor(false);
        chore.chore();
      }

      // expect both dirs and files to be EC now
      checkRegionDirAndFilePolicies(dfs, rootDir, NON_EC_TABLE, "RS-6-3-1024k", "RS-6-3-1024k");

      newDesc = TableDescriptorBuilder.newBuilder(newDesc).setErasureCodingPolicy(null).build();
      // remove EC now
      admin.modifyTable(newDesc);

      // dirs should no longer be EC, but old EC files remain
      checkRegionDirAndFilePolicies(dfs, rootDir, NON_EC_TABLE, null, "RS-6-3-1024k");

      // compact to rewrite EC files without EC, then run discharger to get rid of the old EC files
      UTIL.compact(NON_EC_TABLE, true);
      for (JVMClusterUtil.RegionServerThread regionserver : UTIL.getHBaseCluster()
        .getLiveRegionServerThreads()) {
        CompactedHFilesDischarger chore =
          regionserver.getRegionServer().getCompactedHFilesDischarger();
        chore.setUseExecutor(false);
        chore.chore();
      }

      checkRegionDirAndFilePolicies(dfs, rootDir, NON_EC_TABLE, null, null);
    }
  }

  private void checkRegionDirAndFilePolicies(DistributedFileSystem dfs, Path rootDir,
    TableName testTable, String expectedDirPolicy, String expectedFilePolicy) throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, testTable);
    checkPolicy(dfs, tableDir, expectedDirPolicy);

    int filesMatched = 0;
    for (HRegion region : UTIL.getHBaseCluster().getRegions(testTable)) {
      Path regionDir = new Path(tableDir, region.getRegionInfo().getEncodedName());
      checkPolicy(dfs, regionDir, expectedDirPolicy);
      RemoteIterator<LocatedFileStatus> itr = dfs.listFiles(regionDir, true);
      while (itr.hasNext()) {
        LocatedFileStatus fileStatus = itr.next();
        Path path = fileStatus.getPath();
        if (!HFile.isHFileFormat(dfs, path)) {
          continue;
        }
        filesMatched++;
        checkPolicy(dfs, path, expectedFilePolicy);
      }
    }
    assertThat(filesMatched, greaterThan(0));
  }

  private void checkPolicy(DistributedFileSystem dfs, Path path, String expectedPolicy)
    throws IOException {
    ErasureCodingPolicy policy = dfs.getErasureCodingPolicy(path);
    if (expectedPolicy == null) {
      assertThat(policy, nullValue());
    } else {
      assertThat("policy for " + path, policy, notNullValue());
      assertThat("policy for " + path, policy.getName(), equalTo(expectedPolicy));
    }
  }
}
