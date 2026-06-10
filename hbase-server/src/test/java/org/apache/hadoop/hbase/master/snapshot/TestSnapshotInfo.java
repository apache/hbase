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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestSnapshotInfo {
  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Path rootDir;
  private FileSystem fs;
  private Configuration conf;
  private Admin admin;
  private String currentTestName;

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    TEST_UTIL.startMiniCluster(1);
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    admin = TEST_UTIL.getAdmin();
    currentTestName = testInfo.getTestMethod().get().getName();
  }

  @AfterEach
  public void tearDown() throws IOException {
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testGetSnapshotList() throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);

    // HBase cluster has just started, .hbase-snapshot directory doesn't exist
    assertFalse(fs.exists(snapshotDir));
    List<SnapshotDescription> snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertTrue(snapshotDescList.isEmpty());

    TableName tableName = TableName.valueOf(currentTestName);
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));
    String snapshotName = "snapshot_" + currentTestName;
    admin.snapshot(snapshotName, tableName);

    // .hbase-snapshot directory exists after snapshotting a table
    assertTrue(fs.exists(snapshotDir));
    snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertFalse(snapshotDescList.isEmpty());

    // Deleting snapshot and cluster has no snapshots, .hbase-snapshot directory would still exist
    admin.deleteSnapshot(snapshotName);
    assertTrue(fs.exists(snapshotDir));
    snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertTrue(snapshotDescList.isEmpty());
  }
}
