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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, LargeTests.class })
public class TestSnapshotStats {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotStats.class);

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Admin admin;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getAdmin();
  }

  @After
  public void tearDown() throws IOException {
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testSnapshotTableWithoutAnyData() throws IOException {
    // Create a table without any data
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));

    String snapshotName = "snapshot_" + name.getMethodName();
    admin.snapshot(snapshotName, tableName);

    Optional<SnapshotDescription> optional =
      admin.listSnapshots().stream().filter(s -> snapshotName.equals(s.getName())).findFirst();
    assertTrue(optional.isPresent());
    SnapshotDescription snapshotDescription = optional.get();

    SnapshotInfo.SnapshotStats snapshotStats =
      SnapshotInfo.getSnapshotStats(TEST_UTIL.getConfiguration(), snapshotDescription);
    assertEquals(0L, snapshotStats.getStoreFilesSize());
    assertEquals(0, snapshotStats.getSharedStoreFilePercentage(), 0);

    admin.deleteSnapshot(snapshotName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testSnapshotMobTableWithoutAnyData() throws IOException {
    // Create a MOB table without any data
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).setMobEnabled(true).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));

    String snapshotName = "snapshot_" + name.getMethodName();
    admin.snapshot(snapshotName, tableName);

    Optional<SnapshotDescription> optional =
      admin.listSnapshots().stream().filter(s -> snapshotName.equals(s.getName())).findFirst();
    assertTrue(optional.isPresent());
    SnapshotDescription snapshotDescription = optional.get();

    SnapshotInfo.SnapshotStats snapshotStats =
      SnapshotInfo.getSnapshotStats(TEST_UTIL.getConfiguration(), snapshotDescription);
    assertEquals(0L, snapshotStats.getStoreFilesSize());
    assertEquals(0, snapshotStats.getSharedStoreFilePercentage(), 0);
    assertEquals(0, snapshotStats.getMobStoreFilePercentage(), 0);

    admin.deleteSnapshot(snapshotName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
