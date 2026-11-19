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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobFileType {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobFileType.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName tableName = TableName.valueOf("TEST_MOB_FILE_TYPE");
  private static final String snapshotName = "table_snapshot";
  private static final String family = "info";
  private static final String qf = "q";
  private static Admin admin;

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getAdmin();
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(family)).setMobEnabled(true).setMobThreshold(3L).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));
  }

  @After
  public void tearDown() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    boolean snapshotExist =
      admin.listSnapshots().stream().anyMatch(s -> snapshotName.equals(s.getName()));
    if (snapshotExist) {
      admin.deleteSnapshot(snapshotName);
    }
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testMobFileType() throws IOException {
    Table table = admin.getConnection().getTable(tableName);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qf), makeDummyData(600));
    table.put(put);
    admin.flush(tableName);

    Path mobDirPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    FileStatus[] fileStatuses = fs.listStatus(mobDirPath);
    assertEquals(1, fileStatuses.length);

    Path mobFile = fileStatuses[0].getPath();
    assertTrue(StoreFileInfo.isMobFile(mobFile));
    assertFalse(StoreFileInfo.isMobRefFile(mobFile));

    // snapshot table
    admin.snapshot(snapshotName, tableName);
    boolean snapshotResult =
      admin.listSnapshots().stream().anyMatch(s -> snapshotName.equals(s.getName()));
    assertTrue(snapshotResult);

    // delete table and restore it by snapshot
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    assertFalse(admin.tableExists(tableName));
    admin.restoreSnapshot(snapshotName);
    assertTrue(admin.tableExists(tableName));

    fileStatuses = fs.listStatus(mobDirPath);
    assertEquals(1, fileStatuses.length);
    mobFile = fileStatuses[0].getPath();
    assertFalse(StoreFileInfo.isMobFile(mobFile));
    assertTrue(StoreFileInfo.isMobRefFile(mobFile));
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }
}
