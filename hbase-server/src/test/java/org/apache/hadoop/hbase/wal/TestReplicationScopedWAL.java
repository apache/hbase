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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_GLOBAL;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestReplicationScopedWAL {
  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationScopedWAL.class);

  static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private final byte[] family1 = Bytes.toBytes("f1");
  private final byte[] family2 = Bytes.toBytes("f2");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration()
      .setBoolean(HConstants.REPLICATION_WAL_FILTER_BY_SCOPE_ENABLED, true);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReplicationScopedWAL() throws Exception {
    TableName tableName1 = TableName.valueOf("testReplicationScopedWAL1");
    TableName tableName2 = TableName.valueOf("testReplicationScopedWAL2");

    ColumnFamilyDescriptor column1 =
      ColumnFamilyDescriptorBuilder.newBuilder(family1).setScope(REPLICATION_SCOPE_GLOBAL).build();

    ColumnFamilyDescriptor column2 = ColumnFamilyDescriptorBuilder.of(family2);

    TableDescriptor tableDescriptor1 =
      TableDescriptorBuilder.newBuilder(tableName1).setColumnFamily(column1).build();
    TableDescriptor tableDescriptor2 =
      TableDescriptorBuilder.newBuilder(tableName2).setColumnFamily(column2).build();

    Table table1 = TEST_UTIL.createTable(tableDescriptor1, null);
    Table table2 = TEST_UTIL.createTable(tableDescriptor2, null);

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(family1, Bytes.toBytes("qualifier1"), Bytes.toBytes("value1"));

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(family2, Bytes.toBytes("qulifier2"), Bytes.toBytes("value2"));

    table1.put(put1);
    table2.put(put2);

    TEST_UTIL.flush();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path walRootDir = CommonFSUtils.getWALRootDir(TEST_UTIL.getConfiguration());
    FileStatus[] statusArray = fs.listStatus(walRootDir);
    Assert.assertTrue(statusArray.length >= 2);

    boolean hasReplicated = false;
    boolean hasNormal = false;
    for (FileStatus status : statusArray) {
      Path path = status.getPath();
      if (AbstractFSWALProvider.isFileInReplicationScope(path)) {
        hasReplicated = true;
      } else if (!AbstractFSWALProvider.isMetaFile(path)) {
        hasNormal = true;
      }
    }
    Assert.assertTrue(hasReplicated);
    Assert.assertTrue(hasNormal);

    table1.close();
    table2.close();
  }
}
