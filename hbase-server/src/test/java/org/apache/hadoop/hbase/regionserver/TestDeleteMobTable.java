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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MediumTests.TAG)
public class TestDeleteMobTable {

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private final static byte[] FAMILY = Bytes.toBytes("family");
  private final static byte[] QF = Bytes.toBytes("qualifier");
  private String name;

  @BeforeEach
  public void setTestName(TestInfo testInfo) {
    this.name = testInfo.getTestMethod().get().getName();
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Generate the mob value. the size of the value
   * @return the mob value generated
   */
  private static byte[] generateMobValue(int size) {
    byte[] mobVal = new byte[size];
    Bytes.random(mobVal);
    return mobVal;
  }

  private TableDescriptor createTableDescriptor(TableName tableName, boolean hasMob) {
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
    if (hasMob) {
      builder.setMobEnabled(true);
      builder.setMobThreshold(0);
    }
    return TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(builder.build()).build();
  }

  private Table createTableWithOneFile(TableDescriptor tableDescriptor) throws IOException {
    Table table = TEST_UTIL.createTable(tableDescriptor, null);
    try {
      // insert data
      byte[] value = generateMobValue(10);
      byte[] row = Bytes.toBytes("row");
      Put put = new Put(row);
      put.addColumn(FAMILY, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put);

      // create an hfile
      TEST_UTIL.getAdmin().flush(tableDescriptor.getTableName());
    } catch (IOException e) {
      table.close();
      throw e;
    }
    return table;
  }

  @Test
  public void testDeleteMobTable() throws Exception {
    final TableName tableName = TableName.valueOf(name);
    TableDescriptor tableDescriptor = createTableDescriptor(tableName, true);
    ColumnFamilyDescriptor familyDescriptor = tableDescriptor.getColumnFamily(FAMILY);

    String fileName = null;
    Table table = createTableWithOneFile(tableDescriptor);
    try {
      // the mob file exists
      assertEquals(1, countMobFiles(tableName, familyDescriptor.getNameAsString()));
      assertEquals(0, countArchiveMobFiles(tableName, familyDescriptor.getNameAsString()));
      fileName = assertHasOneMobRow(table, tableName, familyDescriptor.getNameAsString());
      assertFalse(mobArchiveExist(tableName, familyDescriptor.getNameAsString(), fileName));
      assertTrue(mobTableDirExist(tableName));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }

    assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    assertEquals(0, countMobFiles(tableName, familyDescriptor.getNameAsString()));
    assertEquals(1, countArchiveMobFiles(tableName, familyDescriptor.getNameAsString()));
    assertTrue(mobArchiveExist(tableName, familyDescriptor.getNameAsString(), fileName));
    assertFalse(mobTableDirExist(tableName));
  }

  @Test
  public void testDeleteNonMobTable() throws Exception {
    final TableName tableName = TableName.valueOf(name);
    TableDescriptor htd = createTableDescriptor(tableName, false);
    ColumnFamilyDescriptor hcd = htd.getColumnFamily(FAMILY);

    Table table = createTableWithOneFile(htd);
    try {
      // the mob file doesn't exist
      assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
      assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
      assertFalse(mobTableDirExist(tableName));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }

    assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
    assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
    assertFalse(mobTableDirExist(tableName));
  }

  @Test
  public void testMobFamilyDelete() throws Exception {
    final TableName tableName = TableName.valueOf(name);
    TableDescriptor tableDescriptor = createTableDescriptor(tableName, true);
    ColumnFamilyDescriptor familyDescriptor = tableDescriptor.getColumnFamily(FAMILY);
    tableDescriptor = TableDescriptorBuilder.newBuilder(tableDescriptor)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("family2"))).build();

    Table table = createTableWithOneFile(tableDescriptor);
    try {
      // the mob file exists
      assertEquals(1, countMobFiles(tableName, familyDescriptor.getNameAsString()));
      assertEquals(0, countArchiveMobFiles(tableName, familyDescriptor.getNameAsString()));
      String fileName = assertHasOneMobRow(table, tableName, familyDescriptor.getNameAsString());
      assertFalse(mobArchiveExist(tableName, familyDescriptor.getNameAsString(), fileName));
      assertTrue(mobTableDirExist(tableName));

      TEST_UTIL.getAdmin().deleteColumnFamily(tableName, FAMILY);

      assertEquals(0, countMobFiles(tableName, familyDescriptor.getNameAsString()));
      assertEquals(1, countArchiveMobFiles(tableName, familyDescriptor.getNameAsString()));
      assertTrue(mobArchiveExist(tableName, familyDescriptor.getNameAsString(), fileName));
      assertFalse(mobColumnFamilyDirExist(tableName, familyDescriptor.getNameAsString()));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private int countMobFiles(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path mobFileDir = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName);
    if (fs.exists(mobFileDir)) {
      return fs.listStatus(mobFileDir).length;
    }
    return 0;
  }

  private int countArchiveMobFiles(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
      MobUtils.getMobRegionInfo(tn).getEncodedName(), familyName);
    if (fs.exists(storePath)) {
      return fs.listStatus(storePath).length;
    }
    return 0;
  }

  private boolean mobTableDirExist(TableName tn) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path tableDir =
      CommonFSUtils.getTableDir(MobUtils.getMobHome(TEST_UTIL.getConfiguration()), tn);
    return fs.exists(tableDir);
  }

  private boolean mobColumnFamilyDirExist(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path mobFamilyDir = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName);
    return fs.exists(mobFamilyDir);
  }

  private boolean mobArchiveExist(TableName tn, String familyName, String fileName)
    throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
      MobUtils.getMobRegionInfo(tn).getEncodedName(), familyName);
    return fs.exists(new Path(storePath, fileName));
  }

  private String assertHasOneMobRow(Table table, TableName tn, String familyName)
    throws IOException {
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner rs = table.getScanner(scan);
    Result r = rs.next();
    assertNotNull(r);
    String fileName = MobUtils.getMobFileName(r.getColumnLatestCell(FAMILY, QF));
    Path filePath =
      new Path(MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName), fileName);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    assertTrue(fs.exists(filePath));
    r = rs.next();
    assertNull(r);
    return fileName;
  }
}
