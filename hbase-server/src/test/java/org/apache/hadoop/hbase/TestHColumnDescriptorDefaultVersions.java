/**
 *
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.legacy.LegacyTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


/**
 * Verify that the HColumnDescriptor version is set correctly by default, hbase-site.xml, and user
 * input
 */
@Category({MiscTests.class, MediumTests.class})
public class TestHColumnDescriptorDefaultVersions {

  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static TableName TABLE_NAME = null;
  private static final byte[] FAMILY = Bytes.toBytes("cf0");

  /**
   * Start up a mini cluster and put a small table of empty regions into it.
   * @throws Exception
   */
  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @Before
  public void setup() {
    TABLE_NAME = TableName.valueOf(name.getMethodName());

  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTableWithDefault() throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    // Create a table with one family
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    baseHtd.addFamily(hcd);
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the column descriptor
      verifyHColumnDescriptor(1, TABLE_NAME, FAMILY);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testCreateTableWithDefaultFromConf() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().setInt("hbase.column.max.version", 3);
    TEST_UTIL.startMiniCluster(1);

    Admin admin = TEST_UTIL.getHBaseAdmin();
    // Create a table with one family
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMaxVersions(TEST_UTIL.getConfiguration().getInt("hbase.column.max.version", 1));
    baseHtd.addFamily(hcd);
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the column descriptor
      verifyHColumnDescriptor(3, TABLE_NAME, FAMILY);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testCreateTableWithSetVersion() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().setInt("hbase.column.max.version", 3);
    TEST_UTIL.startMiniCluster(1);

    Admin admin = TEST_UTIL.getHBaseAdmin();
    // Create a table with one family
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMaxVersions(5);
    baseHtd.addFamily(hcd);
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the column descriptor
      verifyHColumnDescriptor(5, TABLE_NAME, FAMILY);

    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testHColumnDescriptorCachedMaxVersions() throws Exception {
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMaxVersions(5);
    // Verify the max version
    assertEquals(5, hcd.getMaxVersions());

    // modify the max version
    hcd.setValue(Bytes.toBytes(HConstants.VERSIONS), Bytes.toBytes("8"));
    // Verify the max version
    assertEquals(8, hcd.getMaxVersions());
  }

  private void verifyHColumnDescriptor(int expected, final TableName tableName,
      final byte[]... families) throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();

    // Verify descriptor from master
    HTableDescriptor htd = admin.getTableDescriptor(tableName);
    HColumnDescriptor[] hcds = htd.getColumnFamilies();
    verifyHColumnDescriptor(expected, hcds, tableName, families);

    // Verify descriptor from HDFS
    MasterStorage mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterStorage();
    Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), tableName);
    HTableDescriptor td = LegacyTableDescriptor.getTableDescriptorFromFs(mfs.getFileSystem(), tableDir);
    hcds = td.getColumnFamilies();
    verifyHColumnDescriptor(expected, hcds, tableName, families);
  }

  private void verifyHColumnDescriptor(int expected, final HColumnDescriptor[] hcds,
      final TableName tableName,
      final byte[]... families) {
    for (HColumnDescriptor hcd : hcds) {
      assertEquals(expected, hcd.getMaxVersions());
    }
  }

}
