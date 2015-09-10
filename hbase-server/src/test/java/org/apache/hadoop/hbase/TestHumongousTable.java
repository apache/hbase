/*
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.layout.FsLayout;
import org.apache.hadoop.hbase.fs.layout.HierarchicalFsLayout;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HierarchicalHRegionFileSystem;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, MiscTests.class})
public class TestHumongousTable {
  protected static final Log LOG = LogFactory.getLog(TestHumongousTable.class);
  protected final static int NUM_SLAVES_BASE = 4;
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration CONF;
  protected static HBaseAdmin ADMIN;
  protected static FileSystem FS;

  @BeforeClass
  public static void setUp() throws Exception {
    FsLayout.setLayoutForTesting(HierarchicalFsLayout.get());
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE);
    ADMIN = TEST_UTIL.getHBaseAdmin();
    LOG.info("Done initializing cluster");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      TEST_UTIL.shutdownMiniCluster();
    } finally {
      FsLayout.reset();
    }
  }

  @Before
  public void beforeMethod() throws IOException {
    for (HTableDescriptor desc : ADMIN.listTables(".*")) {
      ADMIN.disableTable(desc.getTableName());
      ADMIN.deleteTable(desc.getTableName());
    }
  }

  @Test(timeout = 60000)
  public void testCreateHumongousTable() throws IOException, InterruptedException {
    // create a humongous table with splits
    String tableNameStr = "testCreateHumongousTable";
    TableName tableName = TableName.valueOf(tableNameStr);
    String familyName = "col";
    TableName testTable = TableName.valueOf(tableNameStr);
    HTableDescriptor desc = new HTableDescriptor(testTable);
    HColumnDescriptor family = new HColumnDescriptor(familyName);
    desc.addFamily(family);
    ADMIN.createTable(desc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);

    // check tableDir and table descriptor
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), testTable);
    FS = TEST_UTIL.getTestFileSystem();
    assertTrue(FS.exists(tableDir));

    // load table with rows and flush stores
    Connection connection = TEST_UTIL.getConnection();
    Table table = connection.getTable(testTable);
    int rowCount = TEST_UTIL.loadTable(table, Bytes.toBytes(familyName));
    ADMIN.flush(tableName);
    assertEquals(TEST_UTIL.countRows(table), rowCount);
    
    verifyColumnFamilies(desc, testTable, familyName);
    
    // test alteration of humongous table too
    String familyName2 = "col2";
    HColumnDescriptor family2 = new HColumnDescriptor(familyName2);
    ADMIN.addColumnFamily(tableName, family2);
    
    // Wait for async add column to finish
    Thread.sleep(5000);
    
    TEST_UTIL.loadTable(table, Bytes.toBytes(familyName2));
    ADMIN.flush(tableName);
    
    verifyColumnFamilies(desc, testTable, familyName, familyName2);
    
    // drop the test table
    ADMIN.disableTable(testTable);
    assertTrue(ADMIN.isTableDisabled(testTable));
    ADMIN.deleteTable(testTable);
    assertEquals(ADMIN.getTableRegions(testTable), null);
    assertFalse(FS.exists(tableDir));
  }
  
  private void verifyColumnFamilies(HTableDescriptor desc, TableName testTable, String... colFamNames) throws IOException {
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), testTable);
    
    List<HRegionInfo> tableRegions = ADMIN.getTableRegions(testTable);
    
    // check region dirs and files on fs
    for (HRegionInfo hri : tableRegions) {
      // check region dir structure
      HierarchicalHRegionFileSystem hrfs = (HierarchicalHRegionFileSystem) HRegionFileSystem.openRegionFromFileSystem(
        CONF, FS, tableDir, hri, true);
      
      Path humongousRegionDir = hrfs.getRegionDir();
      Path normalRegionDir = hrfs.getStandadHBaseRegionDir();
      
      assertTrue(FS.exists(humongousRegionDir));
      assertFalse(FS.exists(normalRegionDir));
      
      String bucket = hri.getEncodedName().substring(
          HRegionInfo.MD5_HEX_LENGTH
              - HRegionFileSystem.HUMONGOUS_DIR_NAME_SIZE);
      assertEquals(humongousRegionDir.getParent().getName(), bucket);
      
      FileStatus[] statList = FS.listStatus(humongousRegionDir);
      Set<String> contents = new HashSet<String>();
      
      LOG.debug("Contents of humongous region dir: " + contents);
      
      for (FileStatus stat : statList) {
        contents.add(stat.getPath().getName());
      }
      
      assertTrue(contents.contains(HRegionFileSystem.REGION_INFO_FILE));
      assertTrue(contents.contains(HConstants.HBASE_TEMP_DIRECTORY));
      assertTrue(contents.contains(HConstants.RECOVERED_EDITS_DIR));
      
      for (String colFam : colFamNames) {
        assertTrue(contents.contains(colFam));

        // familyDir has one store file
        Path famPath = new Path(humongousRegionDir, colFam);
        assertEquals(1, FS.listStatus(famPath).length);
      }
      
      assertEquals("Contents: " + contents + " and fam names: " + Arrays.toString(colFamNames), 
        3 + colFamNames.length, contents.size());
    }
  }
}
