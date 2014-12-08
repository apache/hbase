/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestTableDeleteFamilyHandler {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLENAME =
      TableName.valueOf("column_family_handlers");
  private static final byte[][] FAMILIES = new byte[][] { Bytes.toBytes("cf1"),
      Bytes.toBytes("cf2"), Bytes.toBytes("cf3") };

  /**
   * Start up a mini cluster and put a small table of empty regions into it.
   * 
   * @throws Exception
   */
  @BeforeClass
  public static void beforeAllTests() throws Exception {

    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.startMiniCluster(2);

    // Create a table of three families. This will assign a region.
    TEST_UTIL.createTable(TABLENAME, FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    while(TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionsInTransition().size() > 0) {
      Thread.sleep(100);
    }
    // Create multiple regions in all the three column families
    while(TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionsInTransition().size() > 0) {
      Thread.sleep(100);
    }
    // Load the table with data for all families
    TEST_UTIL.loadTable(t, FAMILIES);

    TEST_UTIL.flush();

    t.close();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.deleteTable(TABLENAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  @Test
  public void deleteColumnFamilyWithMultipleRegions() throws Exception {

    Admin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor beforehtd = admin.getTableDescriptor(TABLENAME);

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();

    // 1 - Check if table exists in descriptor
    assertTrue(admin.isTableAvailable(TABLENAME));

    // 2 - Check if all three families exist in descriptor
    assertEquals(3, beforehtd.getColumnFamilies().length);
    HColumnDescriptor[] families = beforehtd.getColumnFamilies();
    for (int i = 0; i < families.length; i++) {

      assertTrue(families[i].getNameAsString().equals("cf" + (i + 1)));
    }

    // 3 - Check if table exists in FS
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), TABLENAME);
    assertTrue(fs.exists(tableDir));

    // 4 - Check if all the 3 column families exist in FS
    FileStatus[] fileStatus = fs.listStatus(tableDir);
    for (int i = 0; i < fileStatus.length; i++) {
      if (fileStatus[i].isDirectory() == true) {
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath());
        int k = 1;
        for (int j = 0; j < cf.length; j++) {
          if (cf[j].isDirectory() == true
              && cf[j].getPath().getName().startsWith(".") == false) {
            assertEquals(cf[j].getPath().getName(), "cf" + k);
            k++;
          }
        }
      }
    }

    // TEST - Disable and delete the column family
    admin.disableTable(TABLENAME);
    admin.deleteColumn(TABLENAME, Bytes.toBytes("cf2"));

    // 5 - Check if only 2 column families exist in the descriptor
    HTableDescriptor afterhtd = admin.getTableDescriptor(TABLENAME);
    assertEquals(2, afterhtd.getColumnFamilies().length);
    HColumnDescriptor[] newFamilies = afterhtd.getColumnFamilies();
    assertTrue(newFamilies[0].getNameAsString().equals("cf1"));
    assertTrue(newFamilies[1].getNameAsString().equals("cf3"));

    // 6 - Check if the second column family is gone from the FS
    fileStatus = fs.listStatus(tableDir);
    for (int i = 0; i < fileStatus.length; i++) {
      if (fileStatus[i].isDirectory() == true) {
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath());
        for (int j = 0; j < cf.length; j++) {
          if (cf[j].isDirectory() == true) {
            assertFalse(cf[j].getPath().getName().equals("cf2"));
          }
        }
      }
    }
  }

}
