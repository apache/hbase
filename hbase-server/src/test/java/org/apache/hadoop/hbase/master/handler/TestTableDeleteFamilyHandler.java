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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, LargeTests.class})
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
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    // Create a table of three families. This will assign a region.
    TEST_UTIL.createTable(TABLENAME, FAMILIES);
    Table t = TEST_UTIL.getConnection().getTable(TABLENAME);
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

    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  @After
  public void cleanup() throws Exception {
    TEST_UTIL.deleteTable(TABLENAME);
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
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath(), new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().contains(HConstants.RECOVERED_EDITS_DIR)) {
              return false;
            }
            return true;
          }
        });
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
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath(), new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (WALSplitter.isSequenceIdFile(p)) {
              return false;
            }
            return true;
          }
        });
        for (int j = 0; j < cf.length; j++) {
          if (cf[j].isDirectory() == true) {
            assertFalse(cf[j].getPath().getName().equals("cf2"));
          }
        }
      }
    }
  }

  @Test
  public void deleteColumnFamilyTwice() throws Exception {

    Admin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor beforehtd = admin.getTableDescriptor(TABLENAME);
    String cfToDelete = "cf1";

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();

    // 1 - Check if table exists in descriptor
    assertTrue(admin.isTableAvailable(TABLENAME));

    // 2 - Check if all the target column family exist in descriptor
    HColumnDescriptor[] families = beforehtd.getColumnFamilies();
    Boolean foundCF = false;
    int i;
    for (i = 0; i < families.length; i++) {
      if (families[i].getNameAsString().equals(cfToDelete)) {
        foundCF = true;
        break;
      }
    }
    assertTrue(foundCF);

    // 3 - Check if table exists in FS
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), TABLENAME);
    assertTrue(fs.exists(tableDir));

    // 4 - Check if all the target column family exist in FS
    FileStatus[] fileStatus = fs.listStatus(tableDir);
    foundCF = false;
    for (i = 0; i < fileStatus.length; i++) {
      if (fileStatus[i].isDirectory() == true) {
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath(), new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().contains(HConstants.RECOVERED_EDITS_DIR)) {
              return false;
            }
            return true;
          }
        });
        for (int j = 0; j < cf.length; j++) {
          if (cf[j].isDirectory() == true && cf[j].getPath().getName().equals(cfToDelete)) {
            foundCF = true;
            break;
          }
        }
      }
      if (foundCF) {
        break;
      }
    }
    assertTrue(foundCF);

    // TEST - Disable and delete the column family
    if (admin.isTableEnabled(TABLENAME)) {
      admin.disableTable(TABLENAME);
    }
    admin.deleteColumn(TABLENAME, Bytes.toBytes(cfToDelete));

    // 5 - Check if the target column family is gone from the FS
    fileStatus = fs.listStatus(tableDir);
    for (i = 0; i < fileStatus.length; i++) {
      if (fileStatus[i].isDirectory() == true) {
        FileStatus[] cf = fs.listStatus(fileStatus[i].getPath(), new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (WALSplitter.isSequenceIdFile(p)) {
              return false;
            }
            return true;
          }
        });
        for (int j = 0; j < cf.length; j++) {
          if (cf[j].isDirectory() == true) {
            assertFalse(cf[j].getPath().getName().equals(cfToDelete));
          }
        }
      }
    }

    try {
      // Test: delete again
      admin.deleteColumn(TABLENAME, Bytes.toBytes(cfToDelete));
      Assert.fail("Delete a non-exist column family should fail");
    } catch (InvalidFamilyOperationException e) {
      // Expected.
    }
  }

}
