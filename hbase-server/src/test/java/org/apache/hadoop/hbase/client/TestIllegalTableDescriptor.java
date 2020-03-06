/**
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;

@Category({LargeTests.class, ClientTests.class})
public class TestIllegalTableDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIllegalTableDescriptor.class);

  // NOTE: Increment tests were moved to their own class, TestIncrementsFromClientSide.
  private static final Logger LOGGER;

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte [] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();

  static {
    LOGGER = mock(Logger.class);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // replacing HMaster.LOG with our mock logger for verifying logging
    Field field = TableDescriptorChecker.class.getDeclaredField("LOG");
    field.setAccessible(true);
    field.set(null, LOGGER);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, true); // enable for below tests
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIllegalTableDescriptor() throws Exception {
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(
        TableName.valueOf(name.getMethodName()));
    ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setColumnFamily(familyDescriptor);
    checkTableIsLegal(tableDescriptor);

    tableDescriptor.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setMaxFileSize(0);
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(tableDescriptor);

    tableDescriptor.setMemStoreFlushSize(1024);
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setMemStoreFlushSize(0);
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(tableDescriptor);

    tableDescriptor.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(tableDescriptor);

    tableDescriptor.setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, "nonexisting.foo.class");
    checkTableIsIllegal(tableDescriptor);
    tableDescriptor.removeValue(Bytes.toBytes(HConstants.HBASE_REGION_SPLIT_POLICY_KEY));
    checkTableIsLegal(tableDescriptor);

    familyDescriptor.setBlocksize(0);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setBlocksize(1024);
    checkTableIsLegal(tableDescriptor);

    familyDescriptor.setTimeToLive(0);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setTimeToLive(-1);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setTimeToLive(1);
    checkTableIsLegal(tableDescriptor);

    familyDescriptor.setMinVersions(-1);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setMinVersions(3);
    try {
      familyDescriptor.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      familyDescriptor.setMaxVersions(10);
    }
    checkTableIsLegal(tableDescriptor);

    // HBASE-13776 Setting illegal versions for HColumnDescriptor
    //  does not throw IllegalArgumentException
    // finally, minVersions must be less than or equal to maxVersions
    familyDescriptor.setMaxVersions(4);
    familyDescriptor.setMinVersions(5);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setMinVersions(3);

    familyDescriptor.setScope(-1);
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setScope(0);
    checkTableIsLegal(tableDescriptor);

    familyDescriptor.setValue(ColumnFamilyDescriptorBuilder.IN_MEMORY_COMPACTION, "INVALID");
    checkTableIsIllegal(tableDescriptor);
    familyDescriptor.setValue(ColumnFamilyDescriptorBuilder.IN_MEMORY_COMPACTION, "NONE");
    checkTableIsLegal(tableDescriptor);

    try {
      familyDescriptor.setDFSReplication((short) -1);
      fail("Illegal value for setDFSReplication did not throw");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // set an illegal DFS replication value by hand
    familyDescriptor.setValue(ColumnFamilyDescriptorBuilder.DFS_REPLICATION, "-1");
    checkTableIsIllegal(tableDescriptor);
    try {
      familyDescriptor.setDFSReplication((short) -1);
      fail("Should throw exception if an illegal value is explicitly being set");
    } catch (IllegalArgumentException e) {
      // pass
    }

    // check the conf settings to disable sanity checks
    tableDescriptor.setMemStoreFlushSize(0);

    // Check that logs warn on invalid table but allow it.
    tableDescriptor.setValue(TableDescriptorChecker.TABLE_SANITY_CHECKS, Boolean.FALSE.toString());
    checkTableIsLegal(tableDescriptor);

    verify(LOGGER).warn(contains("MEMSTORE_FLUSHSIZE for table "
        + "descriptor or \"hbase.hregion.memstore.flush.size\" (0) is too small, which might "
        + "cause very frequent flushing."));
  }

  private void checkTableIsLegal(TableDescriptor tableDescriptor) throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    admin.createTable(tableDescriptor);
    assertTrue(admin.tableExists(tableDescriptor.getTableName()));
    TEST_UTIL.deleteTable(tableDescriptor.getTableName());
  }

  private void checkTableIsIllegal(TableDescriptor tableDescriptor) throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    try {
      admin.createTable(tableDescriptor);
      fail();
    } catch(Exception ex) {
      // should throw ex
    }
    assertFalse(admin.tableExists(tableDescriptor.getTableName()));
  }
}
