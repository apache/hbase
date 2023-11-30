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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestListTablesByState {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestListTablesByState.class);

  private static HBaseTestingUtil UTIL;
  private static Admin ADMIN;
  private static final int SLAVES = 1;
  private static final byte[] COLUMN = Bytes.toBytes("cf");
  private static final TableName TABLE = TableName.valueOf("test");
  private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN).setMaxVersions(3).build())
    .build();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtil();
    UTIL.startMiniCluster(SLAVES);
    ADMIN = UTIL.getAdmin();
  }

  @Before
  public void before() throws Exception {
    if (ADMIN.tableExists(TABLE)) {
      if (ADMIN.isTableEnabled(TABLE)) {
        ADMIN.disableTable(TABLE);
      }

      ADMIN.deleteTable(TABLE);
    }
  }

  @Test
  public void testListTableNamesByState() throws Exception {
    ADMIN.createTable(TABLE_DESC);
    ADMIN.disableTable(TABLE);
    Assert.assertEquals(ADMIN.listTableNamesByState(false).get(0), TABLE);
    ADMIN.enableTable(TABLE);
    Assert.assertEquals(ADMIN.listTableNamesByState(true).get(0), TABLE);
  }

  @Test
  public void testListTableDescriptorByState() throws Exception {
    ADMIN.createTable(TABLE_DESC);
    ADMIN.disableTable(TABLE);
    Assert.assertEquals(ADMIN.listTableDescriptorsByState(false).get(0).getTableName(), TABLE);
    ADMIN.enableTable(TABLE);
    Assert.assertEquals(ADMIN.listTableDescriptorsByState(true).get(0).getTableName(), TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) {
      ADMIN.close();
    }
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }
}
