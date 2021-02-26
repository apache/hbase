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
package org.apache.hadoop.hbase.chaos.actions;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MediumTests.class})
public class TestChangeSplitPolicyAction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestChangeSplitPolicyAction.class);

  private final static IntegrationTestingUtility TEST_UTIL = new IntegrationTestingUtility();
  private final TableName tableName = TableName.valueOf("ChangeSplitPolicyAction");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  @Before
  public void setUp() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    admin.createTable(builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam")).build());
  }

  @Test
  public void testChangeSplitPolicyAction() throws Exception {
    Action.ActionContext ctx = Mockito.mock(Action.ActionContext.class);
    Mockito.when(ctx.getHBaseIntegrationTestingUtility()).thenReturn(TEST_UTIL);
    Mockito.when(ctx.getHBaseCluster()).thenReturn(TEST_UTIL.getHBaseCluster());
    ChangeSplitPolicyAction action = new ChangeSplitPolicyAction(tableName);
    action.init(ctx);
    action.perform();
  }
}
