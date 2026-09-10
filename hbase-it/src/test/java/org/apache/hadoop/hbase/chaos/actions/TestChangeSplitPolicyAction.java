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

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(MediumTests.TAG)
public class TestChangeSplitPolicyAction {

  private final static IntegrationTestingUtility TEST_UTIL = new IntegrationTestingUtility();
  private final TableName tableName = TableName.valueOf("ChangeSplitPolicyAction");

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
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
