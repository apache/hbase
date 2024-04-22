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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BadMasterObserverForCreateDeleteTable;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestCreateDeleteTableProcedureWithRetry {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCreateDeleteTableProcedureWithRetry.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME =
    TableName.valueOf(TestCreateDeleteTableProcedureWithRetry.class.getSimpleName());

  private static final String CF = "cf";

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      BadMasterObserverForCreateDeleteTable.class.getName());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateDeleteTableRetry() throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    TableDescriptor htd = MasterProcedureTestingUtility.createHTD(TABLE_NAME, CF);
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, null);
    CreateTableProcedure createProc =
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions);
    ProcedureTestingUtility.submitAndWait(procExec, createProc);
    Assert.assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getMiniHBaseCluster().getMaster(),
      TABLE_NAME, regions, CF);

    UTIL.getAdmin().disableTable(TABLE_NAME);
    DeleteTableProcedure deleteProc =
      new DeleteTableProcedure(procExec.getEnvironment(), TABLE_NAME);
    ProcedureTestingUtility.submitAndWait(procExec, deleteProc);
    Assert.assertFalse(UTIL.getAdmin().tableExists(TABLE_NAME));
    MasterProcedureTestingUtility.validateTableDeletion(UTIL.getMiniHBaseCluster().getMaster(),
      TABLE_NAME);
  }
}
