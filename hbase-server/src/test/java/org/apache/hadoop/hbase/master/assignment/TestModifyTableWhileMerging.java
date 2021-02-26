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
package org.apache.hadoop.hbase.master.assignment;

import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;


@Category({MasterTests.class, MediumTests.class})
public class TestModifyTableWhileMerging {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestModifyTableWhileMerging.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestModifyTableWhileMerging.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static TableName TABLE_NAME = TableName.valueOf("test");
  private static Admin admin;
  private static Table client;
  private static byte[] CF = Bytes.toBytes("cf");
  private static byte[] SPLITKEY = Bytes.toBytes("bbbbbbb");

  @BeforeClass
  public static void setupCluster() throws Exception {
    //Set procedure executor thread to 1, making reproducing this issue of HBASE-20921 easier
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(1);
    admin = UTIL.getHBaseAdmin();
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = SPLITKEY;
    client = UTIL.createTable(TABLE_NAME, CF, splitKeys);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void test() throws Exception {
    TableDescriptor tableDescriptor = client.getDescriptor();
    ProcedureExecutor<MasterProcedureEnv> executor = UTIL.getMiniHBaseCluster().getMaster()
        .getMasterProcedureExecutor();
    MasterProcedureEnv env = executor.getEnvironment();
    List<RegionInfo> regionInfos = admin.getRegions(TABLE_NAME);
    MergeTableRegionsProcedure mergeTableRegionsProcedure = new MergeTableRegionsProcedure(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
        .getEnvironment(), new RegionInfo [] {regionInfos.get(0), regionInfos.get(1)}, false);
    ModifyTableProcedure modifyTableProcedure = new ModifyTableProcedure(env, tableDescriptor);
    long procModify = executor.submitProcedure(modifyTableProcedure);
    UTIL.waitFor(30000, () -> executor.getProcedures().stream()
      .filter(p -> p instanceof ModifyTableProcedure)
      .map(p -> (ModifyTableProcedure) p)
      .anyMatch(p -> TABLE_NAME.equals(p.getTableName())));
    long proc = executor.submitProcedure(mergeTableRegionsProcedure);
    UTIL.waitFor(3000000, () -> UTIL.getMiniHBaseCluster().getMaster()
        .getMasterProcedureExecutor().isFinished(procModify));
    Assert.assertEquals("Modify Table procedure should success!",
        ProcedureProtos.ProcedureState.SUCCESS, modifyTableProcedure.getState());
  }

}
