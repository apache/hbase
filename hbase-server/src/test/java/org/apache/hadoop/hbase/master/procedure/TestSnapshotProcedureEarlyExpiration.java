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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureEarlyExpiration extends TestSnapshotProcedure {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureEarlyExpiration.class);

  @Before
  @Override
  public void setup() throws Exception { // Copied from TestSnapshotProcedure with modified
                                         // SnapshotDescription
    TEST_UTIL = new HBaseTestingUtil();
    Configuration config = TEST_UTIL.getConfiguration();
    // using SnapshotVerifyProcedure to verify snapshot
    config.setInt("hbase.snapshot.remote.verify.threshold", 1);
    // disable info server. Info server is useful when we run unit tests locally,
    // but it will
    // fails integration testing of jenkins.
    // config.setInt(HConstants.MASTER_INFO_PORT, 8080);

    // delay dispatch so that we can do something, for example kill a target server
    config.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    config.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("SPTestTable"));
    CF = Bytes.toBytes("cf");
    SNAPSHOT_NAME = "SnapshotProcedureTest";

    Map<String, Object> properties = new HashMap<>();
    properties.put("TTL", 1L);
    snapshot = new SnapshotDescription(SNAPSHOT_NAME, TABLE_NAME, SnapshotType.FLUSH, null, -1, -1,
      properties);

    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(TABLE_NAME, CF, splitKeys);
    TEST_UTIL.loadTable(table, CF, false);
  }

  @Test
  public void testSnapshotEarlyExpiration() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    SnapshotProcedure spySp = getDelayedOnSpecificStateSnapshotProcedure(sp,
      procExec.getEnvironment(), SnapshotState.SNAPSHOT_COMPLETE_SNAPSHOT);

    long procId = procExec.submitProcedure(spySp);

    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), procId);
    assertTrue(spySp.isFailed());
    List<SnapshotProtos.SnapshotDescription> snapshots =
      master.getSnapshotManager().getCompletedSnapshots();
    assertEquals(0, snapshots.size());
  }
}
