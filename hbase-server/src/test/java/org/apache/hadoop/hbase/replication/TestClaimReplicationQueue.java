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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionServerList;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.replication.ClaimReplicationQueuesProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * In HBASE-26029, we reimplement the claim queue operation with proc-v2 and make it a step in SCP,
 * this is a UT to make sure the {@link ClaimReplicationQueuesProcedure} works correctly.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestClaimReplicationQueue extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClaimReplicationQueue.class);

  private static final TableName tableName3 = TableName.valueOf("test3");

  private static final String PEER_ID3 = "3";

  private static Table table3;

  private static Table table4;

  private static volatile boolean EMPTY = false;

  public static final class ServerManagerForTest extends ServerManager {

    public ServerManagerForTest(MasterServices master, RegionServerList storage) {
      super(master, storage);
    }

    @Override
    public List<ServerName> getOnlineServersList() {
      // return no region server to make the procedure hang
      if (EMPTY) {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (e.getClassName().equals(ClaimReplicationQueuesProcedure.class.getName())) {
            return Collections.emptyList();
          }
        }
      }
      return super.getOnlineServersList();
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected ServerManager createServerManager(MasterServices master,
      RegionServerList storage) throws IOException {
      setupClusterConnection();
      return new ServerManagerForTest(master, storage);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF1.setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    TestReplicationBase.setUpBeforeClass();
    createTable(tableName3);
    table3 = connection1.getTable(tableName3);
    table4 = connection2.getTable(tableName3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(table3, true);
    Closeables.close(table4, true);
    TestReplicationBase.tearDownAfterClass();
  }

  @Override
  public void setUpBase() throws Exception {
    super.setUpBase();
    // set up two replication peers and only 1 rs to test claim replication queue with multiple
    // round
    addPeer(PEER_ID3, tableName3);
  }

  @Override
  public void tearDownBase() throws Exception {
    super.tearDownBase();
    removePeer(PEER_ID3);
  }

  @Test
  public void testClaim() throws Exception {
    // disable the peers
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    hbaseAdmin.disableReplicationPeer(PEER_ID3);

    // put some data
    int count1 = UTIL1.loadTable(htable1, famName);
    int count2 = UTIL1.loadTable(table3, famName);

    EMPTY = true;
    UTIL1.getMiniHBaseCluster().stopRegionServer(0).join();
    UTIL1.getMiniHBaseCluster().startRegionServer();

    // since there is no active region server to get the replication queue, the procedure should be
    // in WAITING_TIMEOUT state for most time to retry
    HMaster master = UTIL1.getMiniHBaseCluster().getMaster();
    UTIL1.waitFor(30000,
      () -> master.getProcedures().stream()
        .filter(p -> p instanceof ClaimReplicationQueuesProcedure)
        .anyMatch(p -> p.getState() == ProcedureState.WAITING_TIMEOUT));

    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    hbaseAdmin.enableReplicationPeer(PEER_ID3);

    EMPTY = false;
    // wait until the SCP finished, ClaimReplicationQueuesProcedure is a sub procedure of SCP
    UTIL1.waitFor(30000, () -> master.getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).allMatch(Procedure::isSuccess));

    // we should get all the data in the target cluster
    waitForReplication(htable2, count1, NB_RETRIES);
    waitForReplication(table4, count2, NB_RETRIES);
  }
}
