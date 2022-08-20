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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hbase.master.replication.AssignReplicationQueuesProcedure;
import org.apache.hadoop.hbase.master.replication.RemovePeerProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Make sure we will wait until all the SCPs finished in RemovePeerProcedure.
 * <p/>
 * See HBASE-27109 for more details.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestRemovePeerProcedureWaitForSCP extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRemovePeerProcedureWaitForSCP.class);

  private static final TableName tableName3 = TableName.valueOf("test3");

  private static final String PEER_ID3 = "3";

  private static Table table3;

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
          if (e.getClassName().equals(AssignReplicationQueuesProcedure.class.getName())) {
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
    protected ServerManager createServerManager(MasterServices master, RegionServerList storage)
      throws IOException {
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(table3, true);
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void testWait() throws Exception {
    // disable the peers
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    hbaseAdmin.disableReplicationPeer(PEER_ID3);

    // put some data
    UTIL1.loadTable(htable1, famName);
    UTIL1.loadTable(table3, famName);

    EMPTY = true;
    UTIL1.getMiniHBaseCluster().stopRegionServer(0).join();
    UTIL1.getMiniHBaseCluster().startRegionServer();

    // since there is no active region server to get the replication queue, the procedure should be
    // in WAITING_TIMEOUT state for most time to retry
    HMaster master = UTIL1.getMiniHBaseCluster().getMaster();
    UTIL1.waitFor(30000,
      () -> master.getProcedures().stream()
        .filter(p -> p instanceof AssignReplicationQueuesProcedure)
        .anyMatch(p -> p.getState() == ProcedureState.WAITING_TIMEOUT));

    // call remove replication peer, and make sure it will be stuck in the POST_PEER_MODIFICATION
    // state.
    hbaseAdmin.removeReplicationPeerAsync(PEER_ID3);
    UTIL1.waitFor(30000,
      () -> master.getProcedures().stream().filter(p -> p instanceof RemovePeerProcedure)
        .anyMatch(p -> ((RemovePeerProcedure) p).getCurrentStateId()
            == PeerModificationState.POST_PEER_MODIFICATION_VALUE));
    Thread.sleep(5000);
    assertEquals(PeerModificationState.POST_PEER_MODIFICATION_VALUE,
      ((RemovePeerProcedure) master.getProcedures().stream()
        .filter(p -> p instanceof RemovePeerProcedure).findFirst().get()).getCurrentStateId());
    EMPTY = false;
    // wait until the SCP finished, AssignReplicationQueuesProcedure is a sub procedure of SCP
    UTIL1.waitFor(30000, () -> master.getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).allMatch(Procedure::isSuccess));
    // the RemovePeerProcedure should have also finished
    UTIL1.waitFor(30000, () -> master.getProcedures().stream()
      .filter(p -> p instanceof RemovePeerProcedure).allMatch(Procedure::isSuccess));
    // make sure there is no remaining replication queues for PEER_ID3
    assertThat(master.getReplicationPeerManager().getQueueStorage().listAllQueueIds(PEER_ID3),
      empty());
  }
}
