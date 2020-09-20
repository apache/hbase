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

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListReplicationSinkServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListReplicationSinkServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationFetchServers extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationFetchServers.class);

  private static AtomicBoolean fetchFlag = new AtomicBoolean(false);

  public static class MyObserver implements MasterCoprocessor, MasterObserver {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void postListReplicationSinkServers(ObserverContext<MasterCoprocessorEnvironment> ctx) {
      fetchFlag.set(true);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF2.set(MASTER_COPROCESSOR_CONF_KEY, MyObserver.class.getName());
    TestReplicationBase.setUpBeforeClass();
  }

  @Before
  public void beforeMethod() {
    fetchFlag.set(false);
  }

  @Test
  public void testMasterListReplicationPeerServers() throws IOException, ServiceException {
    AsyncClusterConnection conn = UTIL2.getAsyncConnection();
    ServerName master = UTIL2.getAdmin().getMaster();
    MasterService.BlockingInterface masterStub = MasterService.newBlockingStub(
        conn.getRpcClient().createBlockingRpcChannel(master, User.getCurrent(), 1000));
    ListReplicationSinkServersResponse resp = masterStub.listReplicationSinkServers(
        null, ListReplicationSinkServersRequest.newBuilder().build());
    List<ServerName> servers = ProtobufUtil.toServerNameList(resp.getServerNameList());
    assertFalse(servers.isEmpty());
    assertTrue(fetchFlag.get());
  }

  @Test
  public void testPutData() throws IOException {
    htable1.put(new Put(row).addColumn(famName, famName, row));
    UTIL2.waitFor(30000L, () -> !htable2.get(new Get(row)).isEmpty());
    assertTrue(fetchFlag.get());
  }
}
