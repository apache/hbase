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

import static org.apache.hadoop.hbase.HConstants.HIGH_QOS;
import static org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
import static org.apache.hadoop.hbase.HConstants.SYSTEMTABLE_QOS;
import static org.apache.hadoop.hbase.NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterResponse;

/**
 * Confirm that we will set the priority in {@link HBaseRpcController} for several admin operations.
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestAsyncAdminRpcPriority {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAdminRpcPriority.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private MasterService.Interface masterStub;

  private AdminService.Interface adminStub;

  private AsyncConnection conn;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws IOException {
    masterStub = mock(MasterService.Interface.class);
    adminStub = mock(AdminService.Interface.class);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<GetProcedureResultResponse> done = invocation.getArgument(2);
        done.run(GetProcedureResultResponse.newBuilder()
          .setState(GetProcedureResultResponse.State.FINISHED).build());
        return null;
      }
    }).when(masterStub).getProcedureResult(any(HBaseRpcController.class),
      any(GetProcedureResultRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<CreateTableResponse> done = invocation.getArgument(2);
        done.run(CreateTableResponse.newBuilder().setProcId(1L).build());
        return null;
      }
    }).when(masterStub).createTable(any(HBaseRpcController.class), any(CreateTableRequest.class),
      any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<ShutdownResponse> done = invocation.getArgument(2);
        done.run(ShutdownResponse.getDefaultInstance());
        return null;
      }
    }).when(masterStub).shutdown(any(HBaseRpcController.class), any(ShutdownRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<StopMasterResponse> done = invocation.getArgument(2);
        done.run(StopMasterResponse.getDefaultInstance());
        return null;
      }
    }).when(masterStub).stopMaster(any(HBaseRpcController.class), any(StopMasterRequest.class),
      any());

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<StopServerResponse> done = invocation.getArgument(2);
        done.run(StopServerResponse.getDefaultInstance());
        return null;
      }
    }).when(adminStub).stopServer(any(HBaseRpcController.class), any(StopServerRequest.class),
      any());

    conn = new AsyncConnectionImpl(CONF, new DoNothingConnectionRegistry(CONF), "test",
      UserProvider.instantiate(CONF).getCurrent()) {

      @Override
      CompletableFuture<MasterService.Interface> getMasterStub() {
        return CompletableFuture.completedFuture(masterStub);
      }

      @Override
      Interface getAdminStub(ServerName serverName) throws IOException {
        return adminStub;
      }
    };
  }

  private HBaseRpcController assertPriority(int priority) {
    return argThat(new ArgumentMatcher<HBaseRpcController>() {

      @Override
      public boolean matches(HBaseRpcController controller) {
        return controller.getPriority() == priority;
      }
    });
  }

  @Test
  public void testCreateNormalTable() {
    conn.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build())
      .join();
    verify(masterStub, times(1)).createTable(assertPriority(NORMAL_QOS),
      any(CreateTableRequest.class), any());
  }

  // a bit strange as we can not do this in production but anyway, just a client mock to confirm
  // that we pass the correct priority
  @Test
  public void testCreateSystemTable() {
    conn.getAdmin()
      .createTable(TableDescriptorBuilder
        .newBuilder(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build())
      .join();
    verify(masterStub, times(1)).createTable(assertPriority(SYSTEMTABLE_QOS),
      any(CreateTableRequest.class), any());
  }

  // a bit strange as we can not do this in production but anyway, just a client mock to confirm
  // that we pass the correct priority
  @Test
  public void testCreateMetaTable() {
    conn.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TableName.META_TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build()).join();
    verify(masterStub, times(1)).createTable(assertPriority(SYSTEMTABLE_QOS),
      any(CreateTableRequest.class), any());
  }

  @Test
  public void testShutdown() {
    conn.getAdmin().shutdown().join();
    verify(masterStub, times(1)).shutdown(assertPriority(HIGH_QOS), any(ShutdownRequest.class),
      any());
  }

  @Test
  public void testStopMaster() {
    conn.getAdmin().stopMaster().join();
    verify(masterStub, times(1)).stopMaster(assertPriority(HIGH_QOS), any(StopMasterRequest.class),
      any());
  }

  @Test
  public void testStopRegionServer() {
    conn.getAdmin().stopRegionServer(ServerName.valueOf("rs", 16010, 12345)).join();
    verify(adminStub, times(1)).stopServer(assertPriority(HIGH_QOS), any(StopServerRequest.class),
      any());
  }
}
