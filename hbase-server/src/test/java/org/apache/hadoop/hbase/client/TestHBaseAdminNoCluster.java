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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ServiceException;

@Category({SmallTests.class, ClientTests.class})
public class TestHBaseAdminNoCluster {

  private static final Log LOG = LogFactory.getLog(TestHBaseAdminNoCluster.class);

  /**
   * Verify that PleaseHoldException gets retried.
   * HBASE-8764
   * @throws IOException
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   * @throws ServiceException
   * @throws org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException 
   */
  //TODO: Clean up, with Procedure V2 and nonce to prevent the same procedure to call mulitple
  // time, this test is invalid anymore. Just keep the test around for some time before
  // fully removing it.
  @Ignore
  @Test
  public void testMasterMonitorCallableRetries()
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException, 
  org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {
    Configuration configuration = HBaseConfiguration.create();
    // Set the pause and retry count way down.
    configuration.setLong(HConstants.HBASE_CLIENT_PAUSE, 1);
    final int count = 10;
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, count);
    // Get mocked connection.   Getting the connection will register it so when HBaseAdmin is
    // constructed with same configuration, it will find this mocked connection.
    ClusterConnection connection = HConnectionTestingUtility.getMockedConnection(configuration);
    // Mock so we get back the master interface.  Make it so when createTable is called, we throw
    // the PleaseHoldException.
    MasterKeepAliveConnection masterAdmin = Mockito.mock(MasterKeepAliveConnection.class);
    Mockito.when(masterAdmin.createTable((RpcController)Mockito.any(),
      (CreateTableRequest)Mockito.any())).
        thenThrow(new ServiceException("Test fail").initCause(new PleaseHoldException("test")));
    Mockito.when(connection.getKeepAliveMasterService()).thenReturn(masterAdmin);
    Admin admin = new HBaseAdmin(connection);
    try {
      HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testMasterMonitorCollableRetries"));
      // Pass any old htable descriptor; not important
      try {
        admin.createTable(htd, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
        fail();
      } catch (RetriesExhaustedException e) {
        LOG.info("Expected fail", e);
      }
      // Assert we were called 'count' times.
      Mockito.verify(masterAdmin, Mockito.atLeast(count)).createTable((RpcController)Mockito.any(),
        (CreateTableRequest)Mockito.any());
    } finally {
      admin.close();
      if (connection != null) connection.close();
    }
  }

  @Test
  public void testMasterOperationsRetries() throws Exception {

    // Admin.listTables()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.listTables();
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .getTableDescriptors((RpcController)Mockito.any(),
            (GetTableDescriptorsRequest)Mockito.any());
      }
    });

    // Admin.listTableNames()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.listTableNames();
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .getTableNames((RpcController)Mockito.any(),
            (GetTableNamesRequest)Mockito.any());
      }
    });

    // Admin.getTableDescriptor()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.getTableDescriptor(TableName.valueOf("getTableDescriptor"));
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .getTableDescriptors((RpcController)Mockito.any(),
            (GetTableDescriptorsRequest)Mockito.any());
      }
    });

    // Admin.getTableDescriptorsByTableName()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.getTableDescriptorsByTableName(new ArrayList<TableName>());
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .getTableDescriptors((RpcController)Mockito.any(),
            (GetTableDescriptorsRequest)Mockito.any());
      }
    });

    // Admin.move()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.move(new byte[0], null);
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .moveRegion((RpcController)Mockito.any(),
            (MoveRegionRequest)Mockito.any());
      }
    });

    // Admin.offline()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.offline(new byte[0]);
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .offlineRegion((RpcController)Mockito.any(),
            (OfflineRegionRequest)Mockito.any());
      }
    });

    // Admin.setBalancerRunning()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.setBalancerRunning(true, true);
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .setBalancerRunning((RpcController)Mockito.any(),
            (SetBalancerRunningRequest)Mockito.any());
      }
    });

    // Admin.balancer()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.balancer();
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .balance((RpcController)Mockito.any(),
            (BalanceRequest)Mockito.any());
      }
    });

    // Admin.enabledCatalogJanitor()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.enableCatalogJanitor(true);
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .enableCatalogJanitor((RpcController)Mockito.any(),
            (EnableCatalogJanitorRequest)Mockito.any());
      }
    });

    // Admin.runCatalogScan()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.runCatalogScan();
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .runCatalogScan((RpcController)Mockito.any(),
            (RunCatalogScanRequest)Mockito.any());
      }
    });

    // Admin.isCatalogJanitorEnabled()
    testMasterOperationIsRetried(new MethodCaller() {
      @Override
      public void call(Admin admin) throws Exception {
        admin.isCatalogJanitorEnabled();
      }
      @Override
      public void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception {
        Mockito.verify(masterAdmin, Mockito.atLeast(count))
          .isCatalogJanitorEnabled((RpcController)Mockito.any(),
            (IsCatalogJanitorEnabledRequest)Mockito.any());
      }
    });
  }

  private static interface MethodCaller {
    void call(Admin admin) throws Exception;
    void verify(MasterKeepAliveConnection masterAdmin, int count) throws Exception;
  }

  private void testMasterOperationIsRetried(MethodCaller caller) throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    // Set the pause and retry count way down.
    configuration.setLong(HConstants.HBASE_CLIENT_PAUSE, 1);
    final int count = 10;
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, count);

    ClusterConnection connection = mock(ClusterConnection.class);
    when(connection.getConfiguration()).thenReturn(configuration);
    MasterKeepAliveConnection masterAdmin =
        Mockito.mock(MasterKeepAliveConnection.class, new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            if (invocation.getMethod().getName().equals("close")) {
              return null;
            }
            throw new MasterNotRunningException(); // all methods will throw an exception
          }
        });
    Mockito.when(connection.getKeepAliveMasterService()).thenReturn(masterAdmin);
    RpcControllerFactory rpcControllerFactory = Mockito.mock(RpcControllerFactory.class);
    Mockito.when(connection.getRpcControllerFactory()).thenReturn(rpcControllerFactory);
    Mockito.when(rpcControllerFactory.newController()).thenReturn(
      Mockito.mock(HBaseRpcController.class));

    // we need a real retrying caller
    RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(configuration);
    Mockito.when(connection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);

    Admin admin = null;
    try {
      admin = Mockito.spy(new HBaseAdmin(connection));
      // mock the call to getRegion since in the absence of a cluster (which means the meta
      // is not assigned), getRegion can't function
      Mockito.doReturn(null).when(((HBaseAdmin)admin)).getRegion(Matchers.<byte[]>any());
      try {
        caller.call(admin); // invoke the HBaseAdmin method
        fail();
      } catch (RetriesExhaustedException e) {
        LOG.info("Expected fail", e);
      }
      // Assert we were called 'count' times.
      caller.verify(masterAdmin, count);
    } finally {
      if (admin != null) {admin.close();}
    }
  }
}