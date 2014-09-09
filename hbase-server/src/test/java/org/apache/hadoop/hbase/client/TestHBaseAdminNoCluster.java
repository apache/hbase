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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mortbay.log.Log;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Category(SmallTests.class)
public class TestHBaseAdminNoCluster {
  /**
   * Verify that PleaseHoldException gets retried.
   * HBASE-8764
   * @throws IOException 
   * @throws ZooKeeperConnectionException 
   * @throws MasterNotRunningException 
   * @throws ServiceException 
   */
  @Test
  public void testMasterMonitorCollableRetries()
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
    Configuration configuration = HBaseConfiguration.create();
    // Set the pause and retry count way down.
    configuration.setLong(HConstants.HBASE_CLIENT_PAUSE, 1);
    final int count = 10;
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, count);
    // Get mocked connection.   Getting the connection will register it so when HBaseAdmin is
    // constructed with same configuration, it will find this mocked connection.
    HConnection connection = HConnectionTestingUtility.getMockedConnection(configuration);
    // Mock so we get back the master interface.  Make it so when createTable is called, we throw
    // the PleaseHoldException.
    MasterKeepAliveConnection masterAdmin =
      Mockito.mock(MasterKeepAliveConnection.class);
    Mockito.when(masterAdmin.createTable((RpcController)Mockito.any(),
      (CreateTableRequest)Mockito.any())).
        thenThrow(new ServiceException("Test fail").initCause(new PleaseHoldException("test")));
    Mockito.when(connection.getKeepAliveMasterService()).thenReturn(masterAdmin);
    // Mock up our admin Interfaces
    Admin admin = new HBaseAdmin(configuration);
    try {
      HTableDescriptor htd =
          new HTableDescriptor(TableName.valueOf("testMasterMonitorCollableRetries"));
      // Pass any old htable descriptor; not important
      try {
        admin.createTable(htd, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
        fail();
      } catch (RetriesExhaustedException e) {
        Log.info("Expected fail", e);
      }
      // Assert we were called 'count' times.
      Mockito.verify(masterAdmin, Mockito.atLeast(count)).createTable((RpcController)Mockito.any(),
        (CreateTableRequest)Mockito.any());
    } finally {
      admin.close();
      if (connection != null)HConnectionManager.deleteConnection(configuration);
    }
  }
}