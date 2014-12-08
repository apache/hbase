/*
 *
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

package org.apache.hadoop.hbase.master;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ServiceException;

@Category(MediumTests.class)
public class TestHMasterRPCException {

  @Test
  public void testRPCException() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.MASTER_PORT, "0");
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);
    HMaster hm = new HMaster(conf, cp);
    ServerName sm = hm.getServerName();
    RpcClient rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    try {
      int i = 0;
      //retry the RPC a few times; we have seen SocketTimeoutExceptions if we
      //try to connect too soon. Retry on SocketTimeoutException.
      while (i < 20) {
        try {
          BlockingRpcChannel channel =
            rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
          MasterProtos.MasterService.BlockingInterface stub =
            MasterProtos.MasterService.newBlockingStub(channel);
          stub.isMasterRunning(null, IsMasterRunningRequest.getDefaultInstance());
          fail();
        } catch (ServiceException ex) {
          IOException ie = ProtobufUtil.getRemoteException(ex);
          if (!(ie instanceof SocketTimeoutException)) {
            if (ie.getMessage().startsWith("org.apache.hadoop.hbase.ipc." +
                "ServerNotRunningYetException: Server is not running yet")) {
              // Done.  Got the exception we wanted.
              System.out.println("Expected exception: " + ie.getMessage());
              return;
            } else {
              throw ex;
            }
          } else {
            System.err.println("Got SocketTimeoutException. Will retry. ");
          }
        } catch (Throwable t) {
          fail("Unexpected throwable: " + t);
        }
        Thread.sleep(100);
        i++;
      }
      fail();
    } finally {
      rpcClient.close();
    }
  }
}