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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

/**
 * Test implementation of RSProcedureDispatcher that throws desired errors for testing purpose.
 */
public class RSProcDispatcher extends RSProcedureDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(RSProcDispatcher.class);

  private static final AtomicInteger i = new AtomicInteger();

  public RSProcDispatcher(MasterServices master) {
    super(master);
  }

  @Override
  protected void remoteDispatch(final ServerName serverName,
    final Set<RemoteProcedure> remoteProcedures) {
    if (!master.getServerManager().isServerOnline(serverName)) {
      // fail fast
      submitTask(new DeadRSRemoteCall(serverName, remoteProcedures));
    } else {
      submitTask(new TestExecuteProceduresRemoteCall(serverName, remoteProcedures));
    }
  }

  class TestExecuteProceduresRemoteCall extends ExecuteProceduresRemoteCall {

    public TestExecuteProceduresRemoteCall(ServerName serverName,
      Set<RemoteProcedure> remoteProcedures) {
      super(serverName, remoteProcedures);
    }

    @Override
    public AdminProtos.ExecuteProceduresResponse sendRequest(final ServerName serverName,
      final AdminProtos.ExecuteProceduresRequest request) throws IOException {
      int j = i.addAndGet(1);
      LOG.info("sendRequest() req: {} , j: {}", request, j);
      if (j == 12 || j == 22) {
        // Execute the remote close and open region requests in the last (5th) retry before
        // throwing ConnectionClosedException. This is to ensure even if the region open/close
        // is successfully completed by regionserver, master still schedules SCP because
        // sendRequest() throws error which has retry-limit exhausted.
        try {
          getRsAdmin().executeProcedures(null, request);
        } catch (ServiceException e) {
          throw new RuntimeException(e);
        }
      }
      // For one of the close region requests and one of the open region requests,
      // throw ConnectionClosedException until retry limit is exhausted and master
      // schedules recoveries for the server.
      // We will have ABNORMALLY_CLOSED regions, and they are expected to recover on their own.
      if (j >= 10 && j <= 15 || j >= 18 && j <= 23) {
        throw new ConnectionClosedException("test connection closed error...");
      }
      try {
        return getRsAdmin().executeProcedures(null, request);
      } catch (ServiceException e) {
        throw new RuntimeException(e);
      }
    }

    private AdminProtos.AdminService.BlockingInterface getRsAdmin() throws IOException {
      return master.getServerManager().getRsAdmin(getServerName());
    }
  }

  private class DeadRSRemoteCall extends ExecuteProceduresRemoteCall {

    public DeadRSRemoteCall(ServerName serverName, Set<RemoteProcedure> remoteProcedures) {
      super(serverName, remoteProcedures);
    }

    @Override
    public void run() {
      remoteCallFailed(master.getMasterProcedureExecutor().getEnvironment(),
        new RegionServerStoppedException("Server " + getServerName() + " is not online"));
    }
  }

}
