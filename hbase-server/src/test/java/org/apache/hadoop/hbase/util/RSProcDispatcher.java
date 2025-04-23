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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

/**
 * Test implementation of RSProcedureDispatcher that throws desired errors for testing purpose.
 */
public class RSProcDispatcher extends RSProcedureDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(RSProcDispatcher.class);

  private static final AtomicInteger I = new AtomicInteger();

  private static final List<IOException> ERRORS =
    Arrays.asList(new ConnectionClosedException("test connection closed error..."),
      new UnknownHostException("test unknown host error..."));
  private static final AtomicInteger ERROR_IDX = new AtomicInteger();

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
      int j = I.addAndGet(1);
      LOG.info("sendRequest() req: {} , j: {}", request, j);
      if (j == 12 || j == 22) {
        // Execute the remote close and open region requests in the last (5th) retry before
        // throwing ConnectionClosedException. This is to ensure even if the region open/close
        // is successfully completed by regionserver, master still schedules SCP because
        // sendRequest() throws error which has retry-limit exhausted.
        FutureUtils.get(getRsAdmin().executeProcedures(request));
      }
      // For one of the close region requests and one of the open region requests,
      // throw ConnectionClosedException until retry limit is exhausted and master
      // schedules recoveries for the server.
      // We will have ABNORMALLY_CLOSED regions, and they are expected to recover on their own.
      if (j >= 8 && j <= 13 || j >= 18 && j <= 23) {
        throw ERRORS.get(ERROR_IDX.getAndIncrement() % ERRORS.size());
      }
      return FutureUtils.get(getRsAdmin().executeProcedures(request));
    }

    private AsyncRegionServerAdmin getRsAdmin() {
      return master.getAsyncClusterConnection().getRegionServerAdmin(getServerName());
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
