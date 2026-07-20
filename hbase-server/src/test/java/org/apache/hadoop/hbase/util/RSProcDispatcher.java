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
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Test implementation of RSProcedureDispatcher that throws desired errors for testing purpose.
 */
public class RSProcDispatcher extends RSProcedureDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(RSProcDispatcher.class);

  /** Config key for the fail-fast retry limit, shared with the test so the two cannot drift. */
  static final String FAIL_FAST_LIMIT_KEY = "hbase.master.rs.remote.proc.fail.fast.limit";

  private static final List<IOException> ERRORS =
    Arrays.asList(new ConnectionClosedException("test connection closed error..."),
      new UnknownHostException("test unknown host error..."),
      new ConnectException("test connect error..."));

  private static final AtomicInteger ERROR_IDX = new AtomicInteger();

  // Injection is driven by the test and bound to a target table, not a global call count:
  // remoteDispatch() fires for every remote procedure in the cluster (startup, table creation,
  // chores, background assignments), so counting calls drifts and misses the operations under test.
  private static final AtomicBoolean INJECT = new AtomicBoolean(false);
  private static final AtomicInteger VICTIMS_REMAINING = new AtomicInteger(0);
  private static volatile TableName targetTable;

  // Fail-fast retry limit after which the master schedules an SCP; read from conf to match test.
  private final int failFastLimit;

  /**
   * Fails the next {@code n} open/close-region requests for {@code table} with connection errors
   * until the fail-fast retry limit is exhausted, so the master schedules an SCP. Call right before
   * the operations under test.
   */
  static void injectErrorsForNextRequests(TableName table, int n) {
    ERROR_IDX.set(0);
    targetTable = table;
    VICTIMS_REMAINING.set(n);
    INJECT.set(true);
  }

  /** Stops error injection. Safe to call unconditionally, e.g. from test teardown. */
  static void stopInjecting() {
    INJECT.set(false);
    VICTIMS_REMAINING.set(0);
    targetTable = null;
  }

  public RSProcDispatcher(MasterServices master) {
    super(master);
    this.failFastLimit = master.getConfiguration().getInt(FAIL_FAST_LIMIT_KEY, 10);
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

  /**
   * True if the request opens or closes a region of the injection target table. Open requests carry
   * a full RegionInfo; close requests carry a REGION_NAME specifier the table is parsed from.
   */
  private static boolean targetsInjectionTable(AdminProtos.ExecuteProceduresRequest request) {
    TableName table = targetTable;
    if (table == null) {
      return false;
    }
    for (AdminProtos.OpenRegionRequest open : request.getOpenRegionList()) {
      for (AdminProtos.OpenRegionRequest.RegionOpenInfo info : open.getOpenInfoList()) {
        if (table.equals(ProtobufUtil.toTableName(info.getRegion().getTableName()))) {
          return true;
        }
      }
    }
    for (AdminProtos.CloseRegionRequest close : request.getCloseRegionList()) {
      HBaseProtos.RegionSpecifier region = close.getRegion();
      if (
        region.getType() == HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME
          && table.equals(RegionInfo.getTable(region.getValue().toByteArray()))
      ) {
        return true;
      }
    }
    return false;
  }

  class TestExecuteProceduresRemoteCall extends ExecuteProceduresRemoteCall {

    // attempts: retries of this single request instance (mirrors the dispatcher's
    // numberOfAttemptsSoFar). injectErrors: whether this instance is failed with injected errors,
    // decided once on the first call and kept across its retries.
    private int attempts = 0;
    private Boolean injectErrors = null;

    public TestExecuteProceduresRemoteCall(ServerName serverName,
      Set<RemoteProcedure> remoteProcedures) {
      super(serverName, remoteProcedures);
    }

    @Override
    public AdminProtos.ExecuteProceduresResponse sendRequest(final ServerName serverName,
      final AdminProtos.ExecuteProceduresRequest request) throws IOException {
      if (injectErrors == null) {
        // Claim a slot only for a target-table open/close request, once per instance.
        injectErrors =
          INJECT.get() && targetsInjectionTable(request) && VICTIMS_REMAINING.getAndDecrement() > 0;
      }
      LOG.info("sendRequest() req: {}, attempts: {}, injectErrors: {}", request, attempts,
        injectErrors);
      if (!injectErrors) {
        return FutureUtils.get(getRsAdmin().executeProcedures(request));
      }
      // Throw a connection error each attempt until the retry limit is exhausted (-> SCP). On the
      // last attempt run the real open/close first so the region still recovers.
      if (attempts++ >= failFastLimit - 1) {
        FutureUtils.get(getRsAdmin().executeProcedures(request));
      }
      throw ERRORS.get(ERROR_IDX.getAndIncrement() % ERRORS.size());
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
