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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RemoteProcedureResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;

/**
 * A thread which calls {@code reportProcedureDone} to tell master the result of a remote procedure.
 */
@InterfaceAudience.Private
class RemoteProcedureResultReporter extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProcedureResultReporter.class);

  // Time to pause if master says 'please hold'. Make configurable if needed.
  private static final int INIT_PAUSE_TIME_MS = 1000;

  private static final int MAX_BATCH = 100;

  private final HRegionServer server;

  private final LinkedBlockingQueue<RemoteProcedureResult> results = new LinkedBlockingQueue<>();

  public RemoteProcedureResultReporter(HRegionServer server) {
    this.server = server;
  }

  public void complete(long procId, Throwable error) {
    RemoteProcedureResult.Builder builder = RemoteProcedureResult.newBuilder().setProcId(procId);
    if (error != null) {
      LOG.debug("Failed to complete execution of pid={}", procId, error);
      builder.setStatus(RemoteProcedureResult.Status.ERROR).setError(
        ForeignExceptionUtil.toProtoForeignException(server.getServerName().toString(), error));
    } else {
      LOG.debug("Successfully complete execution of pid={}", procId);
      builder.setStatus(RemoteProcedureResult.Status.SUCCESS);
    }
    results.add(builder.build());
  }

  @Override
  public void run() {
    ReportProcedureDoneRequest.Builder builder = ReportProcedureDoneRequest.newBuilder();
    int tries = 0;
    while (!server.isStopped()) {
      if (builder.getResultCount() == 0) {
        try {
          builder.addResult(results.take());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          continue;
        }
      }
      while (builder.getResultCount() < MAX_BATCH) {
        RemoteProcedureResult result = results.poll();
        if (result == null) {
          break;
        }
        builder.addResult(result);
      }
      ReportProcedureDoneRequest request = builder.build();
      try {
        server.reportProcedureDone(builder.build());
        builder.clear();
        tries = 0;
      } catch (IOException e) {
        boolean pause =
          e instanceof ServerNotRunningYetException || e instanceof PleaseHoldException;
        long pauseTime;
        if (pause) {
          // Do backoff else we flood the Master with requests.
          pauseTime = ConnectionUtils.getPauseTime(INIT_PAUSE_TIME_MS, tries);
        } else {
          pauseTime = INIT_PAUSE_TIME_MS; // Reset.
        }
        LOG.info("Failed procedure report " + TextFormat.shortDebugString(request) + "; retry (#" +
          tries + ")" + (pause ? " after " + pauseTime + "ms delay (Master is coming online...)."
            : " immediately."),
          e);
        Threads.sleep(pauseTime);
        tries++;
      }
    }
  }
}
