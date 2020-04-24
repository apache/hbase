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
package org.apache.hadoop.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecResponse;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.util.Shell;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Receives shell commands from the client and executes them blindly. Intended only for use
 * by {@link ChaosMonkey} via {@link CoprocClusterManager}
 */
@InterfaceAudience.Private
public class ShellExecEndpointCoprocessor extends ShellExecEndpoint.ShellExecService implements
  MasterCoprocessor, RegionServerCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(ShellExecEndpointCoprocessor.class);

  public static final String BACKGROUND_DELAY_MS_KEY = "hbase.it.shellexeccoproc.async.delay.ms";
  public static final long DEFAULT_BACKGROUND_DELAY_MS = 1_000;

  private final ExecutorService backgroundExecutor;
  private Configuration conf;

  public ShellExecEndpointCoprocessor() {
    backgroundExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat(ShellExecEndpointCoprocessor.class.getSimpleName() + "-{}")
        .setDaemon(true)
        .setUncaughtExceptionHandler((t, e) -> LOG.warn("Thread {} threw", t, e))
        .build());
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singletonList(this);
  }

  @Override
  public void start(CoprocessorEnvironment env) {
    conf = env.getConfiguration();
  }

  @Override
  public void shellExec(
    final RpcController controller,
    final ShellExecRequest request,
    final RpcCallback<ShellExecResponse> done
  ) {
    final String command = request.getCommand();
    if (StringUtils.isBlank(command)) {
      throw new RuntimeException("Request contained an empty command.");
    }
    final boolean awaitResponse = !request.hasAwaitResponse() || request.getAwaitResponse();
    final String[] subShellCmd = new String[] { "/usr/bin/env", "bash", "-c", command };
    final Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(subShellCmd);

    final String msgFmt = "Executing command"
      + (!awaitResponse ? " on a background thread" : "") + ": {}";
    LOG.info(msgFmt, command);

    if (awaitResponse) {
      runForegroundTask(shell, controller, done);
    } else {
      runBackgroundTask(shell, done);
    }
  }

  private void runForegroundTask(
    final Shell.ShellCommandExecutor shell,
    final RpcController controller,
    final RpcCallback<ShellExecResponse> done
  ) {
    ShellExecResponse.Builder builder = ShellExecResponse.newBuilder();
    try {
      doExec(shell, builder);
    } catch (IOException e) {
      LOG.error("Failure launching process", e);
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  private void runBackgroundTask(
    final Shell.ShellCommandExecutor shell,
    final RpcCallback<ShellExecResponse> done
  ) {
    final long sleepDuration = conf.getLong(BACKGROUND_DELAY_MS_KEY, DEFAULT_BACKGROUND_DELAY_MS);
    backgroundExecutor.submit(() -> {
      try {
        // sleep first so that the RPC can ACK. race condition here as we have no means of blocking
        // until the IPC response has been acknowledged by the client.
        Thread.sleep(sleepDuration);
        doExec(shell, ShellExecResponse.newBuilder());
      } catch (InterruptedException e) {
        LOG.warn("Interrupted before launching process.", e);
      } catch (IOException e) {
        LOG.error("Failure launching process", e);
      }
    });
    done.run(ShellExecResponse.newBuilder().build());
  }

  /**
   * Execute {@code shell} and collect results into {@code builder} as side-effects.
   */
  private void doExec(
    final Shell.ShellCommandExecutor shell,
    final ShellExecResponse.Builder builder
  ) throws IOException {
    try {
      shell.execute();
      builder
        .setExitCode(shell.getExitCode())
        .setStdout(shell.getOutput());
    } catch (Shell.ExitCodeException e) {
      LOG.warn("Launched process failed", e);
      builder
        .setExitCode(e.getExitCode())
        .setStdout(shell.getOutput())
        .setStderr(e.getMessage());
    }
  }
}
