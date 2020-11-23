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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecService;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overrides commands to make use of coprocessor where possible. Only supports actions taken
 * against Master and Region Server hosts.
 */
@InterfaceAudience.Private
@SuppressWarnings("unused") // no way to test this without a distributed cluster.
public class CoprocClusterManager extends HBaseClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(CoprocClusterManager.class);
  private static final Set<ServiceType> supportedServices = buildSupportedServicesSet();

  @Override
  protected Pair<Integer, String> exec(String hostname, ServiceType service, String... cmd)
    throws IOException {
    if (!supportedServices.contains(service)) {
      throw unsupportedServiceType(service);
    }

    // We only support actions vs. Master or Region Server processes. We're issuing those actions
    // via the coprocessor that's running within those processes. Thus, there's no support for
    // honoring the configured service user.
    final String command = StringUtils.join(cmd, " ");
    LOG.info("Executing remote command: {}, hostname:{}", command, hostname);

    try (final AsyncConnection conn = ConnectionFactory.createAsyncConnection(getConf()).join()) {
      final AsyncAdmin admin = conn.getAdmin();
      final ShellExecRequest req = ShellExecRequest.newBuilder()
        .setCommand(command)
        .setAwaitResponse(false)
        .build();

      final ShellExecResponse resp;
      switch(service) {
        case HBASE_MASTER:
          // What happens if the intended action was killing a backup master? Right now we have
          // no `RestartBackupMasterAction` so it's probably fine.
          resp = masterExec(admin, req);
          break;
        case HBASE_REGIONSERVER:
          final ServerName targetHost = resolveRegionServerName(admin, hostname);
          resp = regionServerExec(admin, req, targetHost);
          break;
        default:
          throw new RuntimeException("should not happen");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Executed remote command: {}, exit code:{} , output:{}", command, resp.getExitCode(),
          resp.getStdout());
      } else {
        LOG.info("Executed remote command: {}, exit code:{}", command, resp.getExitCode());
      }
      return new Pair<>(resp.getExitCode(), resp.getStdout());
    }
  }

  private static Set<ServiceType> buildSupportedServicesSet() {
    final Set<ServiceType> set = new HashSet<>();
    set.add(ServiceType.HBASE_MASTER);
    set.add(ServiceType.HBASE_REGIONSERVER);
    return Collections.unmodifiableSet(set);
  }

  private static ShellExecResponse masterExec(final AsyncAdmin admin,
    final ShellExecRequest req) {
    // TODO: Admin API provides no means of sending exec to a backup master.
    return admin.<ShellExecService.Stub, ShellExecResponse>coprocessorService(
      ShellExecService::newStub,
      (stub, controller, callback) -> stub.shellExec(controller, req, callback))
      .join();
  }

  private static ShellExecResponse regionServerExec(final AsyncAdmin admin,
    final ShellExecRequest req, final ServerName targetHost) {
    return admin.<ShellExecService.Stub, ShellExecResponse>coprocessorService(
      ShellExecService::newStub,
      (stub, controller, callback) -> stub.shellExec(controller, req, callback),
      targetHost)
      .join();
  }

  private static ServerName resolveRegionServerName(final AsyncAdmin admin,
    final String hostname) {
    return admin.getRegionServers()
      .thenApply(names -> names.stream()
        .filter(sn -> Objects.equals(sn.getHostname(), hostname))
        .findAny())
      .join()
      .orElseThrow(() -> serverNotFound(hostname));
  }

  private static RuntimeException serverNotFound(final String hostname) {
    return new RuntimeException(
      String.format("Did not find %s amongst the servers known to the client.", hostname));
  }

  private static RuntimeException unsupportedServiceType(final ServiceType serviceType) {
    return new RuntimeException(
      String.format("Unable to service request for service=%s", serviceType));
  }
}
