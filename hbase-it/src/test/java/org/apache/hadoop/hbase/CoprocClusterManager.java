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

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecService;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.util.Pair;
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
    LOG.info(String.format("Executing remote command: %s, hostname:%s", command, hostname));

    try (final Connection conn = ConnectionFactory.createConnection(getConf())) {
      final Admin admin = conn.getAdmin();
      final ShellExecRequest req = ShellExecRequest.newBuilder()
        .setCommand(command)
        .setAwaitResponse(false)
        .build();

      final ShellExecResponse resp;
      try {
        switch (service) {
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
      } catch (ServiceException se) {
        LOG.error(String.format("Error running command: %s, error: %s", command, se.getMessage()));
        // Wrap and re-throw, can be improved but didn't want to change exception handling
        // in the parent classes.
        throw new IOException(se);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Executed remote command: %s, exit code:%s , output:%s", command,
            resp.getExitCode(), resp.getStdout()));
      } else {
        LOG.info(String.format("Executed remote command: %s, exit code:%s", command,
            resp.getExitCode()));
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

  private static ShellExecResponse masterExec(final Admin admin,
    final ShellExecRequest req) throws ServiceException {
    // TODO: Admin API provides no means of sending exec to a backup master.
    return ShellExecService.newBlockingStub(
        admin.coprocessorService()).shellExec(new HBaseRpcControllerImpl(), req);
  }

  private static ShellExecResponse regionServerExec(final Admin admin,
    final ShellExecRequest req, final ServerName targetHost) throws ServiceException {
    return  ShellExecService.newBlockingStub(
        admin.coprocessorService(targetHost)).shellExec(new HBaseRpcControllerImpl(), req);
  }

  private static ServerName resolveRegionServerName(final Admin admin,
    final String hostname) throws IOException {
    Collection<ServerName> liveServers = admin.getClusterStatus().getServers();
    for (ServerName sname: liveServers) {
      if (sname.getServerName().equals(hostname)) {
        return sname;
      }
    }
    throw serverNotFound(hostname);
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
