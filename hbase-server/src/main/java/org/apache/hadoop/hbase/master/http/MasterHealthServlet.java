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
package org.apache.hadoop.hbase.master.http;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RpcConnectionRegistry;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.monitoring.HealthCheckServlet;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MasterHealthServlet extends HealthCheckServlet<HMaster> {

  private static final String CLIENT_RPC_TIMEOUT = "healthcheck.hbase.client.rpc.timeout";
  private static final int CLIENT_RPC_TIMEOUT_DEFAULT = 5000;
  private static final String CLIENT_RETRIES = "healthcheck.hbase.client.retries";
  private static final int CLIENT_RETRIES_DEFAULT = 2;
  private static final String CLIENT_OPERATION_TIMEOUT =
    "healthcheck.hbase.client.operation.timeout";
  private static final int CLIENT_OPERATION_TIMEOUT_DEFAULT = 15000;

  public MasterHealthServlet() {
    super(HMaster.MASTER);
  }

  @Override
  protected Optional<String> check(HMaster master, HttpServletRequest req) throws IOException {
    Configuration conf = new Configuration(master.getConfiguration());
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      RpcConnectionRegistry.class.getName());
    conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES, master.getServerName().getAddress().toString());
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      conf.getInt(CLIENT_RPC_TIMEOUT, CLIENT_RPC_TIMEOUT_DEFAULT));
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      conf.getInt(CLIENT_RETRIES, CLIENT_RETRIES_DEFAULT));
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      conf.getInt(CLIENT_OPERATION_TIMEOUT, CLIENT_OPERATION_TIMEOUT_DEFAULT));

    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      // this will fail if the server is not accepting requests
      if (conn.getClusterId() == null) {
        throw new IOException("Could not retrieve clusterId from self via rpc");
      }

      if (master.isActiveMaster() && master.isOnline()) {
        // this will fail if there is a problem with the active master
        conn.getAdmin().getClusterMetrics(EnumSet.of(ClusterMetrics.Option.CLUSTER_ID));
      }
    }

    return Optional.empty();
  }
}
