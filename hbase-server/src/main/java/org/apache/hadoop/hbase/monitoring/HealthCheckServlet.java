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
package org.apache.hadoop.hbase.monitoring;

import java.io.IOException;
import java.util.Optional;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RpcConnectionRegistry;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class HealthCheckServlet<T extends HRegionServer> extends HttpServlet {

  private static final String CLIENT_RPC_TIMEOUT = "healthcheck.hbase.client.rpc.timeout";
  private static final int CLIENT_RPC_TIMEOUT_DEFAULT = 5000;
  private static final String CLIENT_RETRIES = "healthcheck.hbase.client.retries";
  private static final int CLIENT_RETRIES_DEFAULT = 2;
  private static final String CLIENT_OPERATION_TIMEOUT =
    "healthcheck.hbase.client.operation.timeout";
  private static final int CLIENT_OPERATION_TIMEOUT_DEFAULT = 15000;

  private final String serverLookupKey;

  public HealthCheckServlet(String serverLookupKey) {
    this.serverLookupKey = serverLookupKey;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    T server = (T) getServletContext().getAttribute(serverLookupKey);
    try {
      check(server, req);
      Optional<String> message = check(server, req);
      resp.setStatus(200);
      resp.getWriter().write(message.orElse("ok"));
    } catch (Exception e) {
      resp.setStatus(500);
      resp.getWriter().write(e.toString());
    } finally {
      resp.getWriter().close();
    }
  }

  private Optional<String> check(T server, HttpServletRequest req) throws IOException {
    if (server == null) {
      throw new IOException("Unable to get access to " + serverLookupKey);
    }
    if (server.isAborted() || server.isStopped() || server.isStopping() || server.isKilled()) {
      throw new IOException("The " + serverLookupKey + " is stopping!");
    }
    if (!server.getRpcServer().isStarted()) {
      throw new IOException("The " + serverLookupKey + "'s RpcServer is not started");
    }

    Configuration conf = new Configuration(server.getConfiguration());
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      RpcConnectionRegistry.class.getName());
    conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES, server.getServerName().getAddress().toString());
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

      return check(server, req, conn);
    }
  }

  protected abstract Optional<String> check(T server, HttpServletRequest req, Connection conn)
    throws IOException;
}
