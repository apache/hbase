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
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Connection to an HTable from within a Coprocessor. We can do some nice tricks since we know we
 * are on a regionserver, for instance skipping the full serialization/deserialization of objects
 * when talking to the server.
 * <p>
 * You should not use this class from any client - its an internal class meant for use by the
 * coprocessor framework.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CoprocessorHConnection extends HConnectionImplementation {

  /**
   * Create an unmanaged {@link HConnection} based on the environment in which we are running the
   * coprocessor. The {@link HConnection} must be externally cleaned up (we bypass the usual HTable
   * cleanup mechanisms since we own everything).
   * @param env environment hosting the {@link HConnection}
   * @return an unmanaged {@link HConnection}.
   * @throws IOException if we cannot create the connection
   */
  public static HConnection getConnectionForEnvironment(CoprocessorEnvironment env)
      throws IOException {
    // this bit is a little hacky - just trying to get it going for the moment
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
      RegionServerServices services = e.getRegionServerServices();
      if (services instanceof HRegionServer) {
        return new CoprocessorHConnection((HRegionServer) services);
      }
    }
    return HConnectionManager.createConnection(env.getConfiguration());
  }

  private final ServerName serverName;
  private final HRegionServer server;

  /**
   * Legacy constructor
   * @param delegate
   * @param server
   * @throws IOException if we cannot create the connection
   * @deprecated delegate is not used
   */
  @Deprecated
  public CoprocessorHConnection(HConnection delegate, HRegionServer server)
      throws IOException {
    this(server);
  }

  /**
   * Constructor that uses server configuration
   * @param server
   * @throws IOException if we cannot create the connection
   */
  public CoprocessorHConnection(HRegionServer server) throws IOException {
    this(server.getConfiguration(), server);
  }

  /**
   * Constructor that accepts custom configuration
   * @param conf
   * @param server
   * @throws IOException if we cannot create the connection
   */
  public CoprocessorHConnection(Configuration conf, HRegionServer server) throws IOException {
    super(conf, false, null, UserProvider.instantiate(conf).getCurrent());
    this.server = server;
    this.serverName = server.getServerName();
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface
      getClient(ServerName serverName) throws IOException {
    // client is trying to reach off-server, so we can't do anything special
    if (!this.serverName.equals(serverName)) {
      return super.getClient(serverName);
    }
    // the client is attempting to write to the same regionserver, we can short-circuit to our
    // local regionserver
    final BlockingService blocking = ClientService.newReflectiveBlockingService(this.server);
    final RpcServerInterface rpc = this.server.getRpcServer();
    final MonitoredRPCHandler status = TaskMonitor.get().createRPCStatus(Thread.currentThread()
        .getName());
    status.pause("Setting up server-local call");
    final long timestamp = EnvironmentEdgeManager.currentTimeMillis();
    BlockingRpcChannel channel = new BlockingRpcChannel() {
      @Override
      public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
          Message request, Message responsePrototype) throws ServiceException {
        try {
          Pair<Message, CellScanner> ret = rpc.call(blocking, method, request, null, timestamp,
            status);
          if (ret.getSecond() != null) {
            PayloadCarryingRpcController rpcc = (PayloadCarryingRpcController) controller;
            rpcc.setCellScanner(ret.getSecond());
          }
          return ret.getFirst();
        } catch (IOException e) {
          throw new ServiceException(e);
        }
      }
    };
    return ClientService.newBlockingStub(channel);
  }
}
