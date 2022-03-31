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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

@InterfaceAudience.Private
public final class ServerConnectionUtils {

  private ServerConnectionUtils() {
  }

  /**
   * A ClusterConnection that will short-circuit RPC making direct invocations against the localhost
   * if the invocation target is 'this' server; save on network and protobuf invocations.
   */
  // TODO This has to still do PB marshalling/unmarshalling stuff. Check how/whether we can avoid.
  // Class is visible so can assert we are short-circuiting when expected.
  public final static class ShortCircuitingClusterConnection extends ConnectionImplementation {
    private final ServerName serverName;
    private final AdminService.BlockingInterface localHostAdmin;
    private final ClientService.BlockingInterface localHostClient;
    private final ClientService.BlockingInterface localClientServiceBlockingInterfaceWrapper;

    private ShortCircuitingClusterConnection(Configuration conf, ExecutorService pool, User user,
        ServerName serverName, AdminService.BlockingInterface admin,
        ClientService.BlockingInterface client) throws IOException {
      super(conf, pool, user);
      this.serverName = serverName;
      this.localHostAdmin = admin;
      this.localHostClient = client;
      this.localClientServiceBlockingInterfaceWrapper =
          new ClientServiceBlockingInterfaceWrapper(this.localHostClient);
    }

    @Override
    public AdminService.BlockingInterface getAdmin(ServerName sn) throws IOException {
      return serverName.equals(sn) ? this.localHostAdmin : super.getAdmin(sn);
    }

    @Override
    public ClientService.BlockingInterface getClient(ServerName sn) throws IOException {
      return serverName.equals(sn) ? this.localClientServiceBlockingInterfaceWrapper
          : super.getClient(sn);
    }

    @Override
    public MasterKeepAliveConnection getMaster() throws IOException {
      if (this.localHostClient instanceof MasterService.BlockingInterface) {
        return new ShortCircuitMasterConnection(
            (MasterService.BlockingInterface) this.localHostClient);
      }
      return super.getMaster();
    }

    /**
     * When we directly invoke {@link RSRpcServices#get} on the same RegionServer through
     * {@link ShortCircuitingClusterConnection} in region CPs such as
     * {@link RegionObserver#postScannerOpen} to get other rows, the {@link RegionScanner} created
     * for the directly {@link RSRpcServices#get} may not be closed until the outmost rpc call is
     * completed if there is an outmost {@link RpcCall}, and even worse , the
     * {@link ServerCall#rpcCallback} may be override which would cause serious problem,so for
     * {@link ShortCircuitingClusterConnection#getClient}, if return
     * {@link ShortCircuitingClusterConnection#localHostClient},we would add a wrapper class to wrap
     * it , which using {@link RpcServer#unsetCurrentCall} and {RpcServer#setCurrentCall} to
     * surround the scan and get method call,so the {@link RegionScanner} created for the directly
     * {@link RSRpcServices#get} could be closed immediately,see HBASE-26812 for more.
     */
    static class ClientServiceBlockingInterfaceWrapper
        implements ClientService.BlockingInterface {

      private ClientService.BlockingInterface target;

      ClientServiceBlockingInterfaceWrapper(ClientService.BlockingInterface target) {
        this.target = target;
      }

      @Override
      public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
        return this.doCall(controller, request, (c, r) -> {
          return target.get(c, r);
        });
      }

      @Override
      public MultiResponse multi(RpcController controller, MultiRequest request)
          throws ServiceException {
        /**
         * Here is for multiGet
         */
        return this.doCall(controller, request, (c, r) -> {
          return target.multi(c, r);
        });
      }

      @Override
      public ScanResponse scan(RpcController controller, ScanRequest request)
          throws ServiceException {
        return this.doCall(controller, request, (c, r) -> {
          return target.scan(c, r);
        });
      }

      interface Operation<REQUEST, RESPONSE> {
        RESPONSE call(RpcController controller, REQUEST request) throws ServiceException;
      }

      private <REQUEST, RESPONSE> RESPONSE doCall(RpcController controller, REQUEST request,
          Operation<REQUEST, RESPONSE> operation) throws ServiceException {
        Optional<RpcCall> rpcCallOptional = RpcServer.unsetCurrentCall();
        try {
          return operation.call(controller, request);
        } finally {
          rpcCallOptional.ifPresent(RpcServer::setCurrentCall);
        }
      }

      @Override
      public MutateResponse mutate(RpcController controller, MutateRequest request)
          throws ServiceException {
        return target.mutate(controller, request);
      }

      @Override
      public BulkLoadHFileResponse bulkLoadHFile(RpcController controller,
          BulkLoadHFileRequest request) throws ServiceException {
        return target.bulkLoadHFile(controller, request);
      }

      @Override
      public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller,
          PrepareBulkLoadRequest request) throws ServiceException {
        return target.prepareBulkLoad(controller, request);
      }

      @Override
      public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller,
          CleanupBulkLoadRequest request) throws ServiceException {
        return target.cleanupBulkLoad(controller, request);
      }

      @Override
      public CoprocessorServiceResponse execService(RpcController controller,
          CoprocessorServiceRequest request) throws ServiceException {
        return target.execService(controller, request);
      }

      @Override
      public CoprocessorServiceResponse execRegionServerService(RpcController controller,
          CoprocessorServiceRequest request) throws ServiceException {
        return target.execRegionServerService(controller, request);
      }
    }
  }

  /**
   * Creates a short-circuit connection that can bypass the RPC layer (serialization,
   * deserialization, networking, etc..) when talking to a local server.
   * @param conf the current configuration
   * @param user the user the connection is for
   * @param serverName the local server name
   * @param admin the admin interface of the local server
   * @param client the client interface of the local server
   * @param registry the connection registry to be used, can be null
   * @return an short-circuit connection.
   * @throws IOException if IO failure occurred
   */
  public static ClusterConnection createShortCircuitConnection(final Configuration conf,
      ExecutorService pool, User user, final ServerName serverName,
      final AdminService.BlockingInterface admin, final ClientService.BlockingInterface client)
      throws IOException {
    if (user == null) {
      user = UserProvider.instantiate(conf).getCurrent();
    }
    return new ShortCircuitingClusterConnection(conf, pool, user, serverName, admin, client);
  }

}
