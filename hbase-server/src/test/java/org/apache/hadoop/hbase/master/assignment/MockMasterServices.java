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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterWalManager;
import org.apache.hadoop.hbase.master.MockNoopMasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * A mocked master services.
 * Tries to fake it. May not always work.
 */
public class MockMasterServices extends MockNoopMasterServices {
  private final MasterFileSystem fileSystemManager;
  private final MasterWalManager walManager;
  private final AssignmentManager assignmentManager;

  private MasterProcedureEnv procedureEnv;
  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private ProcedureStore procedureStore;
  private final ClusterConnection connection;
  private final LoadBalancer balancer;
  private final ServerManager serverManager;
  // Set of regions on a 'server'. Populated externally. Used in below faking 'cluster'.
  private final NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers;

  private final ProcedureEvent initialized = new ProcedureEvent("master initialized");
  public static final String DEFAULT_COLUMN_FAMILY_NAME = "cf";
  public static final ServerName MOCK_MASTER_SERVERNAME =
      ServerName.valueOf("mockmaster.example.org", 1234, -1L);

  public MockMasterServices(Configuration conf,
      NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers)
  throws IOException {
    super(conf);
    this.regionsToRegionServers = regionsToRegionServers;
    Superusers.initialize(conf);
    this.fileSystemManager = new MasterFileSystem(this);
    this.walManager = new MasterWalManager(this);
    // Mock an AM.
    this.assignmentManager = new AssignmentManager(this, new MockRegionStateStore(this)) {
      public boolean isTableEnabled(final TableName tableName) {
        return true;
      }

      public boolean isTableDisabled(final TableName tableName) {
        return false;
      }

      @Override
      protected boolean waitServerReportEvent(ServerName serverName, Procedure proc) {
        // Make a report with current state of the server 'serverName' before we call wait..
        SortedSet<byte []> regions = regionsToRegionServers.get(serverName);
        try {
          getAssignmentManager().reportOnlineRegions(serverName, 0,
              regions == null? new HashSet<byte []>(): regions);
        } catch (YouAreDeadException e) {
          throw new RuntimeException(e);
        }
        return super.waitServerReportEvent(serverName, proc);
      }
    };
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.serverManager = new ServerManager(this);

    // Mock up a Client Interface
    ClientProtos.ClientService.BlockingInterface ri =
        Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
    MutateResponse.Builder builder = MutateResponse.newBuilder();
    builder.setProcessed(true);
    try {
      Mockito.when(ri.mutate((RpcController)Mockito.any(), (MutateRequest)Mockito.any())).
        thenReturn(builder.build());
    } catch (ServiceException se) {
      throw ProtobufUtil.handleRemoteException(se);
    }
    try {
      Mockito.when(ri.multi((RpcController)Mockito.any(), (MultiRequest)Mockito.any())).
        thenAnswer(new Answer<MultiResponse>() {
          @Override
          public MultiResponse answer(InvocationOnMock invocation) throws Throwable {
            return buildMultiResponse( (MultiRequest)invocation.getArguments()[1]);
          }
        });
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
    // Mock n ClusterConnection and an AdminProtocol implementation. Have the
    // ClusterConnection return the HRI.  Have the HRI return a few mocked up responses
    // to make our test work.
    this.connection =
        HConnectionTestingUtility.getMockedConnectionAndDecorate(getConfiguration(),
          Mockito.mock(AdminProtos.AdminService.BlockingInterface.class), ri, MOCK_MASTER_SERVERNAME,
          HRegionInfo.FIRST_META_REGIONINFO);
    // Set hbase.rootdir into test dir.
    Path rootdir = FSUtils.getRootDir(getConfiguration());
    FSUtils.setRootDir(getConfiguration(), rootdir);
    Mockito.mock(AdminProtos.AdminService.BlockingInterface.class);
  }

  public void start(final int numServes, final RSProcedureDispatcher remoteDispatcher)
      throws IOException {
    startProcedureExecutor(remoteDispatcher);
    this.assignmentManager.start();
    for (int i = 0; i < numServes; ++i) {
      serverManager.regionServerReport(
        ServerName.valueOf("localhost", 100 + i, 1), ServerLoad.EMPTY_SERVERLOAD);
    }
    this.procedureExecutor.getEnvironment().setEventReady(initialized, true);
  }

  @Override
  public void stop(String why) {
    stopProcedureExecutor();
    this.assignmentManager.stop();
  }

  private void startProcedureExecutor(final RSProcedureDispatcher remoteDispatcher)
      throws IOException {
    final Configuration conf = getConfiguration();
    final Path logDir = new Path(fileSystemManager.getRootDir(),
        MasterProcedureConstants.MASTER_PROCEDURE_LOGDIR);

    //procedureStore = new WALProcedureStore(conf, fileSystemManager.getFileSystem(), logDir,
    //    new MasterProcedureEnv.WALStoreLeaseRecovery(this));
    this.procedureStore = new NoopProcedureStore();
    this.procedureStore.registerListener(new MasterProcedureEnv.MasterProcedureStoreListener(this));

    this.procedureEnv = new MasterProcedureEnv(this,
       remoteDispatcher != null ? remoteDispatcher : new RSProcedureDispatcher(this));

    this.procedureExecutor = new ProcedureExecutor(conf, procedureEnv, procedureStore,
        procedureEnv.getProcedureScheduler());

    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS,
        Math.max(Runtime.getRuntime().availableProcessors(),
          MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    final boolean abortOnCorruption = conf.getBoolean(
        MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION,
        MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
    this.procedureStore.start(numThreads);
    this.procedureExecutor.start(numThreads, abortOnCorruption);
    this.procedureEnv.getRemoteDispatcher().start();
  }

  private void stopProcedureExecutor() {
    if (this.procedureEnv != null) {
      this.procedureEnv.getRemoteDispatcher().stop();
    }

    if (this.procedureExecutor != null) {
      this.procedureExecutor.stop();
    }

    if (this.procedureStore != null) {
      this.procedureStore.stop(isAborted());
    }
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public ProcedureEvent getInitializedEvent() {
    return this.initialized;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return fileSystemManager;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return walManager;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return procedureExecutor;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
    return balancer;
  }

  @Override
  public ServerManager getServerManager() {
    return serverManager;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return assignmentManager;
  }

  @Override
  public ClusterConnection getConnection() {
    return this.connection;
  }

  @Override
  public ServerName getServerName() {
    return MOCK_MASTER_SERVERNAME;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return super.getCoordinatedStateManager();
  }

  private static class MockRegionStateStore extends RegionStateStore {
    public MockRegionStateStore(final MasterServices master) {
      super(master);
    }

    @Override
    public void start() throws IOException {
    }

    @Override
    public void stop() {
    }

    @Override
    public void updateRegionLocation(HRegionInfo regionInfo, State state, ServerName regionLocation,
        ServerName lastHost, long openSeqNum, long pid) throws IOException {
    }
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return new TableDescriptors() {
      @Override
      public TableDescriptor remove(TableName tablename) throws IOException {
        // noop
        return null;
      }

      @Override
      public Map<String, TableDescriptor> getAll() throws IOException {
        // noop
        return null;
      }

      @Override public Map<String, TableDescriptor> getAllDescriptors() throws IOException {
        // noop
        return null;
      }

      @Override
      public TableDescriptor get(TableName tablename) throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tablename);
        builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(DEFAULT_COLUMN_FAMILY_NAME));
        return builder.build();
      }

      @Override
      public Map<String, TableDescriptor> getByNamespace(String name) throws IOException {
        return null;
      }

      @Override
      public void add(TableDescriptor htd) throws IOException {
        // noop
      }

      @Override
      public void setCacheOn() throws IOException {
      }

      @Override
      public void setCacheOff() throws IOException {
      }
    };
  }

  private static MultiResponse buildMultiResponse(MultiRequest req) {
    MultiResponse.Builder builder = MultiResponse.newBuilder();
    RegionActionResult.Builder regionActionResultBuilder =
        RegionActionResult.newBuilder();
    ResultOrException.Builder roeBuilder = ResultOrException.newBuilder();
    for (RegionAction regionAction: req.getRegionActionList()) {
      regionActionResultBuilder.clear();
      for (ClientProtos.Action action: regionAction.getActionList()) {
        roeBuilder.clear();
        roeBuilder.setResult(ClientProtos.Result.getDefaultInstance());
        roeBuilder.setIndex(action.getIndex());
        regionActionResultBuilder.addResultOrException(roeBuilder.build());
      }
      builder.addRegionActionResult(regionActionResultBuilder.build());
    }
    return builder.build();
  }
}
