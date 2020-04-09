/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.security.User;

/**
 * Provides the coprocessor framework and environment for master oriented
 * operations.  {@link HMaster} interacts with the loaded coprocessors
 * through this class.
 */
@InterfaceAudience.Private
public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessorHost.MasterEnvironment> {

  private static final Log LOG = LogFactory.getLog(MasterCoprocessorHost.class);

  /**
   * Coprocessor environment extension providing access to master related
   * services.
   */
  static class MasterEnvironment extends CoprocessorHost.Environment
      implements MasterCoprocessorEnvironment {
    private final MasterServices masterServices;
    private final MetricRegistry metricRegistry;
    private final boolean supportGroupCPs;

    public MasterEnvironment(final Class<?> implClass, final Coprocessor impl,
        final int priority, final int seq, final Configuration conf,
        final MasterServices services) {
      super(impl, priority, seq, conf);
      this.masterServices = services;
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForMasterCoprocessor(implClass.getName());
      supportGroupCPs = !useLegacyMethod(impl.getClass(),
          "preBalanceRSGroup", ObserverContext.class, String.class);
    }

    @Override
    public MasterServices getMasterServices() {
      return masterServices;
    }

    @Override
    public MetricRegistry getMetricRegistryForMaster() {
      return metricRegistry;
    }

    @Override
    protected void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }
  }

  private MasterServices masterServices;

  public MasterCoprocessorHost(final MasterServices services, final Configuration conf) {
    super(services);
    this.conf = conf;
    this.masterServices = services;
    // Log the state of coprocessor loading here; should appear only once or
    // twice in the daemon log, depending on HBase version, because there is
    // only one MasterCoprocessorHost instance in the master process
    boolean coprocessorsEnabled = conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_COPROCESSORS_ENABLED);
    LOG.info("System coprocessor loading is " + (coprocessorsEnabled ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterEnvironment createEnvironment(final Class<?> implClass,
      final Coprocessor instance, final int priority, final int seq,
      final Configuration conf) {
    for (Object itf : ClassUtils.getAllInterfaces(implClass)) {
      Class<?> c = (Class<?>) itf;
      if (CoprocessorService.class.isAssignableFrom(c)) {
        masterServices.registerService(((CoprocessorService)instance).getService());
      }
    }
    return new MasterEnvironment(implClass, instance, priority, seq, conf,
        masterServices);
  }

  public boolean preCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCreateNamespace(ctx, ns);
      }
    });
  }

  public void postCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCreateNamespace(ctx, ns);
      }
    });
  }

  public boolean preDeleteNamespace(final String namespaceName) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteNamespace(ctx, namespaceName);
      }
    });
  }

  public void postDeleteNamespace(final String namespaceName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteNamespace(ctx, namespaceName);
      }
    });
  }

  public boolean preModifyNamespace(final NamespaceDescriptor ns) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preModifyNamespace(ctx, ns);
      }
    });
  }

  public void postModifyNamespace(final NamespaceDescriptor ns) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postModifyNamespace(ctx, ns);
      }
    });
  }

  public void preGetNamespaceDescriptor(final String namespaceName)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preGetNamespaceDescriptor(ctx, namespaceName);
      }
    });
  }

  public void postGetNamespaceDescriptor(final NamespaceDescriptor ns)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postGetNamespaceDescriptor(ctx, ns);
      }
    });
  }

  public boolean preListNamespaces(final List<String> namespaces) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preListNamespaces(ctx, namespaces);
      }
    });
  }

  public boolean postListNamespaces(final List<String> namespaces) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postListNamespaces(ctx, namespaces);
      }
    });
  }

  public boolean preListNamespaceDescriptors(final List<NamespaceDescriptor> descriptors)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preListNamespaceDescriptors(ctx, descriptors);
      }
    });
  }

  public void postListNamespaceDescriptors(final List<NamespaceDescriptor> descriptors)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postListNamespaceDescriptors(ctx, descriptors);
      }
    });
  }

  /* Implementation of hooks for invoking MasterObservers */

  public void preCreateTable(final HTableDescriptor htd, final HRegionInfo[] regions)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCreateTable(ctx, htd, regions);
      }
    });
  }

  public void postCreateTable(final HTableDescriptor htd, final HRegionInfo[] regions)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCreateTable(ctx, htd, regions);
      }
    });
  }

  public void preCreateTableHandler(final HTableDescriptor htd, final HRegionInfo[] regions,
      final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCreateTableHandler(ctx, htd, regions);
      }
    });
  }

  public void postCreateTableHandler(final HTableDescriptor htd, final HRegionInfo[] regions,
      final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCreateTableHandler(ctx, htd, regions);
      }
    });
  }

  public void preDeleteTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteTable(ctx, tableName);
      }
    });
  }

  public void postDeleteTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteTable(ctx, tableName);
      }
    });
  }

  public void preDeleteTableHandler(final TableName tableName, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteTableHandler(ctx, tableName);
      }
    });
  }

  public void postDeleteTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteTableHandler(ctx, tableName);
      }
    });
  }

  public void preTruncateTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preTruncateTable(ctx, tableName);
      }
    });
  }

  public void postTruncateTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postTruncateTable(ctx, tableName);
      }
    });
  }

  public void preTruncateTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preTruncateTableHandler(ctx, tableName);
      }
    });
  }

  public void postTruncateTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postTruncateTableHandler(ctx, tableName);
      }
    });
  }

  public void preModifyTable(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preModifyTable(ctx, tableName, htd);
      }
    });
  }

  public void postModifyTable(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postModifyTable(ctx, tableName, htd);
      }
    });
  }

  public void preModifyTableHandler(final TableName tableName, final HTableDescriptor htd,
                                    final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preModifyTableHandler(ctx, tableName, htd);
      }
    });
  }

  public void postModifyTableHandler(final TableName tableName, final HTableDescriptor htd,
                                     final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postModifyTableHandler(ctx, tableName, htd);
      }
    });
  }

  public boolean preAddColumn(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preAddColumn(ctx, tableName, column);
      }
    });
  }

  public void postAddColumn(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postAddColumn(ctx, tableName, column);
      }
    });
  }

  public boolean preAddColumnHandler(final TableName tableName, final HColumnDescriptor column,
                                     final User user)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preAddColumnHandler(ctx, tableName, column);
      }
    });
  }

  public void postAddColumnHandler(final TableName tableName, final HColumnDescriptor column,
                                   final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postAddColumnHandler(ctx, tableName, column);
      }
    });
  }

  public boolean preModifyColumn(final TableName tableName, final HColumnDescriptor descriptor)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preModifyColumn(ctx, tableName, descriptor);
      }
    });
  }

  public void postModifyColumn(final TableName tableName, final HColumnDescriptor descriptor)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postModifyColumn(ctx, tableName, descriptor);
      }
    });
  }

  public boolean preModifyColumnHandler(final TableName tableName,
      final HColumnDescriptor descriptor, final User user) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preModifyColumnHandler(ctx, tableName, descriptor);
      }
    });
  }

  public void postModifyColumnHandler(final TableName tableName,
      final HColumnDescriptor descriptor, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postModifyColumnHandler(ctx, tableName, descriptor);
      }
    });
  }

  public boolean preDeleteColumn(final TableName tableName, final byte [] c) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteColumn(ctx, tableName, c);
      }
    });
  }

  public void postDeleteColumn(final TableName tableName, final byte [] c) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteColumn(ctx, tableName, c);
      }
    });
  }

  public boolean preDeleteColumnHandler(final TableName tableName, final byte[] c,
                                        final User user)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteColumnHandler(ctx, tableName, c);
      }
    });
  }

  public void postDeleteColumnHandler(final TableName tableName, final byte[] c,
                                      final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteColumnHandler(ctx, tableName, c);
      }
    });
  }

  public void preEnableTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preEnableTable(ctx, tableName);
      }
    });
  }

  public void postEnableTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postEnableTable(ctx, tableName);
      }
    });
  }

  public void preEnableTableHandler(final TableName tableName, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preEnableTableHandler(ctx, tableName);
      }
    });
  }

  public void postEnableTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postEnableTableHandler(ctx, tableName);
      }
    });
  }

  public void preDisableTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDisableTable(ctx, tableName);
      }
    });
  }

  public void postDisableTable(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDisableTable(ctx, tableName);
      }
    });
  }

  public void preDisableTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDisableTableHandler(ctx, tableName);
      }
    });
  }

  public void postDisableTableHandler(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDisableTableHandler(ctx, tableName);
      }
    });
  }

  public boolean preAbortProcedure(
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preAbortProcedure(ctx, procEnv, procId);
      }
    });
  }

  public void postAbortProcedure() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postAbortProcedure(ctx);
      }
    });
  }

  public boolean preListProcedures() throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preListProcedures(ctx);
      }
    });
  }

  public void postListProcedures(final List<ProcedureInfo> procInfoList) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postListProcedures(ctx, procInfoList);
      }
    });
  }

  public boolean preMove(final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preMove(ctx, region, srcServer, destServer);
      }
    });
  }

  public void postMove(final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postMove(ctx, region, srcServer, destServer);
      }
    });
  }

  public boolean preAssign(final HRegionInfo regionInfo) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preAssign(ctx, regionInfo);
      }
    });
  }

  public void postAssign(final HRegionInfo regionInfo) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postAssign(ctx, regionInfo);
      }
    });
  }

  public boolean preUnassign(final HRegionInfo regionInfo, final boolean force)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preUnassign(ctx, regionInfo, force);
      }
    });
  }

  public void postUnassign(final HRegionInfo regionInfo, final boolean force) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postUnassign(ctx, regionInfo, force);
      }
    });
  }

  public void preRegionOffline(final HRegionInfo regionInfo) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preRegionOffline(ctx, regionInfo);
      }
    });
  }

  public void postRegionOffline(final HRegionInfo regionInfo) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postRegionOffline(ctx, regionInfo);
      }
    });
  }

  public void preDispatchMerge(final HRegionInfo regionInfoA, final HRegionInfo regionInfoB)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDispatchMerge(ctx, regionInfoA, regionInfoB);
      }
    });
  }

  public void postDispatchMerge(final HRegionInfo regionInfoA, final HRegionInfo regionInfoB)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDispatchMerge(ctx, regionInfoA, regionInfoB);
      }
    });
  }

  public boolean preBalance() throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preBalance(ctx);
      }
    });
  }

  public void postBalance(final List<RegionPlan> plans) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postBalance(ctx, plans);
      }
    });
  }

  public boolean preSetSplitOrMergeEnabled(final boolean newValue,
      final Admin.MasterSwitchType switchType) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null :
      new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetSplitOrMergeEnabled(ctx, newValue, switchType);
      }
    });
  }

  public void postSetSplitOrMergeEnabled(final boolean newValue,
      final Admin.MasterSwitchType switchType) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetSplitOrMergeEnabled(ctx, newValue, switchType);
      }
    });
  }

  public boolean preBalanceSwitch(final boolean b) throws IOException {
    return execOperationWithResult(b, coprocessors.isEmpty() ? null :
        new CoprocessorOperationWithResult<Boolean>() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preBalanceSwitch(ctx, getResult()));
      }
    });
  }

  public void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postBalanceSwitch(ctx, oldValue, newValue);
      }
    });
  }

  public void preShutdown() throws IOException {
    // While stopping the cluster all coprocessors method should be executed first then the
    // coprocessor should be cleaned up.
    execShutdown(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preShutdown(ctx);
      }
      @Override
      public void postEnvCall(MasterEnvironment env) {
        // invoke coprocessor stop method
        shutdown(env);
      }
    });
  }

  public void preStopMaster() throws IOException {
    // While stopping master all coprocessors method should be executed first then the coprocessor
    // environment should be cleaned up.
    execShutdown(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preStopMaster(ctx);
      }
      @Override
      public void postEnvCall(MasterEnvironment env) {
        // invoke coprocessor stop method
        shutdown(env);
      }
    });
  }

  public void preMasterInitialization() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preMasterInitialization(ctx);
      }
    });
  }

  public void postStartMaster() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postStartMaster(ctx);
      }
    });
  }

  public void preSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void postSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void preListSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver observer, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        observer.preListSnapshot(ctx, snapshot);
      }
    });
  }

  public void postListSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver observer, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        observer.postListSnapshot(ctx, snapshot);
      }
    });
  }

  public void preCloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCloneSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void postCloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCloneSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void preRestoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preRestoreSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void postRestoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postRestoreSnapshot(ctx, snapshot, hTableDescriptor);
      }
    });
  }

  public void preDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDeleteSnapshot(ctx, snapshot);
      }
    });
  }

  public void postDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDeleteSnapshot(ctx, snapshot);
      }
    });
  }

  @Deprecated
  public boolean preGetTableDescriptors(final List<TableName> tableNamesList,
    final List<HTableDescriptor> descriptors) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preGetTableDescriptors(ctx, tableNamesList, descriptors);
      }
    });
  }

  @Deprecated
  public void postGetTableDescriptors(final List<HTableDescriptor> descriptors)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postGetTableDescriptors(ctx, descriptors);
      }
    });
  }

  public boolean preGetTableDescriptors(final List<TableName> tableNamesList,
      final List<HTableDescriptor> descriptors, final String regex) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preGetTableDescriptors(ctx, tableNamesList, descriptors, regex);
      }
    });
  }

  public void postGetTableDescriptors(final List<TableName> tableNamesList,
      final List<HTableDescriptor> descriptors, final String regex) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postGetTableDescriptors(ctx, tableNamesList, descriptors, regex);
      }
    });
  }

  public boolean preGetTableNames(final List<HTableDescriptor> descriptors,
      final String regex) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preGetTableNames(ctx, descriptors, regex);
      }
    });
  }

  public void postGetTableNames(final List<HTableDescriptor> descriptors,
      final String regex) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postGetTableNames(ctx, descriptors, regex);
      }
    });
  }

  public void preTableFlush(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preTableFlush(ctx, tableName);
      }
    });
  }

  public void postTableFlush(final TableName tableName) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postTableFlush(ctx, tableName);
      }
    });
  }

  public void preSetUserQuota(final String user, final Quotas quotas) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetUserQuota(ctx, user, quotas);
      }
    });
  }

  public void postSetUserQuota(final String user, final Quotas quotas) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetUserQuota(ctx, user, quotas);
      }
    });
  }

  public void preSetUserQuota(final String user, final TableName table, final Quotas quotas)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetUserQuota(ctx, user, table, quotas);
      }
    });
  }

  public void postSetUserQuota(final String user, final TableName table, final Quotas quotas)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetUserQuota(ctx, user, table, quotas);
      }
    });
  }

  public void preSetUserQuota(final String user, final String namespace, final Quotas quotas)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetUserQuota(ctx, user, namespace, quotas);
      }
    });
  }

  public void postSetUserQuota(final String user, final String namespace, final Quotas quotas)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetUserQuota(ctx, user, namespace, quotas);
      }
    });
  }

  public void preSetTableQuota(final TableName table, final Quotas quotas) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetTableQuota(ctx, table, quotas);
      }
    });
  }

  public void postSetTableQuota(final TableName table, final Quotas quotas) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetTableQuota(ctx, table, quotas);
      }
    });
  }

  public void preSetNamespaceQuota(final String namespace, final Quotas quotas) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSetNamespaceQuota(ctx, namespace, quotas);
      }
    });
  }

  public void postSetNamespaceQuota(final String namespace, final Quotas quotas)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSetNamespaceQuota(ctx, namespace, quotas);
      }
    });
  }

  public void preGetClusterStatus() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
              throws IOException {
        oserver.preGetClusterStatus(ctx);
      }
    });
  }

  public void postGetClusterStatus(final ClusterStatus status) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
              throws IOException {
        oserver.postGetClusterStatus(ctx, status);
      }
    });
  }

  public void preClearDeadServers() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
              throws IOException {
        oserver.preClearDeadServers(ctx);
      }
    });
  }

  public void postClearDeadServers(final List<ServerName> servers,
      final List<ServerName> notClearedServers) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver, ObserverContext<MasterCoprocessorEnvironment> ctx)
              throws IOException {
        oserver.postClearDeadServers(ctx, servers, notClearedServers);
      }
    });
  }

  public void preMoveServers(final Set<Address> servers, final String targetGroup)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preMoveServers(ctx, servers, targetGroup);
        }
      }
    });
  }

  public void postMoveServers(final Set<Address> servers, final String targetGroup)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postMoveServers(ctx, servers, targetGroup);
        }
      }
    });
  }

  public void preMoveTables(final Set<TableName> tables, final String targetGroup)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preMoveTables(ctx, tables, targetGroup);
        }
      }
    });
  }

  public void postMoveTables(final Set<TableName> tables, final String targetGroup)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postMoveTables(ctx, tables, targetGroup);
        }
      }
    });
  }

  public void preMoveServersAndTables(final Set<Address> servers, final Set<TableName> tables,
      final String targetGroup) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
                       ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preMoveServersAndTables(ctx, servers, tables, targetGroup);
        }
      }
    });
  }

  public void postMoveServersAndTables(final Set<Address> servers, final Set<TableName> tables,
      final String targetGroup) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
                       ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postMoveServersAndTables(ctx, servers, tables, targetGroup);
        }
      }
    });
  }

  public void preRemoveServers(final Set<Address> servers)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)getEnvironment()).supportGroupCPs) {
          oserver.preRemoveServers(this, servers);
        }
      }
    });
  }

  public void postRemoveServers(final Set<Address> servers)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)getEnvironment()).supportGroupCPs) {
          oserver.postRemoveServers(this, servers);
        }
      }
    });
  }

  public void preAddRSGroup(final String name)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preAddRSGroup(ctx, name);
        }
      }
    });
  }

  public void postAddRSGroup(final String name)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if (((MasterEnvironment) ctx.getEnvironment()).supportGroupCPs) {
          oserver.postAddRSGroup(ctx, name);
        }
      }
    });
  }

  public void preRemoveRSGroup(final String name)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preRemoveRSGroup(ctx, name);
        }
      }
    });
  }

  public void postRemoveRSGroup(final String name)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postRemoveRSGroup(ctx, name);
        }
      }
    });
  }

  public void preBalanceRSGroup(final String name)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preBalanceRSGroup(ctx, name);
        }
      }
    });
  }

  public void postBalanceRSGroup(final String name, final boolean balanceRan)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postBalanceRSGroup(ctx, name, balanceRan);
        }
      }
    });
  }

  public void preRenameRSGroup(final String oldName, final String newName)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null: new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.preRenameRSGroup(ctx, oldName, newName);
        }
      }
    });
  }

  public void postRenameRSGroup(final String oldName, final String newName)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null: new CoprocessorOperation() {
      @Override
      public void call(MasterObserver oserver,
          ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(((MasterEnvironment)ctx.getEnvironment()).supportGroupCPs) {
          oserver.postRenameRSGroup(ctx, oldName, newName);
        }
      }
    });
  }

  private static abstract class CoprocessorOperation
      extends ObserverContext<MasterCoprocessorEnvironment> {
    public CoprocessorOperation() {
      this(RpcServer.getRequestUser());
    }

    public CoprocessorOperation(User user) {
      super(user);
    }

    public abstract void call(MasterObserver oserver,
        ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException;

    public void postEnvCall(MasterEnvironment env) {
    }
  }

  private static abstract class CoprocessorOperationWithResult<T> extends CoprocessorOperation {
    private T result = null;
    public void setResult(final T result) { this.result = result; }
    public T getResult() { return this.result; }
  }

  private <T> T execOperationWithResult(final T defaultValue,
      final CoprocessorOperationWithResult<T> ctx) throws IOException {
    if (ctx == null) return defaultValue;
    ctx.setResult(defaultValue);
    execOperation(ctx);
    return ctx.getResult();
  }

  private boolean execOperation(final CoprocessorOperation ctx) throws IOException {
    if (ctx == null) return false;
    boolean bypass = false;
    List<MasterEnvironment> envs = coprocessors.get();
    for (int i = 0; i < envs.size(); i++) {
      MasterEnvironment env = envs.get(i);
      if (env.getInstance() instanceof MasterObserver) {
        ctx.prepare(env);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ctx.call((MasterObserver)env.getInstance(), ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
      ctx.postEnvCall(env);
    }
    return bypass;
  }

  /**
   * Master coprocessor classes can be configured in any order, based on that priority is set and
   * chained in a sorted order. For preStopMaster()/preShutdown(), coprocessor methods are invoked
   * in call() and environment is shutdown in postEnvCall(). <br>
   * Need to execute all coprocessor methods first then postEnvCall(), otherwise some coprocessors
   * may remain shutdown if any exception occurs during next coprocessor execution which prevent
   * Master stop or cluster shutdown. (Refer:
   * <a href="https://issues.apache.org/jira/browse/HBASE-16663">HBASE-16663</a>
   * @param ctx CoprocessorOperation
   * @return true if bypaas coprocessor execution, false if not.
   * @throws IOException
   */
  private boolean execShutdown(final CoprocessorOperation ctx) throws IOException {
    if (ctx == null) return false;
    boolean bypass = false;
    List<MasterEnvironment> envs = coprocessors.get();
    int envsSize = envs.size();
    // Iterate the coprocessors and execute CoprocessorOperation's call()
    for (int i = 0; i < envsSize; i++) {
      MasterEnvironment env = envs.get(i);
      if (env.getInstance() instanceof MasterObserver) {
        ctx.prepare(env);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ctx.call((MasterObserver) env.getInstance(), ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }

    // Iterate the coprocessors and execute CoprocessorOperation's postEnvCall()
    for (int i = 0; i < envsSize; i++) {
      MasterEnvironment env = envs.get(i);
      ctx.postEnvCall(env);
    }
    return bypass;
  }

}
