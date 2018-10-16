/*
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

import com.google.protobuf.Service;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SharedConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the coprocessor framework and environment for master oriented
 * operations.  {@link HMaster} interacts with the loaded coprocessors
 * through this class.
 */
@InterfaceAudience.Private
public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessor, MasterCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(MasterCoprocessorHost.class);

  /**
   * Coprocessor environment extension providing access to master related
   * services.
   */
  private static class MasterEnvironment extends BaseEnvironment<MasterCoprocessor>
      implements MasterCoprocessorEnvironment {
    private final MetricRegistry metricRegistry;
    private final MasterServices services;

    public MasterEnvironment(final MasterCoprocessor impl, final int priority, final int seq,
        final Configuration conf, final MasterServices services) {
      super(impl, priority, seq, conf);
      this.services = services;
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForMasterCoprocessor(impl.getClass().getName());
    }

    @Override
    public ServerName getServerName() {
      return this.services.getServerName();
    }

    @Override
    public Connection getConnection() {
      return new SharedConnection(this.services.getConnection());
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return this.services.createConnection(conf);
    }

    @Override
    public MetricRegistry getMetricRegistryForMaster() {
      return metricRegistry;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }
  }

  /**
   * Special version of MasterEnvironment that exposes MasterServices for Core Coprocessors only.
   * Temporary hack until Core Coprocessors are integrated into Core.
   */
  private static class MasterEnvironmentForCoreCoprocessors extends MasterEnvironment
      implements HasMasterServices {
    private final MasterServices masterServices;

    public MasterEnvironmentForCoreCoprocessors(final MasterCoprocessor impl, final int priority,
        final int seq, final Configuration conf, final MasterServices services) {
      super(impl, priority, seq, conf, services);
      this.masterServices = services;
    }

    /**
     * @return An instance of MasterServices, an object NOT for general user-space Coprocessor
     * consumption.
     */
    @Override
    public MasterServices getMasterServices() {
      return this.masterServices;
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
    LOG.trace("System coprocessor loading is {}",  (coprocessorsEnabled ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterEnvironment createEnvironment(final MasterCoprocessor instance, final int priority,
      final int seq, final Configuration conf) {
    // If coprocessor exposes any services, register them.
    for (Service service : instance.getServices()) {
      masterServices.registerService(service);
    }
    // If a CoreCoprocessor, return a 'richer' environment, one laden with MasterServices.
    return instance.getClass().isAnnotationPresent(CoreCoprocessor.class)?
        new MasterEnvironmentForCoreCoprocessors(instance, priority, seq, conf, masterServices):
        new MasterEnvironment(instance, priority, seq, conf, masterServices);
  }

  @Override
  public MasterCoprocessor checkAndGetInstance(Class<?> implClass)
      throws InstantiationException, IllegalAccessException {
    try {
      if (MasterCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(MasterCoprocessor.class).getDeclaredConstructor().newInstance();
      } else if (CoprocessorService.class.isAssignableFrom(implClass)) {
        // For backward compatibility with old CoprocessorService impl which don't extend
        // MasterCoprocessor.
        CoprocessorService cs;
        cs = implClass.asSubclass(CoprocessorService.class).getDeclaredConstructor().newInstance();
        return new CoprocessorServiceBackwardCompatiblity.MasterCoprocessorService(cs);
      } else {
        LOG.error("{} is not of type MasterCoprocessor. Check the configuration of {}",
            implClass.getName(), CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private ObserverGetter<MasterCoprocessor, MasterObserver> masterObserverGetter =
      MasterCoprocessor::getMasterObserver;

  abstract class MasterObserverOperation extends
      ObserverOperationWithoutResult<MasterObserver> {
    public MasterObserverOperation(){
      super(masterObserverGetter);
    }

    public MasterObserverOperation(boolean bypassable) {
      this(null, bypassable);
    }

    public MasterObserverOperation(User user) {
      super(masterObserverGetter, user);
    }

    public MasterObserverOperation(User user, boolean bypassable) {
      super(masterObserverGetter, user, bypassable);
    }
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////
  // MasterObserver operations
  //////////////////////////////////////////////////////////////////////////////////////////////////


  public void preCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preCreateNamespace(this, ns);
      }
    });
  }

  public void postCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCreateNamespace(this, ns);
      }
    });
  }

  public void preDeleteNamespace(final String namespaceName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDeleteNamespace(this, namespaceName);
      }
    });
  }

  public void postDeleteNamespace(final String namespaceName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDeleteNamespace(this, namespaceName);
      }
    });
  }

  public void preModifyNamespace(final NamespaceDescriptor currentNsDescriptor,
    final NamespaceDescriptor newNsDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preModifyNamespace(this, currentNsDescriptor, newNsDescriptor);
      }
    });
  }

  public void postModifyNamespace(final NamespaceDescriptor oldNsDescriptor,
    final NamespaceDescriptor currentNsDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postModifyNamespace(this, oldNsDescriptor, currentNsDescriptor);
      }
    });
  }

  public void preGetNamespaceDescriptor(final String namespaceName)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetNamespaceDescriptor(this, namespaceName);
      }
    });
  }

  public void postGetNamespaceDescriptor(final NamespaceDescriptor ns)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetNamespaceDescriptor(this, ns);
      }
    });
  }

  public void preListNamespaceDescriptors(final List<NamespaceDescriptor> descriptors)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preListNamespaceDescriptors(this, descriptors);
      }
    });
  }

  public void postListNamespaceDescriptors(final List<NamespaceDescriptor> descriptors)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postListNamespaceDescriptors(this, descriptors);
      }
    });
  }

  /* Implementation of hooks for invoking MasterObservers */

  public void preCreateTable(final TableDescriptor htd, final RegionInfo[] regions)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preCreateTable(this, htd, regions);
      }
    });
  }

  public void postCreateTable(final TableDescriptor htd, final RegionInfo[] regions)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCreateTable(this, htd, regions);
      }
    });
  }

  public void preCreateTableAction(final TableDescriptor htd, final RegionInfo[] regions,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preCreateTableAction(this, htd, regions);
      }
    });
  }

  public void postCompletedCreateTableAction(
      final TableDescriptor htd, final RegionInfo[] regions, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedCreateTableAction(this, htd, regions);
      }
    });
  }

  public void preDeleteTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDeleteTable(this, tableName);
      }
    });
  }

  public void postDeleteTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDeleteTable(this, tableName);
      }
    });
  }

  public void preDeleteTableAction(final TableName tableName, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDeleteTableAction(this, tableName);
      }
    });
  }

  public void postCompletedDeleteTableAction(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedDeleteTableAction(this, tableName);
      }
    });
  }

  public void preTruncateTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preTruncateTable(this, tableName);
      }
    });
  }

  public void postTruncateTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postTruncateTable(this, tableName);
      }
    });
  }

  public void preTruncateTableAction(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preTruncateTableAction(this, tableName);
      }
    });
  }

  public void postCompletedTruncateTableAction(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedTruncateTableAction(this, tableName);
      }
    });
  }

  public void preModifyTable(final TableName tableName, final TableDescriptor currentDescriptor,
    final TableDescriptor newDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preModifyTable(this, tableName, currentDescriptor, newDescriptor);
      }
    });
  }

  public void postModifyTable(final TableName tableName, final TableDescriptor oldDescriptor,
    final TableDescriptor currentDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postModifyTable(this, tableName, oldDescriptor, currentDescriptor);
      }
    });
  }

  public void preModifyTableAction(final TableName tableName,
    final TableDescriptor currentDescriptor, final TableDescriptor newDescriptor, final User user)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preModifyTableAction(this, tableName, currentDescriptor, newDescriptor);
      }
    });
  }

  public void postCompletedModifyTableAction(final TableName tableName,
    final TableDescriptor oldDescriptor, final TableDescriptor currentDescriptor, final User user)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedModifyTableAction(this, tableName, oldDescriptor, currentDescriptor);
      }
    });
  }

  public void preEnableTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preEnableTable(this, tableName);
      }
    });
  }

  public void postEnableTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postEnableTable(this, tableName);
      }
    });
  }

  public void preEnableTableAction(final TableName tableName, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preEnableTableAction(this, tableName);
      }
    });
  }

  public void postCompletedEnableTableAction(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedEnableTableAction(this, tableName);
      }
    });
  }

  public void preDisableTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDisableTable(this, tableName);
      }
    });
  }

  public void postDisableTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDisableTable(this, tableName);
      }
    });
  }

  public void preDisableTableAction(final TableName tableName, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDisableTableAction(this, tableName);
      }
    });
  }

  public void postCompletedDisableTableAction(final TableName tableName, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedDisableTableAction(this, tableName);
      }
    });
  }

  public void preAbortProcedure(
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preAbortProcedure(this,  procId);
      }
    });
  }

  public void postAbortProcedure() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postAbortProcedure(this);
      }
    });
  }

  public void preGetProcedures() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetProcedures(this);
      }
    });
  }

  public void postGetProcedures(final List<Procedure<?>> procInfoList) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetProcedures(this);
      }
    });
  }

  public void preGetLocks() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetLocks(this);
      }
    });
  }

  public void postGetLocks(final List<LockedResource> lockedResources) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetLocks(this);
      }
    });
  }

  public void preMove(final RegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMove(this, region, srcServer, destServer);
      }
    });
  }

  public void postMove(final RegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMove(this, region, srcServer, destServer);
      }
    });
  }

  public void preAssign(final RegionInfo regionInfo) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preAssign(this, regionInfo);
      }
    });
  }

  public void postAssign(final RegionInfo regionInfo) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postAssign(this, regionInfo);
      }
    });
  }

  public void preUnassign(final RegionInfo regionInfo, final boolean force)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preUnassign(this, regionInfo, force);
      }
    });
  }

  public void postUnassign(final RegionInfo regionInfo, final boolean force) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postUnassign(this, regionInfo, force);
      }
    });
  }

  public void preRegionOffline(final RegionInfo regionInfo) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRegionOffline(this, regionInfo);
      }
    });
  }

  public void postRegionOffline(final RegionInfo regionInfo) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRegionOffline(this, regionInfo);
      }
    });
  }

  public void preMergeRegions(final RegionInfo[] regionsToMerge)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMergeRegions(this, regionsToMerge);
      }
    });
  }

  public void postMergeRegions(final RegionInfo[] regionsToMerge)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMergeRegions(this, regionsToMerge);
      }
    });
  }

  public boolean preBalance() throws IOException {
    return execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preBalance(this);
      }
    });
  }

  public void postBalance(final List<RegionPlan> plans) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postBalance(this, plans);
      }
    });
  }

  public void preSetSplitOrMergeEnabled(final boolean newValue,
      final MasterSwitchType switchType) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetSplitOrMergeEnabled(this, newValue, switchType);
      }
    });
  }

  public void postSetSplitOrMergeEnabled(final boolean newValue,
      final MasterSwitchType switchType) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetSplitOrMergeEnabled(this, newValue, switchType);
      }
    });
  }

  /**
   * Invoked just before calling the split region procedure
   * @param tableName the table where the region belongs to
   * @param splitRow the split point
   * @throws IOException
   */
  public void preSplitRegion(
      final TableName tableName,
      final byte[] splitRow) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSplitRegion(this, tableName, splitRow);
      }
    });
  }

  /**
   * Invoked just before a split
   * @param tableName the table where the region belongs to
   * @param splitRow the split point
   * @param user the user
   * @throws IOException
   */
  public void preSplitRegionAction(
      final TableName tableName,
      final byte[] splitRow,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSplitRegionAction(this, tableName, splitRow);
      }
    });
  }

  /**
   * Invoked just after a split
   * @param regionInfoA the new left-hand daughter region
   * @param regionInfoB the new right-hand daughter region
   * @param user the user
   * @throws IOException
   */
  public void postCompletedSplitRegionAction(
      final RegionInfo regionInfoA,
      final RegionInfo regionInfoB,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedSplitRegionAction(this, regionInfoA, regionInfoB);
      }
    });
  }

  /**
   * This will be called before update META step as part of split table region procedure.
   * @param splitKey
   * @param metaEntries
   * @param user the user
   * @throws IOException
   */
  public void preSplitBeforeMETAAction(
      final byte[] splitKey,
      final List<Mutation> metaEntries,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSplitRegionBeforeMETAAction(this, splitKey, metaEntries);
      }
    });
  }

  /**
   * This will be called after update META step as part of split table region procedure.
   * @param user the user
   * @throws IOException
   */
  public void preSplitAfterMETAAction(final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSplitRegionAfterMETAAction(this);
      }
    });
  }

  /**
   * Invoked just after the rollback of a failed split
   * @param user the user
   * @throws IOException
   */
  public void postRollBackSplitRegionAction(final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRollBackSplitRegionAction(this);
      }
    });
  }

  /**
   * Invoked just before a merge
   * @param regionsToMerge the regions to merge
   * @param user the user
   * @throws IOException
   */
  public void preMergeRegionsAction(
      final RegionInfo[] regionsToMerge, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMergeRegionsAction(this, regionsToMerge);
      }
    });
  }

  /**
   * Invoked after completing merge regions operation
   * @param regionsToMerge the regions to merge
   * @param mergedRegion the new merged region
   * @param user the user
   * @throws IOException
   */
  public void postCompletedMergeRegionsAction(
      final RegionInfo[] regionsToMerge,
      final RegionInfo mergedRegion,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCompletedMergeRegionsAction(this, regionsToMerge, mergedRegion);
      }
    });
  }

  /**
   * Invoked before merge regions operation writes the new region to hbase:meta
   * @param regionsToMerge the regions to merge
   * @param metaEntries the meta entry
   * @param user the user
   * @throws IOException
   */
  public void preMergeRegionsCommit(
      final RegionInfo[] regionsToMerge,
      final @MetaMutationAnnotation List<Mutation> metaEntries,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMergeRegionsCommitAction(this, regionsToMerge, metaEntries);
      }
    });
  }

  /**
   * Invoked after merge regions operation writes the new region to hbase:meta
   * @param regionsToMerge the regions to merge
   * @param mergedRegion the new merged region
   * @param user the user
   * @throws IOException
   */
  public void postMergeRegionsCommit(
      final RegionInfo[] regionsToMerge,
      final RegionInfo mergedRegion,
      final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMergeRegionsCommitAction(this, regionsToMerge, mergedRegion);
      }
    });
  }

  /**
   * Invoked after rollback merge regions operation
   * @param regionsToMerge the regions to merge
   * @param user the user
   * @throws IOException
   */
  public void postRollBackMergeRegionsAction(
      final RegionInfo[] regionsToMerge, final User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation(user) {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRollBackMergeRegionsAction(this, regionsToMerge);
      }
    });
  }

  // This hook allows Coprocessor change value of balance switch.
  public void preBalanceSwitch(final boolean b) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preBalanceSwitch(this, b);
      }
    });
  }

  public void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postBalanceSwitch(this, oldValue, newValue);
      }
    });
  }

  public void preShutdown() throws IOException {
    // While stopping the cluster all coprocessors method should be executed first then the
    // coprocessor should be cleaned up.
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execShutdown(new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preShutdown(this);
      }
      @Override
      public void postEnvCall() {
        // invoke coprocessor stop method
        shutdown(this.getEnvironment());
      }
    });
  }

  public void preStopMaster() throws IOException {
    // While stopping master all coprocessors method should be executed first then the coprocessor
    // environment should be cleaned up.
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execShutdown(new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preStopMaster(this);
      }
      @Override
      public void postEnvCall() {
        // invoke coprocessor stop method
        shutdown(this.getEnvironment());
      }
    });
  }

  public void preMasterInitialization() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMasterInitialization(this);
      }
    });
  }

  public void postStartMaster() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postStartMaster(this);
      }
    });
  }

  public void preSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void postSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void preListSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preListSnapshot(this, snapshot);
      }
    });
  }

  public void postListSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postListSnapshot(this, snapshot);
      }
    });
  }

  public void preCloneSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preCloneSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void postCloneSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postCloneSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void preRestoreSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRestoreSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void postRestoreSnapshot(final SnapshotDescription snapshot,
      final TableDescriptor hTableDescriptor) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRestoreSnapshot(this, snapshot, hTableDescriptor);
      }
    });
  }

  public void preDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDeleteSnapshot(this, snapshot);
      }
    });
  }

  public void postDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDeleteSnapshot(this, snapshot);
      }
    });
  }

  public void preGetTableDescriptors(final List<TableName> tableNamesList,
      final List<TableDescriptor> descriptors, final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetTableDescriptors(this, tableNamesList, descriptors, regex);
      }
    });
  }

  public void postGetTableDescriptors(final List<TableName> tableNamesList,
      final List<TableDescriptor> descriptors, final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetTableDescriptors(this, tableNamesList, descriptors, regex);
      }
    });
  }

  public void preGetTableNames(final List<TableDescriptor> descriptors,
      final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetTableNames(this, descriptors, regex);
      }
    });
  }

  public void postGetTableNames(final List<TableDescriptor> descriptors,
      final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetTableNames(this, descriptors, regex);
      }
    });
  }

  public void preTableFlush(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preTableFlush(this, tableName);
      }
    });
  }

  public void postTableFlush(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postTableFlush(this, tableName);
      }
    });
  }

  public void preSetUserQuota(
      final String user, final GlobalQuotaSettings quotas) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetUserQuota(this, user, quotas);
      }
    });
  }

  public void postSetUserQuota(
      final String user, final GlobalQuotaSettings quotas) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetUserQuota(this, user, quotas);
      }
    });
  }

  public void preSetUserQuota(
      final String user, final TableName table, final GlobalQuotaSettings quotas)
          throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetUserQuota(this, user, table, quotas);
      }
    });
  }

  public void postSetUserQuota(
      final String user, final TableName table, final GlobalQuotaSettings quotas)
          throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetUserQuota(this, user, table, quotas);
      }
    });
  }

  public void preSetUserQuota(
      final String user, final String namespace, final GlobalQuotaSettings quotas)
          throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetUserQuota(this, user, namespace, quotas);
      }
    });
  }

  public void postSetUserQuota(
      final String user, final String namespace, final GlobalQuotaSettings quotas)
          throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetUserQuota(this, user, namespace, quotas);
      }
    });
  }

  public void preSetTableQuota(
      final TableName table, final GlobalQuotaSettings quotas) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetTableQuota(this, table, quotas);
      }
    });
  }

  public void postSetTableQuota(
      final TableName table, final GlobalQuotaSettings quotas) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetTableQuota(this, table, quotas);
      }
    });
  }

  public void preSetNamespaceQuota(
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preSetNamespaceQuota(this, namespace, quotas);
      }
    });
  }

  public void postSetNamespaceQuota(
      final String namespace, final GlobalQuotaSettings quotas) throws IOException{
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postSetNamespaceQuota(this, namespace, quotas);
      }
    });
  }

  public void preMoveServersAndTables(final Set<Address> servers, final Set<TableName> tables,
      final String targetGroup) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMoveServersAndTables(this, servers, tables, targetGroup);
      }
    });
  }

  public void postMoveServersAndTables(final Set<Address> servers, final Set<TableName> tables,
      final String targetGroup) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMoveServersAndTables(this, servers, tables, targetGroup);
      }
    });
  }

  public void preMoveServers(final Set<Address> servers, final String targetGroup)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMoveServers(this, servers, targetGroup);
      }
    });
  }

  public void postMoveServers(final Set<Address> servers, final String targetGroup)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMoveServers(this, servers, targetGroup);
      }
    });
  }

  public void preMoveTables(final Set<TableName> tables, final String targetGroup)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preMoveTables(this, tables, targetGroup);
      }
    });
  }

  public void postMoveTables(final Set<TableName> tables, final String targetGroup)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postMoveTables(this, tables, targetGroup);
      }
    });
  }

  public void preAddRSGroup(final String name)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preAddRSGroup(this, name);
      }
    });
  }

  public void postAddRSGroup(final String name)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postAddRSGroup(this, name);
      }
    });
  }

  public void preRemoveRSGroup(final String name)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRemoveRSGroup(this, name);
      }
    });
  }

  public void postRemoveRSGroup(final String name)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRemoveRSGroup(this, name);
      }
    });
  }

  public void preBalanceRSGroup(final String name)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preBalanceRSGroup(this, name);
      }
    });
  }

  public void postBalanceRSGroup(final String name, final boolean balanceRan)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postBalanceRSGroup(this, name, balanceRan);
      }
    });
  }

  public void preRemoveServers(final Set<Address> servers)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRemoveServers(this, servers);
      }
    });
  }

  public void postRemoveServers(final Set<Address> servers)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRemoveServers(this, servers);
      }
    });
  }

  public void preGetRSGroupInfo(final String groupName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetRSGroupInfo(this, groupName);
      }
    });
  }

  public void postGetRSGroupInfo(final String groupName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetRSGroupInfo(this, groupName);
      }
    });
  }

  public void preGetRSGroupInfoOfTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetRSGroupInfoOfTable(this, tableName);
      }
    });
  }

  public void postGetRSGroupInfoOfTable(final TableName tableName) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetRSGroupInfoOfTable(this, tableName);
      }
    });
  }

  public void preListRSGroups() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preListRSGroups(this);
      }
    });
  }

  public void postListRSGroups() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postListRSGroups(this);
      }
    });
  }

  public void preGetRSGroupInfoOfServer(final Address server) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetRSGroupInfoOfServer(this, server);
      }
    });
  }

  public void postGetRSGroupInfoOfServer(final Address server) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetRSGroupInfoOfServer(this, server);
      }
    });
  }

  public void preAddReplicationPeer(final String peerId, final ReplicationPeerConfig peerConfig)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preAddReplicationPeer(this, peerId, peerConfig);
      }
    });
  }

  public void postAddReplicationPeer(final String peerId, final ReplicationPeerConfig peerConfig)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postAddReplicationPeer(this, peerId, peerConfig);
      }
    });
  }

  public void preRemoveReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRemoveReplicationPeer(this, peerId);
      }
    });
  }

  public void postRemoveReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRemoveReplicationPeer(this, peerId);
      }
    });
  }

  public void preEnableReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preEnableReplicationPeer(this, peerId);
      }
    });
  }

  public void postEnableReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postEnableReplicationPeer(this, peerId);
      }
    });
  }

  public void preDisableReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDisableReplicationPeer(this, peerId);
      }
    });
  }

  public void postDisableReplicationPeer(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDisableReplicationPeer(this, peerId);
      }
    });
  }

  public void preGetReplicationPeerConfig(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetReplicationPeerConfig(this, peerId);
      }
    });
  }

  public void postGetReplicationPeerConfig(final String peerId) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetReplicationPeerConfig(this, peerId);
      }
    });
  }

  public void preUpdateReplicationPeerConfig(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preUpdateReplicationPeerConfig(this, peerId, peerConfig);
      }
    });
  }

  public void postUpdateReplicationPeerConfig(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postUpdateReplicationPeerConfig(this, peerId, peerConfig);
      }
    });
  }

  public void preListReplicationPeers(final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preListReplicationPeers(this, regex);
      }
    });
  }

  public void postListReplicationPeers(final String regex) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postListReplicationPeers(this, regex);
      }
    });
  }

  public void preTransitReplicationPeerSyncReplicationState(String peerId,
      SyncReplicationState state) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preTransitReplicationPeerSyncReplicationState(this, peerId, state);
      }
    });
  }

  public void postTransitReplicationPeerSyncReplicationState(String peerId,
      SyncReplicationState from, SyncReplicationState to) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postTransitReplicationPeerSyncReplicationState(this, peerId, from, to);
      }
    });
  }

  public void preRequestLock(String namespace, TableName tableName, RegionInfo[] regionInfos,
      LockType type, String description) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRequestLock(this, namespace, tableName, regionInfos, description);
      }
    });
  }

  public void postRequestLock(String namespace, TableName tableName, RegionInfo[] regionInfos,
      LockType type, String description) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRequestLock(this, namespace, tableName, regionInfos, description);
      }
    });
  }

  public void preLockHeartbeat(LockProcedure proc, boolean keepAlive) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preLockHeartbeat(this, proc.getTableName(), proc.getDescription());
      }
    });
  }

  public void postLockHeartbeat(LockProcedure proc, boolean keepAlive) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postLockHeartbeat(this);
      }
    });
  }

  public void preGetClusterMetrics() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preGetClusterMetrics(this);
      }
    });
  }

  public void postGetClusterMetrics(ClusterMetrics status) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postGetClusterMetrics(this, status);
      }
    });
  }

  public void preClearDeadServers() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preClearDeadServers(this);
      }
    });
  }

  public void postClearDeadServers(List<ServerName> servers,
      List<ServerName> notClearedServers) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postClearDeadServers(this, servers, notClearedServers);
      }
    });
  }

  public void preDecommissionRegionServers(List<ServerName> servers, boolean offload) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preDecommissionRegionServers(this, servers, offload);
      }
    });
  }

  public void postDecommissionRegionServers(List<ServerName> servers, boolean offload) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postDecommissionRegionServers(this, servers, offload);
      }
    });
  }

  public void preListDecommissionedRegionServers() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preListDecommissionedRegionServers(this);
      }
    });
  }

  public void postListDecommissionedRegionServers() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postListDecommissionedRegionServers(this);
      }
    });
  }

  public void preRecommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.preRecommissionRegionServer(this, server, encodedRegionNames);
      }
    });
  }

  public void postRecommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new MasterObserverOperation() {
      @Override
      public void call(MasterObserver observer) throws IOException {
        observer.postRecommissionRegionServer(this, server, encodedRegionNames);
      }
    });
  }
}
