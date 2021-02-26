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

package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.NavigableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZKNamespaceManager;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This is a helper class used internally to manage the namespace metadata that is stored in
 * TableName.NAMESPACE_TABLE_NAME. It also mirrors updates to the ZK store by forwarding updates to
 * {@link org.apache.hadoop.hbase.ZKNamespaceManager}.
 *
 * WARNING: Do not use. Go via the higher-level {@link ClusterSchema} API instead. This manager
 * is likely to go aways anyways.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
  justification="TODO: synchronize access on nsTable but it is done in tiers above and this " +
    "class is going away/shrinking")
public class TableNamespaceManager implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(TableNamespaceManager.class);
  private volatile boolean stopped = false;

  private Configuration conf;
  private MasterServices masterServices;
  private Table nsTable = null; // FindBugs: IS2_INCONSISTENT_SYNC TODO: Access is not synchronized
  private ZKNamespaceManager zkNamespaceManager;
  private boolean initialized;

  public static final String KEY_MAX_REGIONS = "hbase.namespace.quota.maxregions";
  public static final String KEY_MAX_TABLES = "hbase.namespace.quota.maxtables";
  static final String NS_INIT_TIMEOUT = "hbase.master.namespace.init.timeout";
  static final int DEFAULT_NS_INIT_TIMEOUT = 300000;

  TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.conf = masterServices.getConfiguration();
  }

  public void start() throws IOException {
    if (!masterServices.getTableDescriptors().exists(TableName.NAMESPACE_TABLE_NAME)) {
      LOG.info("Namespace table not found. Creating...");
      createNamespaceTable(masterServices);
    }

    try {
      // Wait for the namespace table to be initialized.
      long startTime = EnvironmentEdgeManager.currentTime();
      int timeout = conf.getInt(NS_INIT_TIMEOUT, DEFAULT_NS_INIT_TIMEOUT);
      while (!isTableAvailableAndInitialized()) {
        if (EnvironmentEdgeManager.currentTime() - startTime + 100 > timeout) {
          // We can't do anything if ns is not online.
          throw new IOException("Timedout " + timeout + "ms waiting for namespace table to "
              + "be assigned and enabled: " + getTableState());
        }
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  private synchronized Table getNamespaceTable() throws IOException {
    if (!isTableNamespaceManagerInitialized()) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return nsTable;
  }

  /*
   * check whether a namespace has already existed.
   */
  public boolean doesNamespaceExist(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return (get(nsTable, namespaceName) != null);
  }

  public synchronized NamespaceDescriptor get(String name) throws IOException {
    if (!isTableNamespaceManagerInitialized()) {
      return null;
    }
    return zkNamespaceManager.get(name);
  }

  private NamespaceDescriptor get(Table table, String name) throws IOException {
    Result res = table.get(new Get(Bytes.toBytes(name)));
    if (res.isEmpty()) {
      return null;
    }
    byte[] val = CellUtil.cloneValue(res.getColumnLatestCell(
        HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES, HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
    return
        ProtobufUtil.toNamespaceDescriptor(
            HBaseProtos.NamespaceDescriptor.parseFrom(val));
  }

  public void insertIntoNSTable(final NamespaceDescriptor ns) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    byte[] row = Bytes.toBytes(ns.getName());
    Put p = new Put(row, true);
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(row)
          .setFamily(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES)
          .setQualifier(TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES)
          .setTimestamp(p.getTimestamp())
          .setType(Cell.Type.Put)
          .setValue(ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray())
          .build());
    nsTable.put(p);
  }

  public void updateZKNamespaceManager(final NamespaceDescriptor ns) throws IOException {
    try {
      zkNamespaceManager.update(ns);
    } catch (IOException ex) {
      String msg = "Failed to update namespace information in ZK.";
      LOG.error(msg, ex);
      throw new IOException(msg, ex);
    }
  }

  public void removeFromNSTable(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    Delete d = new Delete(Bytes.toBytes(namespaceName));
    nsTable.delete(d);
  }

  public void removeFromZKNamespaceManager(final String namespaceName) throws IOException {
    zkNamespaceManager.remove(namespaceName);
  }

  public synchronized NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    ResultScanner scanner =
        getNamespaceTable().getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
    try {
      for(Result r : scanner) {
        byte[] val = CellUtil.cloneValue(r.getColumnLatestCell(
          HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
          HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
        ret.add(ProtobufUtil.toNamespaceDescriptor(
            HBaseProtos.NamespaceDescriptor.parseFrom(val)));
      }
    } finally {
      scanner.close();
    }
    return ret;
  }

  private void createNamespaceTable(MasterServices masterServices) throws IOException {
    masterServices.createSystemTable(HTableDescriptor.NAMESPACE_TABLEDESC);
  }

  @SuppressWarnings("deprecation")
  private boolean isTableNamespaceManagerInitialized() throws IOException {
    if (initialized) {
      this.nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
      return true;
    }
    return false;
  }

  /**
   * Create Namespace in a blocking manner. Keeps trying until
   * {@link ClusterSchema#HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY} expires.
   * Note, by-passes notifying coprocessors and name checks. Use for system namespaces only.
   */
  private void blockingCreateNamespace(final NamespaceDescriptor namespaceDescriptor)
      throws IOException {
    ClusterSchema clusterSchema = this.masterServices.getClusterSchema();
    long procId = clusterSchema.createNamespace(namespaceDescriptor, null, ProcedurePrepareLatch.getNoopLatch());
    block(this.masterServices, procId);
  }


  /**
   * An ugly utility to be removed when refactor TableNamespaceManager.
   * @throws TimeoutIOException
   */
  private static void block(final MasterServices services, final long procId)
  throws TimeoutIOException {
    int timeoutInMillis = services.getConfiguration().
        getInt(ClusterSchema.HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY,
            ClusterSchema.DEFAULT_HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT);
    long deadlineTs = EnvironmentEdgeManager.currentTime() + timeoutInMillis;
    ProcedureExecutor<MasterProcedureEnv> procedureExecutor =
        services.getMasterProcedureExecutor();
    while(EnvironmentEdgeManager.currentTime() < deadlineTs) {
      if (procedureExecutor.isFinished(procId)) return;
      // Sleep some
      Threads.sleep(10);
    }
    throw new TimeoutIOException("Procedure pid=" + procId + " is still running");
  }

  /**
   * This method checks if the namespace table is assigned and then
   * tries to create its Table reference. If it was already created before, it also makes
   * sure that the connection isn't closed.
   * @return true if the namespace table manager is ready to serve, false otherwise
   */
  @SuppressWarnings("deprecation")
  public synchronized boolean isTableAvailableAndInitialized()
  throws IOException {
    // Did we already get a table? If so, still make sure it's available
    if (isTableNamespaceManagerInitialized()) {
      return true;
    }

    // Now check if the table is assigned, if not then fail fast
    if (isTableAssigned() && isTableEnabled()) {
      try {
        boolean initGoodSofar = true;
        nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
        zkNamespaceManager = new ZKNamespaceManager(masterServices.getZooKeeper());
        zkNamespaceManager.start();

        if (get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName()) == null) {
          blockingCreateNamespace(NamespaceDescriptor.DEFAULT_NAMESPACE);
        }
        if (get(nsTable, NamespaceDescriptor.SYSTEM_NAMESPACE.getName()) == null) {
          blockingCreateNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE);
        }

        if (!initGoodSofar) {
          // some required namespace is created asynchronized. We should complete init later.
          return false;
        }

        ResultScanner scanner = nsTable.getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
        try {
          for (Result result : scanner) {
            byte[] val =  CellUtil.cloneValue(result.getColumnLatestCell(
                HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
                HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
            NamespaceDescriptor ns =
                ProtobufUtil.toNamespaceDescriptor(
                    HBaseProtos.NamespaceDescriptor.parseFrom(val));
            zkNamespaceManager.update(ns);
          }
        } finally {
          scanner.close();
        }
        initialized = true;
        return true;
      } catch (IOException ie) {
        LOG.warn("Caught exception in initializing namespace table manager", ie);
        if (nsTable != null) {
          nsTable.close();
        }
        throw ie;
      }
    }
    return false;
  }

  private TableState getTableState() throws IOException {
    return masterServices.getTableStateManager().getTableState(TableName.NAMESPACE_TABLE_NAME);
  }

  private boolean isTableEnabled() throws IOException {
    return getTableState().isEnabled();
  }

  private boolean isTableAssigned() {
    // TODO: we have a better way now (wait on event)
    return masterServices.getAssignmentManager()
        .getRegionStates().hasTableRegionStates(TableName.NAMESPACE_TABLE_NAME);
  }

  public void validateTableAndRegionCount(NamespaceDescriptor desc) throws IOException {
    if (getMaxRegions(desc) <= 0) {
      throw new ConstraintException("The max region quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
    if (getMaxTables(desc) <= 0) {
      throw new ConstraintException("The max tables quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
  }

  public static long getMaxTables(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_TABLES);
    long maxTables = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxTables = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max tables.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxTables = Long.MAX_VALUE;
    }
    return maxTables;
  }

  public static long getMaxRegions(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_REGIONS);
    long maxRegions = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxRegions = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max regions.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxRegions = Long.MAX_VALUE;
    }
    return maxRegions;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    try {
      if (this.zkNamespaceManager != null) {
        this.zkNamespaceManager.stop();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed NamespaceManager close", ioe);
    }
    try {
      if (this.nsTable != null) {
        this.nsTable.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed Namespace Table close", ioe);
    }
    this.stopped = true;
  }
}
