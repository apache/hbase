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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.ServiceNotRunningException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService;
import org.apache.hadoop.hbase.util.NonceKey;

@InterfaceAudience.Private
class ClusterSchemaServiceImpl extends AbstractService implements ClusterSchemaService {
  private final TableNamespaceManager tableNamespaceManager;
  private final MasterServices masterServices;
  private final static List<NamespaceDescriptor> EMPTY_NAMESPACE_LIST =
    Collections.unmodifiableList(new ArrayList<NamespaceDescriptor>(0));

  ClusterSchemaServiceImpl(final MasterServices masterServices) {
    this.masterServices = masterServices;
    this.tableNamespaceManager = new TableNamespaceManager(masterServices);
  }

  // All below are synchronized so consistent view on whether running or not.


  private synchronized void checkIsRunning() throws ServiceNotRunningException {
    if (!isRunning()) throw new ServiceNotRunningException();
  }

  @Override
  protected synchronized void doStart() {
    try {
      notifyStarted();
      this.tableNamespaceManager.start();
    } catch (IOException ioe) {
      notifyFailed(ioe);
    }
  }

  @Override
  protected void doStop() {
    // This is no stop for the table manager.
    notifyStopped();
    TableNamespaceManager tnsm = getTableNamespaceManager();
    if (tnsm != null) {
      tnsm.stop("Stopping");
    }
  }

  @Override
  public TableNamespaceManager getTableNamespaceManager() {
    return this.tableNamespaceManager;
  }

  private long submitProcedure(final Procedure<MasterProcedureEnv> procedure,
      final NonceKey nonceKey) throws ServiceNotRunningException {
    checkIsRunning();
    ProcedureExecutor<MasterProcedureEnv> pe = this.masterServices.getMasterProcedureExecutor();
    return pe.submitProcedure(procedure, nonceKey);
  }

  @Override
  public long createNamespace(NamespaceDescriptor namespaceDescriptor, final NonceKey nonceKey,
      final ProcedurePrepareLatch latch)
      throws IOException {
    return submitProcedure(new CreateNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), namespaceDescriptor, latch),
        nonceKey);
  }

  @Override
  public long modifyNamespace(NamespaceDescriptor namespaceDescriptor, final NonceKey nonceKey,
      final ProcedurePrepareLatch latch) throws IOException {
    return submitProcedure(new ModifyNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), namespaceDescriptor, latch),
        nonceKey);
  }

  @Override
  public long deleteNamespace(String name, final NonceKey nonceKey, final ProcedurePrepareLatch latch)
      throws IOException {
    return submitProcedure(new DeleteNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), name, latch),
      nonceKey);
  }

  @Override
  public NamespaceDescriptor getNamespace(String name) throws IOException {
    NamespaceDescriptor nsd = getTableNamespaceManager().get(name);
    if (nsd == null) throw new NamespaceNotFoundException(name);
    return nsd;
  }

  @Override
  public List<NamespaceDescriptor> getNamespaces() throws IOException {
    checkIsRunning();
    Set<NamespaceDescriptor> set = getTableNamespaceManager().list();
    if (set == null || set.isEmpty()) return EMPTY_NAMESPACE_LIST;
    List<NamespaceDescriptor> list = new ArrayList<>(set.size());
    list.addAll(set);
    return Collections.unmodifiableList(list);
  }
}
