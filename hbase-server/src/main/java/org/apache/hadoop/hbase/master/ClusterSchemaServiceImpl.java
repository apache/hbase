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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServiceNotRunningException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyNamespaceProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;

@InterfaceAudience.Private
class ClusterSchemaServiceImpl implements ClusterSchemaService {
  private boolean running = false;
  private final TableNamespaceManager tableNamespaceManager;
  private final MasterServices masterServices;
  private final int clusterSchemaOperationTimeoutInMillis;
  private final static List<NamespaceDescriptor> EMPTY_NAMESPACE_LIST =
    Collections.unmodifiableList(new ArrayList<NamespaceDescriptor>(0));

  ClusterSchemaServiceImpl(final MasterServices masterServices) {
    this.masterServices = masterServices;
    this.tableNamespaceManager = new TableNamespaceManager(masterServices);
    this.clusterSchemaOperationTimeoutInMillis = this.masterServices.getConfiguration().
      getInt(HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY,
        DEFAULT_HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT);
  }

  // All below are synchronized so consistent view on whether running or not.

  @Override
  public synchronized boolean isRunning() {
    return this.running;
  }

  private synchronized void checkIsRunning() throws ServiceNotRunningException {
    if (!isRunning()) throw new ServiceNotRunningException();
  }

  @Override
  public synchronized void startAndWait() throws IOException {
    if (isRunning()) throw new IllegalStateException("Already running; cannot double-start.");
    // Set to running FIRST because tableNamespaceManager start uses this class to do namespace ops
    this.running = true;
    this.tableNamespaceManager.start();
  }

  @Override
  public synchronized void stopAndWait() throws IOException {
    checkIsRunning();
    // You can't stop tableNamespaceManager.
    this.running = false;
  }

  @Override
  public ProcedureInfo get(final Future<ProcedureInfo> future) throws IOException {
    try {
      ProcedureInfo pi =
        future.get(this.clusterSchemaOperationTimeoutInMillis, TimeUnit.MILLISECONDS);
      // If the procedure got an exception, throw it.
      if (pi.getException() != null) throw pi.getException();
      return pi;
    } catch (ExecutionException ee) {
      // No cleanup to do... just let the exception out.
      if (ee.getCause() instanceof IOException) throw (IOException)ee.getCause();
      else throw new ClusterSchemaException(ee.getCause());
    } catch (InterruptedException e) {
      IOException ioe = new InterruptedIOException();
      ioe.initCause(e);
      throw ioe;
    } catch (TimeoutException e) {
      throw new TimeoutIOException(future.toString());
    }
  }

  @Override
  public TableNamespaceManager getTableNamespaceManager() {
    return this.tableNamespaceManager;
  }

  private Future<ProcedureInfo> submitProcedure(final Procedure<?> procedure, long nonceGroup,
      long nonce)
  throws ServiceNotRunningException {
    checkIsRunning();
    ProcedureExecutor<MasterProcedureEnv> pe = this.masterServices.getMasterProcedureExecutor();
    long procId = pe.submitProcedure(procedure, nonceGroup, nonce);
    return new ProcedureFuture(pe, procId);
  }

  @Override
  public Future<ProcedureInfo> createNamespace(NamespaceDescriptor namespaceDescriptor,
      long nonceGroup, long nonce)
  throws IOException {
    return submitProcedure(new CreateNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), namespaceDescriptor),
        nonceGroup, nonce);
  }

  @Override
  public Future<ProcedureInfo> modifyNamespace(NamespaceDescriptor namespaceDescriptor,
      long nonceGroup, long nonce)
  throws IOException {
    return submitProcedure(new ModifyNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), namespaceDescriptor),
        nonceGroup, nonce);
  }

  @Override
  public Future<ProcedureInfo> deleteNamespace(String name, long nonceGroup, long nonce)
  throws IOException {
    return submitProcedure(new DeleteNamespaceProcedure(
      this.masterServices.getMasterProcedureExecutor().getEnvironment(), name),
        nonceGroup, nonce);
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
    List<NamespaceDescriptor> list = new ArrayList<NamespaceDescriptor>(set.size());
    list.addAll(set);
    return Collections.unmodifiableList(list);
  }
}