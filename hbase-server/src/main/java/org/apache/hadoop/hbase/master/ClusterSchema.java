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
import java.util.List;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * View and edit the current cluster schema. Use this API making any modification to
 * namespaces, tables, etc.
 *
 * <h2>Implementation Notes</h2>
 * Nonces are for when operation is non-idempotent to ensure once-only semantic, even
 * across process failures.
 */
// ClusterSchema is introduced to encapsulate schema modification. Currently the different aspects
// are spread about the code base. This effort is about cleanup, shutting down access, and
// coalescing common code. In particular, we'd contain filesystem modification. Other
// benefits are to make all schema modification work the same way (one way to do an operation only
// rather than the current approach where how an operation is done varies with context) and to make
// it so clusterschema modification can stand apart from Master to faciliate standalone
// testing. It is part of the filesystem refactor project that undoes the dependency on a
// layout in HDFS that mimics our model of tables have regions have column families have files.
// With this Interface in place, with all modifications going via this route where no filesystem
// particulars are exposed, redoing our internals will take less effort.
//
// Currently ClusterSchema Interface will include namespace and table manipulation. Ideally a
// form of this Interface will go all the ways down to the file manipulation level but currently
// TBD.
//
// ClusterSchema is private to the Master; only the Master knows current cluster state and has
// means of editing/altering it.
//
// TODO: Remove Server argument when MasterServices are passed.
// TODO: We return Future<ProcedureInfo> in the below from most methods. It may change to return
// a ProcedureFuture subsequently.
@InterfaceAudience.Private
public interface ClusterSchema {
  /**
   * Timeout for cluster operations in milliseconds.
   */
  public static final String HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY =
      "hbase.master.cluster.schema.operation.timeout";
  /**
   * Default operation timeout in milliseconds.
   */
  public static final int DEFAULT_HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT =
      5 * 60 * 1000;

  /**
   * For internals use only. Do not use! Provisionally part of this Interface.
   * Prefer the high-level APIs available elsewhere in this API.
   * @return Instance of {@link TableNamespaceManager}
   */
  // TODO: Remove from here. Keep internal. This Interface is too high-level to host this accessor.
  TableNamespaceManager getTableNamespaceManager();

  /**
   * Create a new Namespace.
   * @param namespaceDescriptor descriptor for new Namespace
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   *    <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   *    as well as {@link IOException}
   */
  long createNamespace(NamespaceDescriptor namespaceDescriptor, long nonceGroup, long nonce)
  throws IOException;

  /**
   * Modify an existing Namespace.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   *    <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   *    as well as {@link IOException}
   */
  long modifyNamespace(NamespaceDescriptor descriptor, long nonceGroup, long nonce)
  throws IOException;

  /**
   * Delete an existing Namespace.
   * Only empty Namespaces (no tables) can be removed.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   *    <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   *    as well as {@link IOException}
   */
  long deleteNamespace(String name, long nonceGroup, long nonce)
  throws IOException;

  /**
   * Get a Namespace
   * @param name Name of the Namespace
   * @return Namespace descriptor for <code>name</code>
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   *    as well as {@link IOException}
   */
  // No Future here because presumption is that the request will go against cached metadata so
  // return immediately -- no need of running a Procedure.
  NamespaceDescriptor getNamespace(String name) throws IOException;

  /**
   * Get all Namespaces
   * @return All Namespace descriptors
   */
  List<NamespaceDescriptor> getNamespaces() throws IOException;
}