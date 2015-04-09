/**
 *
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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;

import com.google.protobuf.Service;

/**
 * Services Master supplies
 */
@InterfaceAudience.Private
public interface MasterServices extends Server {
  /**
   * @return Master's instance of the {@link AssignmentManager}
   */
  AssignmentManager getAssignmentManager();

  /**
   * @return Master's filesystem {@link MasterFileSystem} utility class.
   */
  MasterFileSystem getMasterFileSystem();

  /**
   * @return Master's {@link ServerManager} instance.
   */
  ServerManager getServerManager();

  /**
   * @return Master's instance of {@link ExecutorService}
   */
  ExecutorService getExecutorService();

  /**
   * @return Master's instance of {@link TableLockManager}
   */
  TableLockManager getTableLockManager();

  /**
   * @return Master's instance of {@link MasterCoprocessorHost}
   */
  MasterCoprocessorHost getMasterCoprocessorHost();

  /**
   * @return Master's instance of {@link MasterQuotaManager}
   */
  MasterQuotaManager getMasterQuotaManager();

  /**
   * @return Master's instance of {@link ProcedureExecutor}
   */
  ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor();

  /**
   * Check table is modifiable; i.e. exists and is offline.
   * @param tableName Name of table to check.
   * @throws TableNotDisabledException
   * @throws TableNotFoundException
   * @throws IOException
   */
  // We actually throw the exceptions mentioned in the
  void checkTableModifiable(final TableName tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException;

  /**
   * Create a table using the given table definition.
   * @param desc The table definition
   * @param splitKeys Starting row keys for the initial table regions.  If null
   *     a single region is created.
   */
  long createTable(HTableDescriptor desc, byte[][] splitKeys)
      throws IOException;

  /**
   * Delete a table
   * @param tableName The table name
   * @throws IOException
   */
  long deleteTable(final TableName tableName) throws IOException;

  /**
   * Truncate a table
   * @param tableName The table name
   * @param preserveSplits True if the splits should be preserved
   * @throws IOException
   */
  public void truncateTable(final TableName tableName, boolean preserveSplits) throws IOException;

  /**
   * Modify the descriptor of an existing table
   * @param tableName The table name
   * @param descriptor The updated table descriptor
   * @throws IOException
   */
  void modifyTable(final TableName tableName, final HTableDescriptor descriptor)
      throws IOException;

  /**
   * Enable an existing table
   * @param tableName The table name
   * @throws IOException
   */
  long enableTable(final TableName tableName) throws IOException;

  /**
   * Disable an existing table
   * @param tableName The table name
   * @throws IOException
   */
  long disableTable(final TableName tableName) throws IOException;


  /**
   * Add a new column to an existing table
   * @param tableName The table name
   * @param column The column definition
   * @throws IOException
   */
  void addColumn(final TableName tableName, final HColumnDescriptor column)
      throws IOException;

  /**
   * Modify the column descriptor of an existing column in an existing table
   * @param tableName The table name
   * @param descriptor The updated column definition
   * @throws IOException
   */
  void modifyColumn(TableName tableName, HColumnDescriptor descriptor)
      throws IOException;

  /**
   * Delete a column from an existing table
   * @param tableName The table name
   * @param columnName The column name
   * @throws IOException
   */
  void deleteColumn(final TableName tableName, final byte[] columnName)
      throws IOException;

  /**
   * @return Return table descriptors implementation.
   */
  TableDescriptors getTableDescriptors();

  /**
   * @return true if master enables ServerShutdownHandler;
   */
  boolean isServerShutdownHandlerEnabled();

  /**
   * Registers a new protocol buffer {@link Service} subclass as a master coprocessor endpoint.
   *
   * <p>
   * Only a single instance may be registered for a given {@link Service} subclass (the
   * instances are keyed on {@link com.google.protobuf.Descriptors.ServiceDescriptor#getFullName()}.
   * After the first registration, subsequent calls with the same service name will fail with
   * a return value of {@code false}.
   * </p>
   * @param instance the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false}
   * otherwise
   */
  boolean registerService(Service instance);

  /**
   * Merge two regions. The real implementation is on the regionserver, master
   * just move the regions together and send MERGE RPC to regionserver
   * @param region_a region to merge
   * @param region_b region to merge
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   */
  void dispatchMergingRegions(
    final HRegionInfo region_a, final HRegionInfo region_b, final boolean forcible
  ) throws IOException;

  /**
   * @return true if master is initialized
   */
  boolean isInitialized();

  /**
   * Create a new namespace
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException;

  /**
   * Modify an existing namespace
   * @param descriptor descriptor which updates the existing namespace
   * @throws IOException
   */
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException;

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   * @throws IOException
   */
  public void deleteNamespace(String name) throws IOException;

  /**
   * Get a namespace descriptor by name
   * @param name name of namespace descriptor
   * @return A descriptor
   * @throws IOException
   */
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException;

  /**
   * List available namespace descriptors
   * @return A descriptor
   * @throws IOException
   */
  public List<NamespaceDescriptor> listNamespaceDescriptors() throws IOException;

  /**
   * Get list of table descriptors by namespace
   * @param name namespace name
   * @return descriptors
   * @throws IOException
   */
  public List<HTableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException;

  /**
   * Get list of table names by namespace
   * @param name namespace name
   * @return table names
   * @throws IOException
   */
  public List<TableName> listTableNamesByNamespace(String name) throws IOException;

  /**
   * @param table
   * @return the timestamp of the last successful major compaction for the passed table,
   * or 0 if no HFile resulting from a major compaction exists
   * @throws IOException
   */
  public long getLastMajorCompactionTimestamp(TableName table) throws IOException;

  /**
   * @param regionName
   * @return the timestamp of the last successful major compaction for the passed region
   * or 0 if no HFile resulting from a major compaction exists
   * @throws IOException
   */
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException;
}
