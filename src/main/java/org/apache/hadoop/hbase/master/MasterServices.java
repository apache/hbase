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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Services Master supplies
 */
public interface MasterServices extends Server {
  /**
   * @return Master's instance of the {@link AssignmentManager}
   */
  public AssignmentManager getAssignmentManager();

  /**
   * @return Master's filesystem {@link MasterFileSystem} utility class.
   */
  public MasterFileSystem getMasterFileSystem();

  /**
   * @return Master's {@link ServerManager} instance.
   */
  public ServerManager getServerManager();

  /**
   * @return Master's instance of {@link ExecutorService}
   */
  public ExecutorService getExecutorService();

  /**
   * Check table is modifiable; i.e. exists and is offline.
   * @param tableName Name of table to check.
   * @throws TableNotDisabledException
   * @throws TableNotFoundException 
   */
  public void checkTableModifiable(final byte [] tableName) throws IOException;

  /**
   * Create a table using the given table definition.
   * @param desc The table definition
   * @param splitKeys Starting row keys for the initial table regions.  If null
   *     a single region is created.
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
      throws IOException;

  /**
   * Delete a table
   * @param tableName The table name
   * @throws IOException
   */
  public void deleteTable(final byte[] tableName) throws IOException;

  /**
   * Modify the descriptor of an existing table
   * @param tableName The table name
   * @param descriptor The updated table descriptor
   * @throws IOException
   */
  public void modifyTable(final byte[] tableName, final HTableDescriptor descriptor)
      throws IOException;

  /**
   * Enable an existing table
   * @param tableName The table name
   * @throws IOException
   */
  public void enableTable(final byte[] tableName) throws IOException;

  /**
   * Disable an existing table
   * @param tableName The table name
   * @throws IOException
   */
  public void disableTable(final byte[] tableName) throws IOException;

  /**
   * Add a new column to an existing table
   * @param tableName The table name
   * @param column The column definition
   * @throws IOException
   */
  public void addColumn(final byte[] tableName, final HColumnDescriptor column)
      throws IOException;

  /**
   * Modify the column descriptor of an existing column in an existing table
   * @param tableName The table name
   * @param descriptor The updated column definition
   * @throws IOException
   */
  public void modifyColumn(byte[] tableName, HColumnDescriptor descriptor)
      throws IOException;

  /**
   * Delete a column from an existing table
   * @param tableName The table name
   * @param columnName The column name
   * @throws IOException
   */
  public void deleteColumn(final byte[] tableName, final byte[] columnName)
      throws IOException;

  /**
   * @return Return table descriptors implementation.
   */
  public TableDescriptors getTableDescriptors();

  /**
   * @return true if master enables ServerShutdownHandler;
   */
  public boolean isServerShutdownHandlerEnabled();
  
  /**
   * @return true if master thinks that meta hlogs should be split separately
   */
  public boolean shouldSplitMetaSeparately();

  /**
   * @return returns the master coprocessor host
   */
  public MasterCoprocessorHost getCoprocessorHost();

  /**
   * Registers a new CoprocessorProtocol subclass and instance to
   * be available for handling
   * {@link HMaster#execCoprocessor(org.apache.hadoop.hbase.client.coprocessor.Exec)} calls.
   *
   * <p>
   * Only a single protocol type/handler combination may be registered.
   *
   * After the first registration, subsequent calls with the same protocol type
   * will fail with a return value of {@code false}.
   * </p>
   * @param protocol a {@code CoprocessorProtocol} subinterface defining the
   * protocol methods
   * @param handler an instance implementing the interface
   * @param <T> the protocol type
   * @return {@code true} if the registration was successful, {@code false}
   * otherwise
   */
  public <T extends CoprocessorProtocol> boolean registerProtocol(
      Class<T> protocol, T handler);
}
