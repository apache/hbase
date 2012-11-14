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

import com.google.protobuf.Service;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.executor.ExecutorService;

/**
 * Services Master supplies
 */
@InterfaceAudience.Private
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
   * @return Return table descriptors implementation.
   */
  public TableDescriptors getTableDescriptors();

  /**
   * @return true if master enables ServerShutdownHandler;
   */
  public boolean isServerShutdownHandlerEnabled();

  /**
   * Registers a new protocol buffer {@link Service} subclass as a master coprocessor endpoint to
   * be available for handling
   * {@link org.apache.hadoop.hbase.MasterAdminProtocol#execMasterService(com.google.protobuf.RpcController,
   * org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest)} calls.
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
  public boolean registerService(Service instance);

}
