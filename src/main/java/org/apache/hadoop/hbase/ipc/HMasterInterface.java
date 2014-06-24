/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;

/**
 * Clients interact with the HMasterInterface to gain access to meta-level
 * HBase functionality, like finding an HRegionServer and creating/destroying
 * tables.
 *
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 *
 */
public interface HMasterInterface extends HBaseRPCProtocolVersion, ThriftClientInterface {

  /** @return true if master is available */
  public boolean isMasterRunning();

  // Admin tools would use these cmds

  /**
   * Creates a new table.  If splitKeys are specified, then the table will be
   * created with an initial set of multiple regions.  If splitKeys is null,
   * the table will be created with a single region.
   * @param desc table descriptor
   * @param splitKeys
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException;

  /**
   * Deletes a table
   * @param tableName table to delete
   * @throws IOException e
   */
  public void deleteTable(final byte [] tableName) throws IOException;

  /**
   * Batch adds, modifies, and deletes columns from the specified table.
   * Any of the lists may be null, in which case those types of alterations 
   * will not occur.
   * .regioninfo files in the relevant table will be properly rewritten after
   * the modification
   *
   * @param tableName table to modify
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @return true if all .regioninfo files in the table were properly rewritten
   * @throws IOException e
   */
  public boolean alterTable(final byte [] tableName,
                         List<HColumnDescriptor> columnAdditions,
                         List<Pair<byte [], HColumnDescriptor>> columnModifications,
                         List<byte []> columnDeletions) throws IOException;

  /**
   * Batch adds, modifies, and deletes columns from the specified table.
   * Any of the lists may be null, in which case those types of alterations
   * will not occur.
   * .regioninfo files in the relevant table will be properly rewritten after
   * the modification
   *
   * @param tableName table to modify
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @return true if all .regioninfo files in the table were properly rewritten
   * @throws IOException e
   */
  public boolean alterTable(final byte [] tableName,
                         List<HColumnDescriptor> columnAdditions,
                         List<Pair<byte [], HColumnDescriptor>> columnModifications,
                         List<byte []> columnDeletions,
                         int waitInterval,
                         int numConcurentRegionsClosed) throws IOException;

  /**
   * Adds a column to the specified table
   * @param tableName table to modify
   * @param column column descriptor
   * @throws IOException e
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException;

  /**
   * Modifies an existing column on the specified table
   * @param tableName table name
   * @param columnName name of the column to edit
   * @param descriptor new column descriptor
   * @throws IOException e
   */
  public void modifyColumn(final byte [] tableName, final byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException;

  /**
   * Deletes a column from the specified table. Table must be disabled.
   * @param tableName table to alter
   * @param columnName column family to remove
   * @throws IOException e
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException;

  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param tableName table to enable
   * @throws IOException e
   */
  public void enableTable(final byte [] tableName) throws IOException;

  /**
   * Take table offline
   *
   * @param tableName table to take offline
   * @throws IOException e
   */
  public void disableTable(final byte [] tableName) throws IOException;

  /**
   * Modify a table's metadata
   *
   * @param tableName table to modify
   * @param op the operation to do
   * @param args arguments for operation
   * @throws IOException e
   */
  public void modifyTable(byte [] tableName, HConstants.Modify op, Writable [] args)
    throws IOException;

  /**
   * Shutdown an HBase cluster.
   * @throws IOException e
   */
  public void shutdown() throws IOException;

  /**
   * Return cluster status.
   * @return status object
   */
  public ClusterStatus getClusterStatus();

  /**
   * Clears the specified region from being in transition.  Used by HBaseFsck.
   * @param region region to clear from transition map
   */
  public void clearFromTransition(HRegionInfo region);

  /**
   * Used by the client to get the number of regions that have received the
   * updated schema
   * 
   * @param tableName
   * @return Pair getFirst() is the number of regions pending an update
   *              getSecond() total number of regions of the table
   * @throws IOException
   */
  public Pair<Integer, Integer> getAlterStatus(byte [] tableName)
      throws IOException;
  
  public void enableLoadBalancer();
  
  public void disableLoadBalancer();
  
  public boolean isLoadBalancerDisabled();

  /**
   * Clears the blacklisted servers map, with which new regions can be
   * assigned to the region servers which were present in this list
   */
  public void clearAllBlacklistedServers();

  /**
   * Removes a particular server from the blacklist map
   * @param hostAndPort
   */
  public void clearBlacklistedServer(final String hostAndPort);

  /**
   * Adds a server to the blacklist map. With this, the Master will not assign
   * any new regions to this region server. Only an explicit MOVE_REGION
   * request will move a region to the blacklisted server.
   * @param hostAndPort
   */
  public void addServerToBlacklist(final String hostAndPort);

  /**
   * Tells whether a server is blacklisted or not.
   * @param hostAndPort
   * @return true if the server is blacklist, else false.
   */
  public boolean isServerBlackListed(String hostAndPort);

  /**
   * Update the configuration from disk.
   */
  public void updateConfiguration();
}
