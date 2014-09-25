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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Services provided by {@link HRegionServer}
 */
@InterfaceAudience.Private
public interface RegionServerServices
    extends OnlineRegions, FavoredNodesForRegion, PriorityFunction {
  /**
   * @return True if this regionserver is stopping.
   */
  boolean isStopping();

  /** @return the HLog for a particular region. Pass null for getting the
   * default (common) WAL */
  HLog getWAL(HRegionInfo regionInfo) throws IOException;

  /**
   * @return Implementation of {@link CompactionRequestor} or null.
   */
  CompactionRequestor getCompactionRequester();

  /**
   * @return Implementation of {@link FlushRequester} or null.
   */
  FlushRequester getFlushRequester();

  /**
   * @return the RegionServerAccounting for this Region Server
   */
  RegionServerAccounting getRegionServerAccounting();

  /**
   * @return RegionServer's instance of {@link TableLockManager}
   */
  TableLockManager getTableLockManager();

  /**
   * Tasks to perform after region open to complete deploy of region on
   * regionserver
   *
   * @param r Region to open.
   * @param ct Instance of {@link CatalogTracker}
   * @throws KeeperException
   * @throws IOException
   */
  void postOpenDeployTasks(final HRegion r, final CatalogTracker ct)
  throws KeeperException, IOException;

  /**
   * Notify master that a handler requests to change a region state
   */
  boolean reportRegionTransition(TransitionCode code, long openSeqNum, HRegionInfo... hris);

  /**
   * Notify master that a handler requests to change a region state
   */
  boolean reportRegionTransition(TransitionCode code, HRegionInfo... hris);

  /**
   * Returns a reference to the region server's RPC server
   */
  RpcServerInterface getRpcServer();

  /**
   * Get the regions that are currently being opened or closed in the RS
   * @return map of regions in transition in this RS
   */
  ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS();

  /**
   * @return Return the FileSystem object used by the regionserver
   */
  FileSystem getFileSystem();

  /**
   * @return The RegionServer's "Leases" service
   */
  Leases getLeases();

  /**
   * @return hbase executor service
   */
  ExecutorService getExecutorService();

  /**
   * @return The RegionServer's CatalogTracker
   */
  CatalogTracker getCatalogTracker();

  /**
   * @return set of recovering regions on the hosting region server
   */
  Map<String, HRegion> getRecoveringRegions();

  /**
   * Only required for "old" log replay; if it's removed, remove this.
   * @return The RegionServer's NonceManager
   */
  public ServerNonceManager getNonceManager();
}
