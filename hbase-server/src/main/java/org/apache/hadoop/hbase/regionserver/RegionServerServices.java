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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.Service;

/**
 * Services provided by {@link HRegionServer}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionServerServices extends OnlineRegions, FavoredNodesForRegion {
  /**
   * @return True if this regionserver is stopping.
   */
  boolean isStopping();

  /** @return the WAL for a particular region. Pass null for getting the
   * default (common) WAL */
  WAL getWAL(HRegionInfo regionInfo) throws IOException;

  /** @return the List of WALs that are used by this server
   *  Doesn't include the meta WAL
   */
  List<WAL> getWALs() throws IOException;

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
   * @return RegionServer's instance of {@link RegionServerQuotaManager}
   */
  RegionServerQuotaManager getRegionServerQuotaManager();

  /**
   * @return RegionServer's instance of {@link SecureBulkLoadManager}
   */
  SecureBulkLoadManager getSecureBulkLoadManager();

  /**
   * Context for postOpenDeployTasks().
   */
  class PostOpenDeployContext {
    private final Region region;
    private final long masterSystemTime;

    @InterfaceAudience.Private
    public PostOpenDeployContext(Region region, long masterSystemTime) {
      this.region = region;
      this.masterSystemTime = masterSystemTime;
    }
    public Region getRegion() {
      return region;
    }
    public long getMasterSystemTime() {
      return masterSystemTime;
    }
  }

  /**
   * Tasks to perform after region open to complete deploy of region on
   * regionserver
   *
   * @param context the context
   * @throws KeeperException
   * @throws IOException
   */
  void postOpenDeployTasks(final PostOpenDeployContext context) throws KeeperException, IOException;

  /**
   * Tasks to perform after region open to complete deploy of region on
   * regionserver
   *
   * @param r Region to open.
   * @throws KeeperException
   * @throws IOException
   * @deprecated use {@link #postOpenDeployTasks(PostOpenDeployContext)}
   */
  @Deprecated
  void postOpenDeployTasks(final Region r) throws KeeperException, IOException;

  class RegionStateTransitionContext {
    private final TransitionCode code;
    private final long openSeqNum;
    private final long masterSystemTime;
    private final HRegionInfo[] hris;

    @InterfaceAudience.Private
    public RegionStateTransitionContext(TransitionCode code, long openSeqNum, long masterSystemTime,
        HRegionInfo... hris) {
      this.code = code;
      this.openSeqNum = openSeqNum;
      this.masterSystemTime = masterSystemTime;
      this.hris = hris;
    }
    public TransitionCode getCode() {
      return code;
    }
    public long getOpenSeqNum() {
      return openSeqNum;
    }
    public long getMasterSystemTime() {
      return masterSystemTime;
    }
    public HRegionInfo[] getHris() {
      return hris;
    }
  }

  /**
   * Notify master that a handler requests to change a region state
   */
  boolean reportRegionStateTransition(final RegionStateTransitionContext context);

  /**
   * Notify master that a handler requests to change a region state
   * @deprecated use {@link #reportRegionStateTransition(RegionStateTransitionContext)}
   */
  @Deprecated
  boolean reportRegionStateTransition(TransitionCode code, long openSeqNum, HRegionInfo... hris);

  /**
   * Notify master that a handler requests to change a region state
   * @deprecated use {@link #reportRegionStateTransition(RegionStateTransitionContext)}
   */
  @Deprecated
  boolean reportRegionStateTransition(TransitionCode code, HRegionInfo... hris);

  /**
   * Notify master that a region wants to be splitted.
   */
  long requestRegionSplit(final HRegionInfo regionInfo, final byte[] splitRow);

  /**
   * Check with master whether a procedure is completed (either succeed or fail)
   */
  boolean isProcedureFinished(final long procId) throws IOException;

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
   * @return set of recovering regions on the hosting region server
   */
  Map<String, Region> getRecoveringRegions();

  /**
   * Only required for "old" log replay; if it's removed, remove this.
   * @return The RegionServer's NonceManager
   */
  public ServerNonceManager getNonceManager();

  /**
   * @return all the online tables in this RS
   */
  Set<TableName> getOnlineTables();


  /**
   * Registers a new protocol buffer {@link Service} subclass as a coprocessor endpoint to be
   * available for handling
   * @param service the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false}
   */
  boolean registerService(Service service);

  /**
   * @return heap memory manager instance
   */
  HeapMemoryManager getHeapMemoryManager();

  /**
   * @return the max compaction pressure of all stores on this regionserver. The value should be
   *         greater than or equal to 0.0, and any value greater than 1.0 means we enter the
   *         emergency state that some stores have too many store files.
   * @see org.apache.hadoop.hbase.regionserver.Store#getCompactionPressure()
   */
  double getCompactionPressure();

  /**
   * @return the controller to avoid flush too fast
   */
  ThroughputController getFlushThroughputController();

  /**
   * @return the flush pressure of all stores on this regionserver. The value should be greater than
   *         or equal to 0.0, and any value greater than 1.0 means we enter the emergency state that
   *         global memstore size already exceeds lower limit.
   */
  @Deprecated
  double getFlushPressure();

  /**
   * @return the metrics tracker for the region server
   */
  MetricsRegionServer getMetrics();

  /**
   * Master based locks on namespaces/tables/regions.
   */
  EntityLock regionLock(List<HRegionInfo> regionInfos, String description,
      Abortable abort) throws IOException;

  /**
   * Unassign the given region from the current regionserver and assign it randomly. Could still be
   * assigned to us. This is used to solve some tough problems for which you need to reset the state
   * of a region. For example, if you hit FileNotFound exception and want to refresh the store file
   * list.
   * <p>
   * See HBASE-17712 for more details.
   */
  void unassign(byte[] regionName) throws IOException;
}
