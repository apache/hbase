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

import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * A curated subset of services provided by {@link HRegionServer}.
 * For use internally only. Passed to Managers, Services and Chores so can pass less-than-a
 * full-on HRegionServer at test-time. Be judicious adding API. Changes cause ripples through
 * the code base.
 */
@InterfaceAudience.Private
public interface RegionServerServices extends Server, MutableOnlineRegions, FavoredNodesForRegion {

  /** @return the WAL for a particular region. Pass null for getting the
   * default (common) WAL */
  WAL getWAL(RegionInfo regionInfo) throws IOException;

  /** @return the List of WALs that are used by this server
   *  Doesn't include the meta WAL
   */
  List<WAL> getWALs() throws IOException;

  /**
   * @return Implementation of {@link FlushRequester} or null. Usually it will not be null unless
   *         during intialization.
   */
  FlushRequester getFlushRequester();

  /**
   * @return Implementation of {@link CompactionRequester} or null. Usually it will not be null
   *         unless during intialization.
   */
  CompactionRequester getCompactionRequestor();

  /**
   * @return the RegionServerAccounting for this Region Server
   */
  RegionServerAccounting getRegionServerAccounting();

  /**
   * @return RegionServer's instance of {@link RegionServerRpcQuotaManager}
   */
  RegionServerRpcQuotaManager getRegionServerRpcQuotaManager();

  /**
   * @return RegionServer's instance of {@link SecureBulkLoadManager}
   */
  SecureBulkLoadManager getSecureBulkLoadManager();

  /**
   * @return RegionServer's instance of {@link RegionServerSpaceQuotaManager}
   */
  RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager();

  /**
   * Context for postOpenDeployTasks().
   */
  class PostOpenDeployContext {
    private final HRegion region;
    private final long openProcId;
    private final long masterSystemTime;

    public PostOpenDeployContext(HRegion region, long openProcId, long masterSystemTime) {
      this.region = region;
      this.openProcId = openProcId;
      this.masterSystemTime = masterSystemTime;
    }

    public HRegion getRegion() {
      return region;
    }

    public long getOpenProcId() {
      return openProcId;
    }

    public long getMasterSystemTime() {
      return masterSystemTime;
    }
  }

  /**
   * Tasks to perform after region open to complete deploy of region on regionserver
   * @param context the context
   */
  void postOpenDeployTasks(final PostOpenDeployContext context) throws IOException;

  class RegionStateTransitionContext {
    private final TransitionCode code;
    private final long openSeqNum;
    private final long masterSystemTime;
    private final long[] procIds;
    private final RegionInfo[] hris;

    public RegionStateTransitionContext(TransitionCode code, long openSeqNum, long masterSystemTime,
        RegionInfo... hris) {
      this.code = code;
      this.openSeqNum = openSeqNum;
      this.masterSystemTime = masterSystemTime;
      this.hris = hris;
      this.procIds = new long[hris.length];
    }

    public RegionStateTransitionContext(TransitionCode code, long openSeqNum, long procId,
        long masterSystemTime, RegionInfo hri) {
      this.code = code;
      this.openSeqNum = openSeqNum;
      this.masterSystemTime = masterSystemTime;
      this.hris = new RegionInfo[] { hri };
      this.procIds = new long[] { procId };
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

    public RegionInfo[] getHris() {
      return hris;
    }

    public long[] getProcIds() {
      return procIds;
    }
  }

  /**
   * Notify master that a handler requests to change a region state
   */
  boolean reportRegionStateTransition(final RegionStateTransitionContext context);

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
   * @return The RegionServer's "Leases" service
   */
  LeaseManager getLeaseManager();

  /**
   * @return hbase executor service
   */
  ExecutorService getExecutorService();

  /**
   * Only required for "old" log replay; if it's removed, remove this.
   * @return The RegionServer's NonceManager
   */
  ServerNonceManager getNonceManager();

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
  EntityLock regionLock(List<RegionInfo> regionInfos, String description,
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

  /**
   * @return True if cluster is up; false if cluster is not up (we are shutting down).
   */
  boolean isClusterUp();

  /**
   * @return Return table descriptors implementation.
   */
  TableDescriptors getTableDescriptors();

  /**
   * @return The block cache instance.
   */
  Optional<BlockCache> getBlockCache();

  /**
   * @return The cache for mob files.
   */
  Optional<MobFileCache> getMobFileCache();

  /**
   * @return the {@link AccessChecker}
   */
  AccessChecker getAccessChecker();

  /**
   * @return {@link ZKPermissionWatcher}
   */
  ZKPermissionWatcher getZKPermissionWatcher();

  /**
   * Reports the provided Region sizes hosted by this RegionServer to the active Master.
   *
   * @param sizeStore The sizes for Regions locally hosted.
   * @return {@code false} if reporting should be temporarily paused, {@code true} otherwise.
   */
  boolean reportRegionSizesForQuotas(RegionSizeStore sizeStore);

  /**
   * Reports a collection of files, and their sizes, that belonged to the given {@code table} were
   * just moved to the archive directory.
   *
   * @param tableName The name of the table that files previously belonged to
   * @param archivedFiles Files and their sizes that were moved to archive
   * @return {@code true} if the files were successfully reported, {@code false} otherwise.
   */
  boolean reportFileArchivalForQuotas(
      TableName tableName, Collection<Entry<String,Long>> archivedFiles);
}
