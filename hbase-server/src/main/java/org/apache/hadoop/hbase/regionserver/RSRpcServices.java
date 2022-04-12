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
package org.apache.hadoop.hbase.regionserver;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.LeaseManager.Lease;
import org.apache.hadoop.hbase.regionserver.LeaseManager.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.handler.AssignRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenPriorityRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.UnassignRegionHandler;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.RejectReplicationRequestStateChecker;
import org.apache.hadoop.hbase.replication.regionserver.RejectRequestsFromClientStateChecker;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.wal.WALSplitUtil.MutationReplay;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RemoteProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.BootstrapNodeService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.GetAllBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.GetAllBootstrapNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRegionLoadStats;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameInt64Pair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos.ScanMetrics;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse.TableQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;

/**
 * Implements the regionserver RPC services.
 */
@InterfaceAudience.Private
public class RSRpcServices extends HBaseRpcServicesBase<HRegionServer>
  implements ClientService.BlockingInterface, BootstrapNodeService.BlockingInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RSRpcServices.class);

  /** RPC scheduler to use for the region server. */
  public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
    "hbase.region.server.rpc.scheduler.factory.class";

  /**
   * Minimum allowable time limit delta (in milliseconds) that can be enforced during scans. This
   * configuration exists to prevent the scenario where a time limit is specified to be so
   * restrictive that the time limit is reached immediately (before any cells are scanned).
   */
  private static final String REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA =
      "hbase.region.server.rpc.minimum.scan.time.limit.delta";
  /**
   * Default value of {@link RSRpcServices#REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA}
   */
  private static final long DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA = 10;

  /**
   * Whether to reject rows with size > threshold defined by
   * {@link RSRpcServices#BATCH_ROWS_THRESHOLD_NAME}
   */
  private static final String REJECT_BATCH_ROWS_OVER_THRESHOLD =
    "hbase.rpc.rows.size.threshold.reject";

  /**
   * Default value of config {@link RSRpcServices#REJECT_BATCH_ROWS_OVER_THRESHOLD}
   */
  private static final boolean DEFAULT_REJECT_BATCH_ROWS_OVER_THRESHOLD = false;

  // Request counter. (Includes requests that are not serviced by regions.)
  // Count only once for requests with multiple actions like multi/caching-scan/replayBatch
  final LongAdder requestCount = new LongAdder();

  // Request counter for rpc get
  final LongAdder rpcGetRequestCount = new LongAdder();

  // Request counter for rpc scan
  final LongAdder rpcScanRequestCount = new LongAdder();
  
  // Request counter for scans that might end up in full scans
  final LongAdder rpcFullScanRequestCount = new LongAdder();

  // Request counter for rpc multi
  final LongAdder rpcMultiRequestCount = new LongAdder();

  // Request counter for rpc mutate
  final LongAdder rpcMutateRequestCount = new LongAdder();

  private final long maxScannerResultSize;

  private ScannerIdGenerator scannerIdGenerator;
  private final ConcurrentMap<String, RegionScannerHolder> scanners = new ConcurrentHashMap<>();
  // Hold the name of a closed scanner for a while. This is used to keep compatible for old clients
  // which may send next or close request to a region scanner which has already been exhausted. The
  // entries will be removed automatically after scannerLeaseTimeoutPeriod.
  private final Cache<String, String> closedScanners;
  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  /**
   * The RPC timeout period (milliseconds)
   */
  private final int rpcTimeout;

  /**
   * The minimum allowable delta to use for the scan limit
   */
  private final long minimumScanTimeLimitDelta;

  /**
   * Row size threshold for multi requests above which a warning is logged
   */
  private final int rowSizeWarnThreshold;
  /*
   * Whether we should reject requests with very high no of rows i.e. beyond threshold
   * defined by rowSizeWarnThreshold
   */
  private final boolean rejectRowsWithSizeOverThreshold;

  final AtomicBoolean clearCompactionQueues = new AtomicBoolean(false);

  /**
   * Services launched in RSRpcServices. By default they are on but you can use the below
   * booleans to selectively enable/disable either Admin or Client Service (Rare is the case
   * where you would ever turn off one or the other).
   */
  public static final String REGIONSERVER_ADMIN_SERVICE_CONFIG =
    "hbase.regionserver.admin.executorService";
  public static final String REGIONSERVER_CLIENT_SERVICE_CONFIG =
    "hbase.regionserver.client.executorService";
  public static final String REGIONSERVER_CLIENT_META_SERVICE_CONFIG =
    "hbase.regionserver.client.meta.executorService";

  /**
   * An Rpc callback for closing a RegionScanner.
   */
  private static final class RegionScannerCloseCallBack implements RpcCallback {

    private final RegionScanner scanner;

    public RegionScannerCloseCallBack(RegionScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public void run() throws IOException {
      this.scanner.close();
    }
  }

  /**
   * An Rpc callback for doing shipped() call on a RegionScanner.
   */
  private class RegionScannerShippedCallBack implements RpcCallback {
    private final String scannerName;
    private final Shipper shipper;
    private final Lease lease;

    public RegionScannerShippedCallBack(String scannerName, Shipper shipper, Lease lease) {
      this.scannerName = scannerName;
      this.shipper = shipper;
      this.lease = lease;
    }

    @Override
    public void run() throws IOException {
      this.shipper.shipped();
      // We're done. On way out re-add the above removed lease. The lease was temp removed for this
      // Rpc call and we are at end of the call now. Time to add it back.
      if (scanners.containsKey(scannerName)) {
        if (lease != null) {
          server.getLeaseManager().addLease(lease);
        }
      }
    }
  }

  /**
   * An RpcCallBack that creates a list of scanners that needs to perform callBack operation on
   * completion of multiGets.
   */
   static class RegionScannersCloseCallBack implements RpcCallback {
    private final List<RegionScanner> scanners = new ArrayList<>();

    public void addScanner(RegionScanner scanner) {
      this.scanners.add(scanner);
    }

    @Override
    public void run() {
      for (RegionScanner scanner : scanners) {
        try {
          scanner.close();
        } catch (IOException e) {
          LOG.error("Exception while closing the scanner " + scanner, e);
        }
      }
    }
  }

  /**
   * Holder class which holds the RegionScanner, nextCallSeq and RpcCallbacks together.
   */
  static final class RegionScannerHolder {
    private final AtomicLong nextCallSeq = new AtomicLong(0);
    private final RegionScanner s;
    private final HRegion r;
    private final RpcCallback closeCallBack;
    private final RpcCallback shippedCallback;
    private byte[] rowOfLastPartialResult;
    private boolean needCursor;
    private boolean fullRegionScan;
    private final String clientIPAndPort;
    private final String userName;

    RegionScannerHolder(RegionScanner s, HRegion r,
        RpcCallback closeCallBack, RpcCallback shippedCallback, boolean needCursor,
        boolean fullRegionScan, String clientIPAndPort, String userName) {
      this.s = s;
      this.r = r;
      this.closeCallBack = closeCallBack;
      this.shippedCallback = shippedCallback;
      this.needCursor = needCursor;
      this.fullRegionScan = fullRegionScan;
      this.clientIPAndPort = clientIPAndPort;
      this.userName = userName;
    }

    long getNextCallSeq() {
      return nextCallSeq.get();
    }

    boolean incNextCallSeq(long currentSeq) {
      // Use CAS to prevent multiple scan request running on the same scanner.
      return nextCallSeq.compareAndSet(currentSeq, currentSeq + 1);
    }

    // Should be called only when we need to print lease expired messages otherwise
    // cache the String once made.
    @Override
    public String toString() {
      return "clientIPAndPort=" + this.clientIPAndPort +
        ", userName=" + this.userName +
        ", regionInfo=" + this.r.getRegionInfo().getRegionNameAsString();
    }
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    @Override
    public void leaseExpired() {
      RegionScannerHolder rsh = scanners.remove(this.scannerName);
      if (rsh == null) {
        LOG.warn("Scanner lease {} expired but no outstanding scanner", this.scannerName);
        return;
      }
      LOG.info("Scanner lease {} expired {}", this.scannerName, rsh);
      server.getMetrics().incrScannerLeaseExpired();
      RegionScanner s = rsh.s;
      HRegion region = null;
      try {
        region = server.getRegion(s.getRegionInfo().getRegionName());
        if (region != null && region.getCoprocessorHost() != null) {
          region.getCoprocessorHost().preScannerClose(s);
        }
      } catch (IOException e) {
        LOG.error("Closing scanner {} {}", this.scannerName, rsh, e);
      } finally {
        try {
          s.close();
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postScannerClose(s);
          }
        } catch (IOException e) {
          LOG.error("Closing scanner {} {}", this.scannerName, rsh, e);
        }
      }
    }
  }

  private static ResultOrException getResultOrException(final ClientProtos.Result r,
                                                        final int index){
    return getResultOrException(ResponseConverter.buildActionResult(r), index);
  }

  private static ResultOrException getResultOrException(final Exception e, final int index) {
    return getResultOrException(ResponseConverter.buildActionResult(e), index);
  }

  private static ResultOrException getResultOrException(
      final ResultOrException.Builder builder, final int index) {
    return builder.setIndex(index).build();
  }

  /**
   * Checks for the following pre-checks in order:
   * <ol>
   *   <li>RegionServer is running</li>
   *   <li>If authorization is enabled, then RPC caller has ADMIN permissions</li>
   * </ol>
   * @param requestName name of rpc request. Used in reporting failures to provide context.
   * @throws ServiceException If any of the above listed pre-check fails.
   */
  private void rpcPreCheck(String requestName) throws ServiceException {
    try {
      checkOpen();
      requirePermission(requestName, Permission.Action.ADMIN);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private boolean isClientCellBlockSupport(RpcCallContext context) {
    return context != null && context.isClientCellBlockSupported();
  }

  private void addResult(final MutateResponse.Builder builder, final Result result,
      final HBaseRpcController rpcc, boolean clientCellBlockSupported) {
    if (result == null) return;
    if (clientCellBlockSupported) {
      builder.setResult(ProtobufUtil.toResultNoData(result));
      rpcc.setCellScanner(result.cellScanner());
    } else {
      ClientProtos.Result pbr = ProtobufUtil.toResult(result);
      builder.setResult(pbr);
    }
  }

  private void addResults(ScanResponse.Builder builder, List<Result> results,
      HBaseRpcController controller, boolean isDefaultRegion, boolean clientCellBlockSupported) {
    builder.setStale(!isDefaultRegion);
    if (results.isEmpty()) {
      return;
    }
    if (clientCellBlockSupported) {
      for (Result res : results) {
        builder.addCellsPerResult(res.size());
        builder.addPartialFlagPerResult(res.mayHaveMoreCellsInRow());
      }
      controller.setCellScanner(CellUtil.createCellScanner(results));
    } else {
      for (Result res : results) {
        ClientProtos.Result pbr = ProtobufUtil.toResult(res);
        builder.addResults(pbr);
      }
    }
  }

  private CheckAndMutateResult checkAndMutate(HRegion region, List<ClientProtos.Action> actions,
    CellScanner cellScanner, Condition condition, long nonceGroup,
    ActivePolicyEnforcement spaceQuotaEnforcement) throws IOException {
    int countOfCompleteMutation = 0;
    try {
      if (!region.getRegionInfo().isMetaRegion()) {
        server.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      List<Mutation> mutations = new ArrayList<>();
      long nonce = HConstants.NO_NONCE;
      for (ClientProtos.Action action: actions) {
        if (action.hasGet()) {
          throw new DoNotRetryIOException("Atomic put and/or delete only, not a Get=" +
            action.getGet());
        }
        MutationProto mutation = action.getMutation();
        MutationType type = mutation.getMutateType();
        switch (type) {
          case PUT:
            Put put = ProtobufUtil.toPut(mutation, cellScanner);
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, put);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(put);
            mutations.add(put);
            break;
          case DELETE:
            Delete del = ProtobufUtil.toDelete(mutation, cellScanner);
            ++countOfCompleteMutation;
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(del);
            mutations.add(del);
            break;
          case INCREMENT:
            Increment increment = ProtobufUtil.toIncrement(mutation, cellScanner);
            nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, increment);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(increment);
            mutations.add(increment);
            break;
          case APPEND:
            Append append = ProtobufUtil.toAppend(mutation, cellScanner);
            nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, append);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(append);
            mutations.add(append);
            break;
          default:
            throw new DoNotRetryIOException("invalid mutation type : " + type);
        }
      }

      if (mutations.size() == 0) {
        return new CheckAndMutateResult(true, null);
      } else {
        CheckAndMutate checkAndMutate = ProtobufUtil.toCheckAndMutate(condition, mutations);
        CheckAndMutateResult result = null;
        if (region.getCoprocessorHost() != null) {
          result = region.getCoprocessorHost().preCheckAndMutate(checkAndMutate);
        }
        if (result == null) {
          result = region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
          if (region.getCoprocessorHost() != null) {
            result = region.getCoprocessorHost().postCheckAndMutate(checkAndMutate, result);
          }
        }
        return result;
      }
    } finally {
      // Currently, the checkAndMutate isn't supported by batch so it won't mess up the cell scanner
      // even if the malformed cells are not skipped.
      for (int i = countOfCompleteMutation; i < actions.size(); ++i) {
        skipCellsForMutation(actions.get(i), cellScanner);
      }
    }
  }

  /**
   * Execute an append mutation.
   *
   * @return result to return to client if default operation should be
   * bypassed as indicated by RegionObserver, null otherwise
   */
  private Result append(final HRegion region, final OperationQuota quota,
      final MutationProto mutation, final CellScanner cellScanner, long nonceGroup,
      ActivePolicyEnforcement spaceQuota)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    Append append = ProtobufUtil.toAppend(mutation, cellScanner);
    checkCellSizeLimit(region, append);
    spaceQuota.getPolicyEnforcement(region).check(append);
    quota.addMutation(append);
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    Result r = region.append(append, nonceGroup, nonce);
    if (server.getMetrics() != null) {
      server.getMetrics().updateAppend(region.getTableDescriptor().getTableName(),
        EnvironmentEdgeManager.currentTime() - before);
    }
    return r == null ? Result.EMPTY_RESULT : r;
  }

  /**
   * Execute an increment mutation.
   */
  private Result increment(final HRegion region, final OperationQuota quota,
      final MutationProto mutation, final CellScanner cells, long nonceGroup,
      ActivePolicyEnforcement spaceQuota)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    Increment increment = ProtobufUtil.toIncrement(mutation, cells);
    checkCellSizeLimit(region, increment);
    spaceQuota.getPolicyEnforcement(region).check(increment);
    quota.addMutation(increment);
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    Result r = region.increment(increment, nonceGroup, nonce);
    final MetricsRegionServer metricsRegionServer = server.getMetrics();
    if (metricsRegionServer != null) {
      metricsRegionServer.updateIncrement(region.getTableDescriptor().getTableName(),
        EnvironmentEdgeManager.currentTime() - before);
    }
    return r == null ? Result.EMPTY_RESULT : r;
  }

  /**
   * Run through the regionMutation <code>rm</code> and per Mutation, do the work, and then when
   * done, add an instance of a {@link ResultOrException} that corresponds to each Mutation.
   * @param cellsToReturn  Could be null. May be allocated in this method.  This is what this
   * method returns as a 'result'.
   * @param closeCallBack the callback to be used with multigets
   * @param context the current RpcCallContext
   * @return Return the <code>cellScanner</code> passed
   */
  private List<CellScannable> doNonAtomicRegionMutation(final HRegion region,
      final OperationQuota quota, final RegionAction actions, final CellScanner cellScanner,
      final RegionActionResult.Builder builder, List<CellScannable> cellsToReturn, long nonceGroup,
      final RegionScannersCloseCallBack closeCallBack, RpcCallContext context,
      ActivePolicyEnforcement spaceQuotaEnforcement) {
    // Gather up CONTIGUOUS Puts and Deletes in this mutations List.  Idea is that rather than do
    // one at a time, we instead pass them in batch.  Be aware that the corresponding
    // ResultOrException instance that matches each Put or Delete is then added down in the
    // doNonAtomicBatchOp call.  We should be staying aligned though the Put and Delete are
    // deferred/batched
    List<ClientProtos.Action> mutations = null;
    long maxQuotaResultSize = Math.min(maxScannerResultSize, quota.getReadAvailable());
    IOException sizeIOE = null;
    Object lastBlock = null;
    ClientProtos.ResultOrException.Builder resultOrExceptionBuilder = ResultOrException.newBuilder();
    boolean hasResultOrException = false;
    for (ClientProtos.Action action : actions.getActionList()) {
      hasResultOrException = false;
      resultOrExceptionBuilder.clear();
      try {
        Result r = null;

        if (context != null
            && context.isRetryImmediatelySupported()
            && (context.getResponseCellSize() > maxQuotaResultSize
              || context.getResponseBlockSize() + context.getResponseExceptionSize()
              > maxQuotaResultSize)) {

          // We're storing the exception since the exception and reason string won't
          // change after the response size limit is reached.
          if (sizeIOE == null ) {
            // We don't need the stack un-winding do don't throw the exception.
            // Throwing will kill the JVM's JIT.
            //
            // Instead just create the exception and then store it.
            sizeIOE = new MultiActionResultTooLarge("Max size exceeded"
                + " CellSize: " + context.getResponseCellSize()
                + " BlockSize: " + context.getResponseBlockSize());

            // Only report the exception once since there's only one request that
            // caused the exception. Otherwise this number will dominate the exceptions count.
            rpcServer.getMetrics().exception(sizeIOE);
          }

          // Now that there's an exception is known to be created
          // use it for the response.
          //
          // This will create a copy in the builder.
          NameBytesPair pair = ResponseConverter.buildException(sizeIOE);
          resultOrExceptionBuilder.setException(pair);
          context.incrementResponseExceptionSize(pair.getSerializedSize());
          resultOrExceptionBuilder.setIndex(action.getIndex());
          builder.addResultOrException(resultOrExceptionBuilder.build());
          skipCellsForMutation(action, cellScanner);
          continue;
        }
        if (action.hasGet()) {
          long before = EnvironmentEdgeManager.currentTime();
          ClientProtos.Get pbGet = action.getGet();
          // An asynchbase client, https://github.com/OpenTSDB/asynchbase, starts by trying to do
          // a get closest before. Throwing the UnknownProtocolException signals it that it needs
          // to switch and do hbase2 protocol (HBase servers do not tell clients what versions
          // they are; its a problem for non-native clients like asynchbase. HBASE-20225.
          if (pbGet.hasClosestRowBefore() && pbGet.getClosestRowBefore()) {
            throw new UnknownProtocolException("Is this a pre-hbase-1.0.0 or asynchbase client? " +
                "Client is invoking getClosestRowBefore removed in hbase-2.0.0 replaced by " +
                "reverse Scan.");
          }
          try {
            Get get = ProtobufUtil.toGet(pbGet);
            if (context != null) {
              r = get(get, (region), closeCallBack, context);
            } else {
              r = region.get(get);
            }
          } finally {
            final MetricsRegionServer metricsRegionServer = server.getMetrics();
            if (metricsRegionServer != null) {
              metricsRegionServer.updateGet(
                  region.getTableDescriptor().getTableName(),
                  EnvironmentEdgeManager.currentTime() - before);
            }
          }
        } else if (action.hasServiceCall()) {
          hasResultOrException = true;
          Message result =
            execServiceOnRegion(region, action.getServiceCall());
          ClientProtos.CoprocessorServiceResult.Builder serviceResultBuilder =
            ClientProtos.CoprocessorServiceResult.newBuilder();
          resultOrExceptionBuilder.setServiceResult(
            serviceResultBuilder.setValue(
              serviceResultBuilder.getValueBuilder()
                .setName(result.getClass().getName())
                // TODO: Copy!!!
                .setValue(UnsafeByteOperations.unsafeWrap(result.toByteArray()))));
        } else if (action.hasMutation()) {
          MutationType type = action.getMutation().getMutateType();
          if (type != MutationType.PUT && type != MutationType.DELETE && mutations != null &&
              !mutations.isEmpty()) {
            // Flush out any Puts or Deletes already collected.
            doNonAtomicBatchOp(builder, region, quota, mutations, cellScanner,
              spaceQuotaEnforcement);
            mutations.clear();
          }
          switch (type) {
            case APPEND:
              r = append(region, quota, action.getMutation(), cellScanner, nonceGroup,
                  spaceQuotaEnforcement);
              break;
            case INCREMENT:
              r = increment(region, quota, action.getMutation(), cellScanner, nonceGroup,
                  spaceQuotaEnforcement);
              break;
            case PUT:
            case DELETE:
              // Collect the individual mutations and apply in a batch
              if (mutations == null) {
                mutations = new ArrayList<>(actions.getActionCount());
              }
              mutations.add(action);
              break;
            default:
              throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
          }
        } else {
          throw new HBaseIOException("Unexpected Action type");
        }
        if (r != null) {
          ClientProtos.Result pbResult = null;
          if (isClientCellBlockSupport(context)) {
            pbResult = ProtobufUtil.toResultNoData(r);
            //  Hard to guess the size here.  Just make a rough guess.
            if (cellsToReturn == null) {
              cellsToReturn = new ArrayList<>();
            }
            cellsToReturn.add(r);
          } else {
            pbResult = ProtobufUtil.toResult(r);
          }
          lastBlock = addSize(context, r, lastBlock);
          hasResultOrException = true;
          resultOrExceptionBuilder.setResult(pbResult);
        }
        // Could get to here and there was no result and no exception.  Presumes we added
        // a Put or Delete to the collecting Mutations List for adding later.  In this
        // case the corresponding ResultOrException instance for the Put or Delete will be added
        // down in the doNonAtomicBatchOp method call rather than up here.
      } catch (IOException ie) {
        rpcServer.getMetrics().exception(ie);
        hasResultOrException = true;
        NameBytesPair pair = ResponseConverter.buildException(ie);
        resultOrExceptionBuilder.setException(pair);
        context.incrementResponseExceptionSize(pair.getSerializedSize());
      }
      if (hasResultOrException) {
        // Propagate index.
        resultOrExceptionBuilder.setIndex(action.getIndex());
        builder.addResultOrException(resultOrExceptionBuilder.build());
      }
    }
    // Finish up any outstanding mutations
    if (!CollectionUtils.isEmpty(mutations)) {
      doNonAtomicBatchOp(builder, region, quota, mutations, cellScanner, spaceQuotaEnforcement);
    }
    return cellsToReturn;
  }

  private void checkCellSizeLimit(final HRegion r, final Mutation m) throws IOException {
    if (r.maxCellSize > 0) {
      CellScanner cells = m.cellScanner();
      while (cells.advance()) {
        int size = PrivateCellUtil.estimatedSerializedSizeOf(cells.current());
        if (size > r.maxCellSize) {
          String msg = "Cell[" + cells.current() + "] with size " + size
            + " exceeds limit of " + r.maxCellSize + " bytes";
          LOG.debug(msg);
          throw new DoNotRetryIOException(msg);
        }
      }
    }
  }

  private void doAtomicBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations,
    final CellScanner cells, long nonceGroup, ActivePolicyEnforcement spaceQuotaEnforcement)
    throws IOException {
    // Just throw the exception. The exception will be caught and then added to region-level
    // exception for RegionAction. Leaving the null to action result is ok since the null
    // result is viewed as failure by hbase client. And the region-lever exception will be used
    // to replaced the null result. see AsyncRequestFutureImpl#receiveMultiAction and
    // AsyncBatchRpcRetryingCaller#onComplete for more details.
    doBatchOp(builder, region, quota, mutations, cells, nonceGroup, spaceQuotaEnforcement, true);
  }

  private void doNonAtomicBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations,
    final CellScanner cells, ActivePolicyEnforcement spaceQuotaEnforcement) {
    try {
      doBatchOp(builder, region, quota, mutations, cells, HConstants.NO_NONCE,
        spaceQuotaEnforcement, false);
    } catch (IOException e) {
      // Set the exception for each action. The mutations in same RegionAction are group to
      // different batch and then be processed individually. Hence, we don't set the region-level
      // exception here for whole RegionAction.
      for (Action mutation : mutations) {
        builder.addResultOrException(getResultOrException(e, mutation.getIndex()));
      }
    }
  }

  /**
   * Execute a list of mutations.
   *
   * @param builder
   * @param region
   * @param mutations
   */
  private void doBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations,
    final CellScanner cells, long nonceGroup, ActivePolicyEnforcement spaceQuotaEnforcement,
    boolean atomic) throws IOException {
    Mutation[] mArray = new Mutation[mutations.size()];
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      /** HBASE-17924
       * mutationActionMap is a map to map the relation between mutations and actions
       * since mutation array may have been reoredered.In order to return the right
       * result or exception to the corresponding actions, We need to know which action
       * is the mutation belong to. We can't sort ClientProtos.Action array, since they
       * are bonded to cellscanners.
       */
      Map<Mutation, ClientProtos.Action> mutationActionMap = new HashMap<>();
      int i = 0;
      long nonce = HConstants.NO_NONCE;
      for (ClientProtos.Action action: mutations) {
        if (action.hasGet()) {
          throw new DoNotRetryIOException("Atomic put and/or delete only, not a Get=" +
            action.getGet());
        }
        MutationProto m = action.getMutation();
        Mutation mutation;
        switch (m.getMutateType()) {
          case PUT:
            mutation = ProtobufUtil.toPut(m, cells);
            batchContainsPuts = true;
            break;

          case DELETE:
            mutation = ProtobufUtil.toDelete(m, cells);
            batchContainsDelete = true;
            break;

          case INCREMENT:
            mutation = ProtobufUtil.toIncrement(m, cells);
            nonce = m.hasNonce() ? m.getNonce() : HConstants.NO_NONCE;
            break;

          case APPEND:
            mutation = ProtobufUtil.toAppend(m, cells);
            nonce = m.hasNonce() ? m.getNonce() : HConstants.NO_NONCE;
            break;

          default:
            throw new DoNotRetryIOException("Invalid mutation type : " + m.getMutateType());
        }
        mutationActionMap.put(mutation, action);
        mArray[i++] = mutation;
        checkCellSizeLimit(region, mutation);
        // Check if a space quota disallows this mutation
        spaceQuotaEnforcement.getPolicyEnforcement(region).check(mutation);
        quota.addMutation(mutation);
      }

      if (!region.getRegionInfo().isMetaRegion()) {
        server.getMemStoreFlusher().reclaimMemStoreMemory();
      }

      // HBASE-17924
      // Sort to improve lock efficiency for non-atomic batch of operations. If atomic
      // order is preserved as its expected from the client
      if (!atomic) {
        Arrays.sort(mArray, (v1, v2) -> Row.COMPARATOR.compare(v1, v2));
      }

      OperationStatus[] codes = region.batchMutate(mArray, atomic, nonceGroup, nonce);

      // When atomic is true, it indicates that the mutateRow API or the batch API with
      // RowMutations is called. In this case, we need to merge the results of the
      // Increment/Append operations if the mutations include those operations, and set the merged
      // result to the first element of the ResultOrException list
      if (atomic) {
        List<ResultOrException> resultOrExceptions = new ArrayList<>();
        List<Result> results = new ArrayList<>();
        for (i = 0; i < codes.length; i++) {
          if (codes[i].getResult() != null) {
            results.add(codes[i].getResult());
          }
          if (i != 0) {
            resultOrExceptions.add(getResultOrException(
              ClientProtos.Result.getDefaultInstance(), i));
          }
        }

        if (results.isEmpty()) {
          builder.addResultOrException(getResultOrException(
            ClientProtos.Result.getDefaultInstance(), 0));
        } else {
          // Merge the results of the Increment/Append operations
          List<Cell> cellList = new ArrayList<>();
          for (Result result : results) {
            if (result.rawCells() != null) {
              cellList.addAll(Arrays.asList(result.rawCells()));
            }
          }
          Result result = Result.create(cellList);

          // Set the merged result of the Increment/Append operations to the first element of the
          // ResultOrException list
          builder.addResultOrException(getResultOrException(ProtobufUtil.toResult(result), 0));
        }

        builder.addAllResultOrException(resultOrExceptions);
        return;
      }

      for (i = 0; i < codes.length; i++) {
        Mutation currentMutation = mArray[i];
        ClientProtos.Action currentAction = mutationActionMap.get(currentMutation);
        int index = currentAction.hasIndex() ? currentAction.getIndex() : i;
        Exception e;
        switch (codes[i].getOperationStatusCode()) {
          case BAD_FAMILY:
            e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          case SANITY_CHECK_FAILURE:
            e = new FailedSanityCheckException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          default:
            e = new DoNotRetryIOException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          case SUCCESS:
            builder.addResultOrException(getResultOrException(
              ClientProtos.Result.getDefaultInstance(), index));
            break;

          case STORE_TOO_BUSY:
            e = new RegionTooBusyException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;
        }
      }
    } finally {
      int processedMutationIndex = 0;
      for (Action mutation : mutations) {
        // The non-null mArray[i] means the cell scanner has been read.
        if (mArray[processedMutationIndex++] == null) {
          skipCellsForMutation(mutation, cells);
        }
      }
      updateMutationMetrics(region, before, batchContainsPuts, batchContainsDelete);
    }
  }

  private void updateMutationMetrics(HRegion region, long starttime, boolean batchContainsPuts,
    boolean batchContainsDelete) {
    final MetricsRegionServer metricsRegionServer = server.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      if (batchContainsPuts) {
        metricsRegionServer.updatePutBatch(
            region.getTableDescriptor().getTableName(), after - starttime);
      }
      if (batchContainsDelete) {
        metricsRegionServer.updateDeleteBatch(
            region.getTableDescriptor().getTableName(), after - starttime);
      }
    }
  }

  /**
   * Execute a list of Put/Delete mutations. The function returns OperationStatus instead of
   * constructing MultiResponse to save a possible loop if caller doesn't need MultiResponse.
   * @return an array of OperationStatus which internally contains the OperationStatusCode and the
   *         exceptionMessage if any
   * @deprecated Since 3.0.0, will be removed in 4.0.0. We do not use this method for replaying
   *             edits for secondary replicas any more, see
   *             {@link #replicateToReplica(RpcController, ReplicateWALEntryRequest)}.
   */
  @Deprecated
  private OperationStatus[] doReplayBatchOp(final HRegion region,
    final List<MutationReplay> mutations, long replaySeqId) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      for (Iterator<MutationReplay> it = mutations.iterator(); it.hasNext();) {
        MutationReplay m = it.next();

        if (m.getType() == MutationType.PUT) {
          batchContainsPuts = true;
        } else {
          batchContainsDelete = true;
        }

        NavigableMap<byte[], List<Cell>> map = m.mutation.getFamilyCellMap();
        List<Cell> metaCells = map.get(WALEdit.METAFAMILY);
        if (metaCells != null && !metaCells.isEmpty()) {
          for (Cell metaCell : metaCells) {
            CompactionDescriptor compactionDesc = WALEdit.getCompaction(metaCell);
            boolean isDefaultReplica = RegionReplicaUtil.isDefaultReplica(region.getRegionInfo());
            HRegion hRegion = region;
            if (compactionDesc != null) {
              // replay the compaction. Remove the files from stores only if we are the primary
              // region replica (thus own the files)
              hRegion.replayWALCompactionMarker(compactionDesc, !isDefaultReplica, isDefaultReplica,
                replaySeqId);
              continue;
            }
            FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(metaCell);
            if (flushDesc != null && !isDefaultReplica) {
              hRegion.replayWALFlushMarker(flushDesc, replaySeqId);
              continue;
            }
            RegionEventDescriptor regionEvent = WALEdit.getRegionEventDescriptor(metaCell);
            if (regionEvent != null && !isDefaultReplica) {
              hRegion.replayWALRegionEventMarker(regionEvent);
              continue;
            }
            BulkLoadDescriptor bulkLoadEvent = WALEdit.getBulkLoadDescriptor(metaCell);
            if (bulkLoadEvent != null) {
              hRegion.replayWALBulkLoadEventMarker(bulkLoadEvent);
              continue;
            }
          }
          it.remove();
        }
      }
      requestCount.increment();
      if (!region.getRegionInfo().isMetaRegion()) {
        server.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      return region.batchReplay(mutations.toArray(
        new MutationReplay[mutations.size()]), replaySeqId);
    } finally {
      updateMutationMetrics(region, before, batchContainsPuts, batchContainsDelete);
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, RegionScannerHolder> e : scanners.entrySet()) {
      try {
        e.getValue().s.close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  // Directly invoked only for testing
  public RSRpcServices(final HRegionServer rs) throws IOException {
    super(rs, rs.getProcessName());
    final Configuration conf = rs.getConfiguration();
    rowSizeWarnThreshold = conf.getInt(
      HConstants.BATCH_ROWS_THRESHOLD_NAME, HConstants.BATCH_ROWS_THRESHOLD_DEFAULT);
    rejectRowsWithSizeOverThreshold =
      conf.getBoolean(REJECT_BATCH_ROWS_OVER_THRESHOLD, DEFAULT_REJECT_BATCH_ROWS_OVER_THRESHOLD);
    scannerLeaseTimeoutPeriod = conf.getInt(
      HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    maxScannerResultSize = conf.getLong(
      HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_SERVER_SCANNER_MAX_RESULT_SIZE);
    rpcTimeout = conf.getInt(
      HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    minimumScanTimeLimitDelta = conf.getLong(
      REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA,
      DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA);
    rpcServer.setNamedQueueRecorder(rs.getNamedQueueRecorder());
    closedScanners = CacheBuilder.newBuilder()
        .expireAfterAccess(scannerLeaseTimeoutPeriod, TimeUnit.MILLISECONDS).build();
  }

  @Override
  protected boolean defaultReservoirEnabled() {
    return true;
  }

  @Override
  protected ServerType getDNSServerType() {
    return DNS.ServerType.REGIONSERVER;
  }

  @Override
  protected String getHostname(Configuration conf, String defaultHostname) {
    return conf.get("hbase.regionserver.ipc.address", defaultHostname);
  }

  @Override
  protected String getPortConfigName() {
    return HConstants.REGIONSERVER_PORT;
  }

  @Override
  protected int getDefaultPort() {
    return HConstants.DEFAULT_REGIONSERVER_PORT;
  }

  @Override
  protected Class<?> getRpcSchedulerFactoryClass(Configuration conf) {
    return conf.getClass(REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  protected RpcServerInterface
    createRpcServer(
      final Server server,
      final RpcSchedulerFactory rpcSchedulerFactory,
      final InetSocketAddress bindAddress,
      final String name
  ) throws IOException {
    final Configuration conf = server.getConfiguration();
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      return RpcServerFactory.createRpcServer(server, name, getServices(),
          bindAddress, // use final bindAddress for this server.
          conf, rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '"
          + HConstants.REGIONSERVER_PORT + "' configuration property.",
          be.getCause() != null ? be.getCause() : be);
    }
  }

  protected Class<?> getRpcSchedulerFactoryClass() {
    final Configuration conf = server.getConfiguration();
    return conf.getClass(REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  protected PriorityFunction createPriority() {
    return new RSAnnotationReadingPriorityFunction(this);
  }

  public int getScannersCount() {
    return scanners.size();
  }

  /**
   * @return The outstanding RegionScanner for <code>scannerId</code> or null if none found.
   */
  RegionScanner getScanner(long scannerId) {
    RegionScannerHolder rsh = getRegionScannerHolder(scannerId);
    return rsh == null? null: rsh.s;
  }

  /**
   * @return The associated RegionScannerHolder for <code>scannerId</code> or null.
   */
  private RegionScannerHolder getRegionScannerHolder(long scannerId) {
    return scanners.get(toScannerName(scannerId));
  }

  public String getScanDetailsWithId(long scannerId) {
    RegionScanner scanner = getScanner(scannerId);
    if (scanner == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    builder.append("table: ").append(scanner.getRegionInfo().getTable().getNameAsString());
    builder.append(" region: ").append(scanner.getRegionInfo().getRegionNameAsString());
    builder.append(" operation_id: ").append(scanner.getOperationId());
    return builder.toString();
  }

  public String getScanDetailsWithRequest(ScanRequest request) {
    try {
      if (!request.hasRegion()) {
        return null;
      }
      Region region = getRegion(request.getRegion());
      StringBuilder builder = new StringBuilder();
      builder.append("table: ").append(region.getRegionInfo().getTable().getNameAsString());
      builder.append(" region: ").append(region.getRegionInfo().getRegionNameAsString());
      for (NameBytesPair pair : request.getScan().getAttributeList()) {
        if (OperationWithAttributes.ID_ATRIBUTE.equals(pair.getName())) {
          builder.append(" operation_id: ").append(Bytes.toString(pair.getValue().toByteArray()));
          break;
        }
      }
      return builder.toString();
    } catch (IOException ignored) {
      return null;
    }
  }

  /**
   * Get the vtime associated with the scanner.
   * Currently the vtime is the number of "next" calls.
   */
  long getScannerVirtualTime(long scannerId) {
    RegionScannerHolder rsh = getRegionScannerHolder(scannerId);
    return rsh == null? 0L: rsh.getNextCallSeq();
  }

  /**
   * Method to account for the size of retained cells and retained data blocks.
   * @param context rpc call context
   * @param r result to add size.
   * @param lastBlock last block to check whether we need to add the block size in context.
   * @return an object that represents the last referenced block from this response.
   */
  Object addSize(RpcCallContext context, Result r, Object lastBlock) {
    if (context != null && r != null && !r.isEmpty()) {
      for (Cell c : r.rawCells()) {
        context.incrementResponseCellSize(PrivateCellUtil.estimatedSerializedSizeOf(c));

        // Since byte buffers can point all kinds of crazy places it's harder to keep track
        // of which blocks are kept alive by what byte buffer.
        // So we make a guess.
        if (c instanceof ByteBufferExtendedCell) {
          ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) c;
          ByteBuffer bb = bbCell.getValueByteBuffer();
          if (bb != lastBlock) {
            context.incrementResponseBlockSize(bb.capacity());
            lastBlock = bb;
          }
        } else {
          // We're using the last block being the same as the current block as
          // a proxy for pointing to a new block. This won't be exact.
          // If there are multiple gets that bounce back and forth
          // Then it's possible that this will over count the size of
          // referenced blocks. However it's better to over count and
          // use two rpcs than to OOME the regionserver.
          byte[] valueArray = c.getValueArray();
          if (valueArray != lastBlock) {
            context.incrementResponseBlockSize(valueArray.length);
            lastBlock = valueArray;
          }
        }

      }
    }
    return lastBlock;
  }

  /**
   * @return Remote client's ip and port else null if can't be determined.
   */
  @RestrictedApi(
    explanation = "Should only be called in TestRSRpcServices and RSRpcServices",
    link = "", allowedOnPath = ".*(TestRSRpcServices|RSRpcServices).java")
  static String getRemoteClientIpAndPort() {
    RpcCall rpcCall = RpcServer.getCurrentCall().orElse(null);
    if (rpcCall == null) {
      return HConstants.EMPTY_STRING;
    }
    InetAddress address = rpcCall.getRemoteAddress();
    if (address == null) {
      return HConstants.EMPTY_STRING;
    }
    // Be careful here with InetAddress. Do InetAddress#getHostAddress. It will not do a name
    // resolution. Just use the IP. It is generally a smaller amount of info to keep around while
    // scanning than a hostname anyways.
    return Address.fromParts(address.getHostAddress(), rpcCall.getRemotePort()).toString();
  }

  /**
   * @return Remote client's username.
   */
  @RestrictedApi(
    explanation = "Should only be called in TestRSRpcServices and RSRpcServices",
    link = "", allowedOnPath = ".*(TestRSRpcServices|RSRpcServices).java")
  static String getUserName() {
    RpcCall rpcCall = RpcServer.getCurrentCall().orElse(null);
    if (rpcCall == null) {
      return HConstants.EMPTY_STRING;
    }
    return rpcCall.getRequestUserName().orElse(HConstants.EMPTY_STRING);
  }

  private RegionScannerHolder addScanner(String scannerName, RegionScanner s, Shipper shipper,
      HRegion r, boolean needCursor, boolean fullRegionScan) throws LeaseStillHeldException {
    Lease lease = server.getLeaseManager().createLease(
        scannerName, this.scannerLeaseTimeoutPeriod, new ScannerListener(scannerName));
    RpcCallback shippedCallback = new RegionScannerShippedCallBack(scannerName, shipper, lease);
    RpcCallback closeCallback = s instanceof RpcCallback?
      (RpcCallback)s: new RegionScannerCloseCallBack(s);
    RegionScannerHolder rsh = new RegionScannerHolder(s, r, closeCallback, shippedCallback,
      needCursor, fullRegionScan, getRemoteClientIpAndPort(), getUserName());
    RegionScannerHolder existing = scanners.putIfAbsent(scannerName, rsh);
    assert existing == null : "scannerId must be unique within regionserver's whole lifecycle! " +
      scannerName + ", " + existing;
    return rsh;
  }

  private boolean isFullRegionScan(Scan scan, HRegion region) {
    // If the scan start row equals or less than the start key of the region
    // and stop row greater than equals end key (if stop row present)
    // or if the stop row is empty
    // account this as a full region scan
    if (Bytes.compareTo(scan.getStartRow(), region.getRegionInfo().getStartKey())  <= 0
      && (Bytes.compareTo(scan.getStopRow(), region.getRegionInfo().getEndKey()) >= 0 &&
      !Bytes.equals(region.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW)
      || Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      return true;
    }
    return false;
  }

  /**
   * Find the HRegion based on a region specifier
   *
   * @param regionSpecifier the region specifier
   * @return the corresponding region
   * @throws IOException if the specifier is not null,
   *    but failed to find the region
   */
  public HRegion getRegion(
      final RegionSpecifier regionSpecifier) throws IOException {
    return server.getRegion(regionSpecifier.getValue().toByteArray());
  }

  /**
   * Find the List of HRegions based on a list of region specifiers
   *
   * @param regionSpecifiers the list of region specifiers
   * @return the corresponding list of regions
   * @throws IOException if any of the specifiers is not null,
   *    but failed to find the region
   */
  private List<HRegion> getRegions(final List<RegionSpecifier> regionSpecifiers,
      final CacheEvictionStatsBuilder stats) {
    List<HRegion> regions = Lists.newArrayListWithCapacity(regionSpecifiers.size());
    for (RegionSpecifier regionSpecifier: regionSpecifiers) {
      try {
        regions.add(server.getRegion(regionSpecifier.getValue().toByteArray()));
      } catch (NotServingRegionException e) {
        stats.addException(regionSpecifier.getValue().toByteArray(), e);
      }
    }
    return regions;
  }

  PriorityFunction getPriority() {
    return priority;
  }

  private RegionServerRpcQuotaManager getRpcQuotaManager() {
    return server.getRegionServerRpcQuotaManager();
  }

  private RegionServerSpaceQuotaManager getSpaceQuotaManager() {
    return server.getRegionServerSpaceQuotaManager();
  }

  void start(ZKWatcher zkWatcher) {
    this.scannerIdGenerator = new ScannerIdGenerator(this.server.getServerName());
    internalStart(zkWatcher);
  }

  void stop() {
    closeAllScanners();
    internalStop();
  }

  /**
   * Called to verify that this server is up and running.
   */
  // TODO : Rename this and HMaster#checkInitialized to isRunning() (or a better name).
  protected void checkOpen() throws IOException {
    if (server.isAborted()) {
      throw new RegionServerAbortedException("Server " + server.getServerName() + " aborting");
    }
    if (server.isStopped()) {
      throw new RegionServerStoppedException("Server " + server.getServerName() + " stopping");
    }
    if (!server.isDataFileSystemOk()) {
      throw new RegionServerStoppedException("File system not available");
    }
    if (!server.isOnline()) {
      throw new ServerNotRunningYetException("Server " + server.getServerName()
          + " is not running yet");
    }
  }

  /**
   * By default, put up an Admin and a Client Service.
   * Set booleans <code>hbase.regionserver.admin.executorService</code> and
   * <code>hbase.regionserver.client.executorService</code> if you want to enable/disable services.
   * Default is that both are enabled.
   * @return immutable list of blocking services and the security info classes that this server
   * supports
   */
  protected List<BlockingServiceAndInterface> getServices() {
    boolean admin = getConfiguration().getBoolean(REGIONSERVER_ADMIN_SERVICE_CONFIG, true);
    boolean client = getConfiguration().getBoolean(REGIONSERVER_CLIENT_SERVICE_CONFIG, true);
    boolean clientMeta =
      getConfiguration().getBoolean(REGIONSERVER_CLIENT_META_SERVICE_CONFIG, true);
    List<BlockingServiceAndInterface> bssi = new ArrayList<>();
    if (client) {
      bssi.add(new BlockingServiceAndInterface(ClientService.newReflectiveBlockingService(this),
        ClientService.BlockingInterface.class));
    }
    if (admin) {
      bssi.add(new BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
        AdminService.BlockingInterface.class));
    }
    if (clientMeta) {
      bssi.add(new BlockingServiceAndInterface(ClientMetaService.newReflectiveBlockingService(this),
        ClientMetaService.BlockingInterface.class));
    }
    return new ImmutableList.Builder<BlockingServiceAndInterface>().addAll(bssi).build();
  }

  /**
   * Close a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public CloseRegionResponse closeRegion(final RpcController controller,
      final CloseRegionRequest request) throws ServiceException {
    final ServerName sn = (request.hasDestinationServer() ?
      ProtobufUtil.toServerName(request.getDestinationServer()) : null);

    try {
      checkOpen();
      throwOnWrongStartCode(request);
      final String encodedRegionName = ProtobufUtil.getRegionEncodedName(request.getRegion());

      requestCount.increment();
      if (sn == null) {
        LOG.info("Close " + encodedRegionName + " without moving");
      } else {
        LOG.info("Close " + encodedRegionName + ", moving to " + sn);
      }
      boolean closed = server.closeRegion(encodedRegionName, false, sn);
      CloseRegionResponse.Builder builder = CloseRegionResponse.newBuilder().setClosed(closed);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Compact a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
      final CompactRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      // Quota support is enabled, the requesting user is not system/super user
      // and a quota policy is enforced that disables compactions.
      if (QuotaUtil.isQuotaEnabled(getConfiguration()) &&
          !Superusers.isSuperUser(RpcServer.getRequestUser().orElse(null)) &&
          this.server.getRegionServerSpaceQuotaManager()
              .areCompactionsDisabled(region.getTableDescriptor().getTableName())) {
        throw new DoNotRetryIOException(
            "Compactions on this region are " + "disabled due to a space quota violation.");
      }
      region.startRegionOperation(Operation.COMPACT_REGION);
      LOG.info("Compacting " + region.getRegionInfo().getRegionNameAsString());
      boolean major = request.hasMajor() && request.getMajor();
      if (request.hasFamily()) {
        byte[] family = request.getFamily().toByteArray();
        String log = "User-triggered " + (major ? "major " : "") + "compaction for region " +
            region.getRegionInfo().getRegionNameAsString() + " and family " +
            Bytes.toString(family);
        LOG.trace(log);
        region.requestCompaction(family, log, Store.PRIORITY_USER, major,
          CompactionLifeCycleTracker.DUMMY);
      } else {
        String log = "User-triggered " + (major ? "major " : "") + "compaction for region " +
            region.getRegionInfo().getRegionNameAsString();
        LOG.trace(log);
        region.requestCompaction(log, Store.PRIORITY_USER, major, CompactionLifeCycleTracker.DUMMY);
      }
      return CompactRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CompactionSwitchResponse compactionSwitch(RpcController controller,
      CompactionSwitchRequest request) throws ServiceException {
    rpcPreCheck("compactionSwitch");
    final CompactSplit compactSplitThread = server.getCompactSplitThread();
    requestCount.increment();
    boolean prevState = compactSplitThread.isCompactionsEnabled();
    CompactionSwitchResponse response =
        CompactionSwitchResponse.newBuilder().setPrevState(prevState).build();
    if (prevState == request.getEnabled()) {
      // passed in requested state is same as current state. No action required
      return response;
    }
    compactSplitThread.switchCompaction(request.getEnabled());
    return response;
  }

  /**
   * Flush a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public FlushRegionResponse flushRegion(final RpcController controller,
      final FlushRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionInfo().getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getEarliestFlushTimeForAllStores() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        boolean writeFlushWalMarker =  request.hasWriteFlushWalMarker() ?
            request.getWriteFlushWalMarker() : false;
        // Go behind the curtain so we can manage writing of the flush WAL marker
        HRegion.FlushResultImpl flushResult = null;
        if (request.hasFamily()) {
          List families = new ArrayList();
          families.add(request.getFamily().toByteArray());
          flushResult =
            region.flushcache(families, writeFlushWalMarker, FlushLifeCycleTracker.DUMMY);
        } else {
          flushResult = region.flushcache(true, writeFlushWalMarker, FlushLifeCycleTracker.DUMMY);
        }
        boolean compactionNeeded = flushResult.isCompactionNeeded();
        if (compactionNeeded) {
          server.getCompactSplitThread().requestSystemCompaction(region,
            "Compaction through user triggered flush");
        }
        builder.setFlushed(flushResult.isFlushSucceeded());
        builder.setWroteFlushWalMarker(flushResult.wroteFlushWalMarker);
      }
      builder.setLastFlushTime(region.getEarliestFlushTimeForAllStores());
      return builder.build();
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of wal
      // is required. Currently the only way to do this is a restart of
      // the server.
      server.abort("Replay of WAL required. Forcing server shutdown", ex);
      throw new ServiceException(ex);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetOnlineRegionResponse getOnlineRegion(final RpcController controller,
      final GetOnlineRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      Map<String, HRegion> onlineRegions = server.getOnlineRegions();
      List<RegionInfo> list = new ArrayList<>(onlineRegions.size());
      for (HRegion region: onlineRegions.values()) {
        list.add(region.getRegionInfo());
      }
      list.sort(RegionInfo.COMPARATOR);
      return ResponseConverter.buildGetOnlineRegionResponse(list);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  // Master implementation of this Admin Service differs given it is not
  // able to supply detail only known to RegionServer. See note on
  // MasterRpcServers#getRegionInfo.
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
    final GetRegionInfoRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      RegionInfo info = region.getRegionInfo();
      byte[] bestSplitRow;
      if (request.hasBestSplitRow() && request.getBestSplitRow()) {
        bestSplitRow = region.checkSplit(true).orElse(null);
        // when all table data are in memstore, bestSplitRow = null
        // try to flush region first
        if (bestSplitRow == null) {
          region.flush(true);
          bestSplitRow = region.checkSplit(true).orElse(null);
        }
      } else {
        bestSplitRow = null;
      }
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(ProtobufUtil.toRegionInfo(info));
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(ProtobufUtil.createCompactionState(region.getCompactionState()));
      }
      builder.setSplittable(region.isSplittable());
      builder.setMergeable(region.isMergeable());
      if (request.hasBestSplitRow() && request.getBestSplitRow() && bestSplitRow != null) {
        builder.setBestSplitRow(UnsafeByteOperations.unsafeWrap(bestSplitRow));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetRegionLoadResponse getRegionLoad(RpcController controller,
      GetRegionLoadRequest request) throws ServiceException {

    List<HRegion> regions;
    if (request.hasTableName()) {
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      regions = server.getRegions(tableName);
    } else {
      regions = server.getRegions();
    }
    List<RegionLoad> rLoads = new ArrayList<>(regions.size());
    RegionLoad.Builder regionLoadBuilder = ClusterStatusProtos.RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();

    try {
      for (HRegion region : regions) {
        rLoads.add(server.createRegionLoad(region, regionLoadBuilder, regionSpecifier));
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetRegionLoadResponse.Builder builder = GetRegionLoadResponse.newBuilder();
    builder.addAllRegionLoads(rLoads);
    return builder.build();
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller,
    ClearCompactionQueuesRequest request) throws ServiceException {
    LOG.debug("Client=" + RpcServer.getRequestUserName().orElse(null) + "/"
        + RpcServer.getRemoteAddress().orElse(null) + " clear compactions queue");
    ClearCompactionQueuesResponse.Builder respBuilder = ClearCompactionQueuesResponse.newBuilder();
    requestCount.increment();
    if (clearCompactionQueues.compareAndSet(false,true)) {
      final CompactSplit compactSplitThread = server.getCompactSplitThread();
      try {
        checkOpen();
        server.getRegionServerCoprocessorHost().preClearCompactionQueues();
        for (String queueName : request.getQueueNameList()) {
          LOG.debug("clear " + queueName + " compaction queue");
          switch (queueName) {
            case "long":
              compactSplitThread.clearLongCompactionsQueue();
              break;
            case "short":
              compactSplitThread.clearShortCompactionsQueue();
              break;
            default:
              LOG.warn("Unknown queue name " + queueName);
              throw new IOException("Unknown queue name " + queueName);
          }
        }
        server.getRegionServerCoprocessorHost().postClearCompactionQueues();
      } catch (IOException ie) {
        throw new ServiceException(ie);
      } finally {
        clearCompactionQueues.set(false);
      }
    } else {
      LOG.warn("Clear compactions queue is executing by other admin.");
    }
    return respBuilder.build();
  }

  /**
   * Get some information of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetServerInfoResponse getServerInfo(final RpcController controller,
      final GetServerInfoRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    requestCount.increment();
    int infoPort = server.getInfoServer() != null ? server.getInfoServer().getPort() : -1;
    return ResponseConverter.buildGetServerInfoResponse(server.getServerName(), infoPort);
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetStoreFileResponse getStoreFile(final RpcController controller,
      final GetStoreFileRequest request) throws ServiceException {
    try {
      checkOpen();
      HRegion region = getRegion(request.getRegion());
      requestCount.increment();
      Set<byte[]> columnFamilies;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getTableDescriptor().getColumnFamilyNames();
      } else {
        columnFamilies = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
        for (ByteString cf: request.getFamilyList()) {
          columnFamilies.add(cf.toByteArray());
        }
      }
      int nCF = columnFamilies.size();
      List<String>  fileList = region.getStoreFileList(
        columnFamilies.toArray(new byte[nCF][]));
      GetStoreFileResponse.Builder builder = GetStoreFileResponse.newBuilder();
      builder.addAllStoreFile(fileList);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private void throwOnWrongStartCode(OpenRegionRequest request) throws ServiceException {
    if (!request.hasServerStartCode()) {
      LOG.warn("OpenRegionRequest for {} does not have a start code", request.getOpenInfoList());
      return;
    }
    throwOnWrongStartCode(request.getServerStartCode());
  }

  private void throwOnWrongStartCode(CloseRegionRequest request) throws ServiceException {
    if (!request.hasServerStartCode()) {
      LOG.warn("CloseRegionRequest for {} does not have a start code", request.getRegion());
      return;
    }
    throwOnWrongStartCode(request.getServerStartCode());
  }

  private void throwOnWrongStartCode(long serverStartCode) throws ServiceException {
    // check that we are the same server that this RPC is intended for.
    if (server.getServerName().getStartcode() != serverStartCode) {
      throw new ServiceException(new DoNotRetryIOException(
        "This RPC was intended for a " + "different server with startCode: " + serverStartCode +
          ", this server is: " + server.getServerName()));
    }
  }

  private void throwOnWrongStartCode(ExecuteProceduresRequest req) throws ServiceException {
    if (req.getOpenRegionCount() > 0) {
      for (OpenRegionRequest openReq : req.getOpenRegionList()) {
        throwOnWrongStartCode(openReq);
      }
    }
    if (req.getCloseRegionCount() > 0) {
      for (CloseRegionRequest closeReq : req.getCloseRegionList()) {
        throwOnWrongStartCode(closeReq);
      }
    }
  }

  /**
   * Open asynchronously a region or a set of regions on the region server.
   *
   * The opening is coordinated by ZooKeeper, and this method requires the znode to be created
   *  before being called. As a consequence, this method should be called only from the master.
   * <p>
   * Different manages states for the region are:
   * </p><ul>
   *  <li>region not opened: the region opening will start asynchronously.</li>
   *  <li>a close is already in progress: this is considered as an error.</li>
   *  <li>an open is already in progress: this new open request will be ignored. This is important
   *  because the Master can do multiple requests if it crashes.</li>
   *  <li>the region is already opened:  this new open request will be ignored.</li>
   *  </ul>
   * <p>
   * Bulk assign: If there are more than 1 region to open, it will be considered as a bulk assign.
   * For a single region opening, errors are sent through a ServiceException. For bulk assign,
   * errors are put in the response as FAILED_OPENING.
   * </p>
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public OpenRegionResponse openRegion(final RpcController controller,
      final OpenRegionRequest request) throws ServiceException {
    requestCount.increment();
    throwOnWrongStartCode(request);

    OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
    final int regionCount = request.getOpenInfoCount();
    final Map<TableName, TableDescriptor> htds = new HashMap<>(regionCount);
    final boolean isBulkAssign = regionCount > 1;
    try {
      checkOpen();
    } catch (IOException ie) {
      TableName tableName = null;
      if (regionCount == 1) {
        org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo ri = request.getOpenInfo(0).getRegion();
        if (ri != null) {
          tableName = ProtobufUtil.toTableName(ri.getTableName());
        }
      }
      if (!TableName.META_TABLE_NAME.equals(tableName)) {
        throw new ServiceException(ie);
      }
      // We are assigning meta, wait a little for regionserver to finish initialization.
      // Default to quarter of RPC timeout
      int timeout = server.getConfiguration()
        .getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT) >> 2;
      long endTime = EnvironmentEdgeManager.currentTime() + timeout;
      synchronized (server.online) {
        try {
          while (EnvironmentEdgeManager.currentTime() <= endTime
              && !server.isStopped() && !server.isOnline()) {
            server.online.wait(server.getMsgInterval());
          }
          checkOpen();
        } catch (InterruptedException t) {
          Thread.currentThread().interrupt();
          throw new ServiceException(t);
        } catch (IOException e) {
          throw new ServiceException(e);
        }
      }
    }

    long masterSystemTime = request.hasMasterSystemTime() ? request.getMasterSystemTime() : -1;

    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      final RegionInfo region = ProtobufUtil.toRegionInfo(regionOpenInfo.getRegion());
      TableDescriptor htd;
      try {
        String encodedName = region.getEncodedName();
        byte[] encodedNameBytes = region.getEncodedNameAsBytes();
        final HRegion onlineRegion = server.getRegion(encodedName);
        if (onlineRegion != null) {
          // The region is already online. This should not happen any more.
          String error = "Received OPEN for the region:"
            + region.getRegionNameAsString() + ", which is already online";
          LOG.warn(error);
          //server.abort(error);
          //throw new IOException(error);
          builder.addOpeningState(RegionOpeningState.OPENED);
          continue;
        }
        LOG.info("Open " + region.getRegionNameAsString());

        final Boolean previous = server.getRegionsInTransitionInRS().putIfAbsent(
          encodedNameBytes, Boolean.TRUE);

        if (Boolean.FALSE.equals(previous)) {
          if (server.getRegion(encodedName) != null) {
            // There is a close in progress. This should not happen any more.
            String error = "Received OPEN for the region:"
              + region.getRegionNameAsString() + ", which we are already trying to CLOSE";
            server.abort(error);
            throw new IOException(error);
          }
          server.getRegionsInTransitionInRS().put(encodedNameBytes, Boolean.TRUE);
        }

        if (Boolean.TRUE.equals(previous)) {
          // An open is in progress. This is supported, but let's log this.
          LOG.info("Receiving OPEN for the region:" +
            region.getRegionNameAsString() + ", which we are already trying to OPEN"
              + " - ignoring this new request for this region.");
        }

        // We are opening this region. If it moves back and forth for whatever reason, we don't
        // want to keep returning the stale moved record while we are opening/if we close again.
        server.removeFromMovedRegions(region.getEncodedName());

        if (previous == null || !previous.booleanValue()) {
          htd = htds.get(region.getTable());
          if (htd == null) {
            htd = server.getTableDescriptors().get(region.getTable());
            htds.put(region.getTable(), htd);
          }
          if (htd == null) {
            throw new IOException("Missing table descriptor for " + region.getEncodedName());
          }
          // If there is no action in progress, we can submit a specific handler.
          // Need to pass the expected version in the constructor.
          if (server.getExecutorService() == null) {
            LOG.info("No executor executorService; skipping open request");
          } else {
            if (region.isMetaRegion()) {
              server.getExecutorService()
                .submit(new OpenMetaHandler(server, server, region, htd, masterSystemTime));
            } else {
              if (regionOpenInfo.getFavoredNodesCount() > 0) {
                server.updateRegionFavoredNodesMapping(region.getEncodedName(),
                  regionOpenInfo.getFavoredNodesList());
              }
              if (htd.getPriority() >= HConstants.ADMIN_QOS || region.getTable().isSystemTable()) {
                server.getExecutorService().submit(
                  new OpenPriorityRegionHandler(server, server, region, htd, masterSystemTime));
              } else {
                server.getExecutorService()
                  .submit(new OpenRegionHandler(server, server, region, htd, masterSystemTime));
              }
            }
          }
        }

        builder.addOpeningState(RegionOpeningState.OPENED);
      } catch (IOException ie) {
        LOG.warn("Failed opening region " + region.getRegionNameAsString(), ie);
        if (isBulkAssign) {
          builder.addOpeningState(RegionOpeningState.FAILED_OPENING);
        } else {
          throw new ServiceException(ie);
        }
      }
    }
    return builder.build();
  }

  /**
   * Warmup a region on this server.
   * This method should only be called by Master. It synchronously opens the region and
   * closes the region bringing the most important pages in cache.
   */
  @Override
  public WarmupRegionResponse warmupRegion(final RpcController controller,
      final WarmupRegionRequest request) throws ServiceException {
    final RegionInfo region = ProtobufUtil.toRegionInfo(request.getRegionInfo());
    WarmupRegionResponse response = WarmupRegionResponse.getDefaultInstance();
    try {
      checkOpen();
      String encodedName = region.getEncodedName();
      byte[] encodedNameBytes = region.getEncodedNameAsBytes();
      final HRegion onlineRegion = server.getRegion(encodedName);
      if (onlineRegion != null) {
        LOG.info("{} is online; skipping warmup", region);
        return response;
      }
      TableDescriptor htd = server.getTableDescriptors().get(region.getTable());
      if (server.getRegionsInTransitionInRS().containsKey(encodedNameBytes)) {
        LOG.info("{} is in transition; skipping warmup", region);
        return response;
      }
      LOG.info("Warmup {}", region.getRegionNameAsString());
      HRegion.warmupHRegion(region, htd, server.getWAL(region),
        server.getConfiguration(), server, null);
    } catch (IOException ie) {
      LOG.error("Failed warmup of {}", region.getRegionNameAsString(), ie);
      throw new ServiceException(ie);
    }

    return response;
  }

  private CellScanner getAndReset(RpcController controller) {
    HBaseRpcController hrc = (HBaseRpcController) controller;
    CellScanner cells = hrc.cellScanner();
    hrc.setCellScanner(null);
    return cells;
  }

  /**
   * Replay the given changes when distributedLogReplay WAL edits from a failed RS. The guarantee is
   * that the given mutations will be durable on the receiving RS if this method returns without any
   * exception.
   * @param controller the RPC controller
   * @param request the request
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Not used any more, put here only for
   *             compatibility with old region replica implementation. Now we will use
   *             {@code replicateToReplica} method instead.
   */
  @Deprecated
  @Override
  @QosPriority(priority = HConstants.REPLAY_QOS)
  public ReplicateWALEntryResponse replay(final RpcController controller,
    final ReplicateWALEntryRequest request) throws ServiceException {
    long before = EnvironmentEdgeManager.currentTime();
    CellScanner cells = getAndReset(controller);
    try {
      checkOpen();
      List<WALEntry> entries = request.getEntryList();
      if (entries == null || entries.isEmpty()) {
        // empty input
        return ReplicateWALEntryResponse.newBuilder().build();
      }
      ByteString regionName = entries.get(0).getKey().getEncodedRegionName();
      HRegion region = server.getRegionByEncodedName(regionName.toStringUtf8());
      RegionCoprocessorHost coprocessorHost =
          ServerRegionReplicaUtil.isDefaultReplica(region.getRegionInfo())
            ? region.getCoprocessorHost()
            : null; // do not invoke coprocessors if this is a secondary region replica
      List<Pair<WALKey, WALEdit>> walEntries = new ArrayList<>();

      // Skip adding the edits to WAL if this is a secondary region replica
      boolean isPrimary = RegionReplicaUtil.isDefaultReplica(region.getRegionInfo());
      Durability durability = isPrimary ? Durability.USE_DEFAULT : Durability.SKIP_WAL;

      for (WALEntry entry : entries) {
        if (!regionName.equals(entry.getKey().getEncodedRegionName())) {
          throw new NotServingRegionException("Replay request contains entries from multiple " +
              "regions. First region:" + regionName.toStringUtf8() + " , other region:"
              + entry.getKey().getEncodedRegionName());
        }
        if (server.nonceManager != null && isPrimary) {
          long nonceGroup = entry.getKey().hasNonceGroup()
            ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          server.nonceManager.reportOperationFromWal(
              nonceGroup,
              nonce,
              entry.getKey().getWriteTime());
        }
        Pair<WALKey, WALEdit> walEntry = (coprocessorHost == null) ? null : new Pair<>();
        List<MutationReplay> edits = WALSplitUtil.getMutationsFromWALEntry(entry,
          cells, walEntry, durability);
        if (coprocessorHost != null) {
          // Start coprocessor replay here. The coprocessor is for each WALEdit instead of a
          // KeyValue.
          if (coprocessorHost.preWALRestore(region.getRegionInfo(), walEntry.getFirst(),
            walEntry.getSecond())) {
            // if bypass this log entry, ignore it ...
            continue;
          }
          walEntries.add(walEntry);
        }
        if(edits!=null && !edits.isEmpty()) {
          // HBASE-17924
          // sort to improve lock efficiency
          Collections.sort(edits, (v1, v2) -> Row.COMPARATOR.compare(v1.mutation, v2.mutation));
          long replaySeqId = (entry.getKey().hasOrigSequenceNumber()) ?
            entry.getKey().getOrigSequenceNumber() : entry.getKey().getLogSequenceNumber();
          OperationStatus[] result = doReplayBatchOp(region, edits, replaySeqId);
          // check if it's a partial success
          for (int i = 0; result != null && i < result.length; i++) {
            if (result[i] != OperationStatus.SUCCESS) {
              throw new IOException(result[i].getExceptionMsg());
            }
          }
        }
      }

      //sync wal at the end because ASYNC_WAL is used above
      WAL wal = region.getWAL();
      if (wal != null) {
        wal.sync();
      }

      if (coprocessorHost != null) {
        for (Pair<WALKey, WALEdit> entry : walEntries) {
          coprocessorHost.postWALRestore(region.getRegionInfo(), entry.getFirst(),
            entry.getSecond());
        }
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = server.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateReplay(EnvironmentEdgeManager.currentTime() - before);
      }
    }
  }

  /**
   * Replay the given changes on a secondary replica
   */
  @Override
  public ReplicateWALEntryResponse replicateToReplica(RpcController controller,
    ReplicateWALEntryRequest request) throws ServiceException {
    CellScanner cells = getAndReset(controller);
    try {
      checkOpen();
      List<WALEntry> entries = request.getEntryList();
      if (entries == null || entries.isEmpty()) {
        // empty input
        return ReplicateWALEntryResponse.newBuilder().build();
      }
      ByteString regionName = entries.get(0).getKey().getEncodedRegionName();
      HRegion region = server.getRegionByEncodedName(regionName.toStringUtf8());
      if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
        throw new DoNotRetryIOException(
          "Should not replicate to primary replica " + region.getRegionInfo() + ", CODE BUG?");
      }
      for (WALEntry entry : entries) {
        if (!regionName.equals(entry.getKey().getEncodedRegionName())) {
          throw new NotServingRegionException(
            "ReplicateToReplica request contains entries from multiple " +
              "regions. First region:" + regionName.toStringUtf8() + " , other region:" +
              entry.getKey().getEncodedRegionName());
        }
        region.replayWALEntry(entry, cells);
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private void checkShouldRejectReplicationRequest(List<WALEntry> entries) throws IOException {
    ReplicationSourceService replicationSource = server.getReplicationSourceService();
    if (replicationSource == null || entries.isEmpty()) {
      return;
    }
    // We can ensure that all entries are for one peer, so only need to check one entry's
    // table name. if the table hit sync replication at peer side and the peer cluster
    // is (or is transiting to) state ACTIVE or DOWNGRADE_ACTIVE, we should reject to apply
    // those entries according to the design doc.
    TableName table = TableName.valueOf(entries.get(0).getKey().getTableName().toByteArray());
    if (replicationSource.getSyncReplicationPeerInfoProvider().checkState(table,
      RejectReplicationRequestStateChecker.get())) {
      throw new DoNotRetryIOException(
          "Reject to apply to sink cluster because sync replication state of sink cluster "
              + "is ACTIVE or DOWNGRADE_ACTIVE, table: " + table);
    }
  }

  /**
   * Replicate WAL entries on the region server.
   * @param controller the RPC controller
   * @param request the request
   */
  @Override
  @QosPriority(priority=HConstants.REPLICATION_QOS)
  public ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
      final ReplicateWALEntryRequest request) throws ServiceException {
    try {
      checkOpen();
      if (server.getReplicationSinkService() != null) {
        requestCount.increment();
        List<WALEntry> entries = request.getEntryList();
        checkShouldRejectReplicationRequest(entries);
        CellScanner cellScanner = getAndReset(controller);
        server.getRegionServerCoprocessorHost().preReplicateLogEntries();
        server.getReplicationSinkService().replicateLogEntries(entries, cellScanner,
          request.getReplicationClusterId(), request.getSourceBaseNamespaceDirPath(),
          request.getSourceHFileArchiveDirPath());
        server.getRegionServerCoprocessorHost().postReplicateLogEntries();
        return ReplicateWALEntryResponse.newBuilder().build();
      } else {
        throw new ServiceException("Replication services are not initialized yet");
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
      final RollWALWriterRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      server.getRegionServerCoprocessorHost().preRollWALWriterRequest();
      server.getWalRoller().requestRollAll();
      server.getRegionServerCoprocessorHost().postRollWALWriterRequest();
      RollWALWriterResponse.Builder builder = RollWALWriterResponse.newBuilder();
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }


  /**
   * Stop the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public StopServerResponse stopServer(final RpcController controller,
      final StopServerRequest request) throws ServiceException {
    rpcPreCheck("stopServer");
    requestCount.increment();
    String reason = request.getReason();
    server.stop(reason);
    return StopServerResponse.newBuilder().build();
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
      UpdateFavoredNodesRequest request) throws ServiceException {
    rpcPreCheck("updateFavoredNodes");
    List<UpdateFavoredNodesRequest.RegionUpdateInfo> openInfoList = request.getUpdateInfoList();
    UpdateFavoredNodesResponse.Builder respBuilder = UpdateFavoredNodesResponse.newBuilder();
    for (UpdateFavoredNodesRequest.RegionUpdateInfo regionUpdateInfo : openInfoList) {
      RegionInfo hri = ProtobufUtil.toRegionInfo(regionUpdateInfo.getRegion());
      if (regionUpdateInfo.getFavoredNodesCount() > 0) {
        server.updateRegionFavoredNodesMapping(hri.getEncodedName(),
          regionUpdateInfo.getFavoredNodesList());
      }
    }
    respBuilder.setResponse(openInfoList.size());
    return respBuilder.build();
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws ServiceException if failed unrecoverably
   */
  @Override
  public BulkLoadHFileResponse bulkLoadHFile(final RpcController controller,
      final BulkLoadHFileRequest request) throws ServiceException {
    long start = EnvironmentEdgeManager.currentTime();
    List<String> clusterIds = new ArrayList<>(request.getClusterIdsList());
    if(clusterIds.contains(this.server.getClusterId())){
      return BulkLoadHFileResponse.newBuilder().setLoaded(true).build();
    } else {
      clusterIds.add(this.server.getClusterId());
    }
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      final boolean spaceQuotaEnabled = QuotaUtil.isQuotaEnabled(getConfiguration());
      long sizeToBeLoaded = -1;

      // Check to see if this bulk load would exceed the space quota for this table
      if (spaceQuotaEnabled) {
        ActivePolicyEnforcement activeSpaceQuotas = getSpaceQuotaManager().getActiveEnforcements();
        SpaceViolationPolicyEnforcement enforcement = activeSpaceQuotas.getPolicyEnforcement(
            region);
        if (enforcement != null) {
          // Bulk loads must still be atomic. We must enact all or none.
          List<String> filePaths = new ArrayList<>(request.getFamilyPathCount());
          for (FamilyPath familyPath : request.getFamilyPathList()) {
            filePaths.add(familyPath.getPath());
          }
          // Check if the batch of files exceeds the current quota
          sizeToBeLoaded = enforcement.computeBulkLoadSize(getFileSystem(filePaths), filePaths);
        }
      }
      // secure bulk load
      Map<byte[], List<Path>> map =
        server.getSecureBulkLoadManager().secureBulkLoadHFiles(region, request, clusterIds);
      BulkLoadHFileResponse.Builder builder = BulkLoadHFileResponse.newBuilder();
      builder.setLoaded(map != null);
      if (map != null) {
        // Treat any negative size as a flag to "ignore" updating the region size as that is
        // not possible to occur in real life (cannot bulk load a file with negative size)
        if (spaceQuotaEnabled && sizeToBeLoaded > 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Incrementing space use of " + region.getRegionInfo() + " by "
                + sizeToBeLoaded + " bytes");
          }
          // Inform space quotas of the new files for this region
          getSpaceQuotaManager().getRegionSizeStore().incrementRegionSize(
              region.getRegionInfo(), sizeToBeLoaded);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = server.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateBulkLoad(EnvironmentEdgeManager.currentTime() - start);
      }
    }
  }

  @Override
  public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller,
      PrepareBulkLoadRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();

      HRegion region = getRegion(request.getRegion());

      String bulkToken = server.getSecureBulkLoadManager().prepareBulkLoad(region, request);
      PrepareBulkLoadResponse.Builder builder = PrepareBulkLoadResponse.newBuilder();
      builder.setBulkToken(bulkToken);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller,
      CleanupBulkLoadRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();

      HRegion region = getRegion(request.getRegion());

      server.getSecureBulkLoadManager().cleanupBulkLoad(region, request);
      return CleanupBulkLoadResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CoprocessorServiceResponse execService(final RpcController controller,
      final CoprocessorServiceRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      Message result = execServiceOnRegion(region, request.getCall());
      CoprocessorServiceResponse.Builder builder = CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(
        RegionSpecifierType.REGION_NAME, region.getRegionInfo().getRegionName()));
      // TODO: COPIES!!!!!!
      builder.setValue(builder.getValueBuilder().setName(result.getClass().getName()).
        setValue(org.apache.hbase.thirdparty.com.google.protobuf.ByteString.
            copyFrom(result.toByteArray())));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private FileSystem getFileSystem(List<String> filePaths) throws IOException {
    if (filePaths.isEmpty()) {
      // local hdfs
      return server.getFileSystem();
    }
    // source hdfs
    return new Path(filePaths.get(0)).getFileSystem(server.getConfiguration());
  }

  private Message execServiceOnRegion(HRegion region,
      final ClientProtos.CoprocessorServiceCall serviceCall) throws IOException {
    // ignore the passed in controller (from the serialized call)
    ServerRpcController execController = new ServerRpcController();
    return region.execService(execController, serviceCall);
  }

  private boolean shouldRejectRequestsFromClient(HRegion region) {
    TableName table = region.getRegionInfo().getTable();
    ReplicationSourceService service = server.getReplicationSourceService();
    return service != null && service.getSyncReplicationPeerInfoProvider()
            .checkState(table, RejectRequestsFromClientStateChecker.get());
  }

  private void rejectIfInStandByState(HRegion region) throws DoNotRetryIOException {
    if (shouldRejectRequestsFromClient(region)) {
      throw new DoNotRetryIOException(
        region.getRegionInfo().getRegionNameAsString() + " is in STANDBY state.");
    }
  }

  /**
   * Get data from a table.
   *
   * @param controller the RPC controller
   * @param request the get request
   * @throws ServiceException
   */
  @Override
  public GetResponse get(final RpcController controller, final GetRequest request)
      throws ServiceException {
    long before = EnvironmentEdgeManager.currentTime();
    OperationQuota quota = null;
    HRegion region = null;
    try {
      checkOpen();
      requestCount.increment();
      rpcGetRequestCount.increment();
      region = getRegion(request.getRegion());
      rejectIfInStandByState(region);

      GetResponse.Builder builder = GetResponse.newBuilder();
      ClientProtos.Get get = request.getGet();
      // An asynchbase client, https://github.com/OpenTSDB/asynchbase, starts by trying to do
      // a get closest before. Throwing the UnknownProtocolException signals it that it needs
      // to switch and do hbase2 protocol (HBase servers do not tell clients what versions
      // they are; its a problem for non-native clients like asynchbase. HBASE-20225.
      if (get.hasClosestRowBefore() && get.getClosestRowBefore()) {
        throw new UnknownProtocolException("Is this a pre-hbase-1.0.0 or asynchbase client? " +
            "Client is invoking getClosestRowBefore removed in hbase-2.0.0 replaced by " +
            "reverse Scan.");
      }
      Boolean existence = null;
      Result r = null;
      RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
      quota = getRpcQuotaManager().checkQuota(region, OperationQuota.OperationType.GET);

      Get clientGet = ProtobufUtil.toGet(get);
      if (get.getExistenceOnly() && region.getCoprocessorHost() != null) {
        existence = region.getCoprocessorHost().preExists(clientGet);
      }
      if (existence == null) {
        if (context != null) {
          r = get(clientGet, (region), null, context);
        } else {
          // for test purpose
          r = region.get(clientGet);
        }
        if (get.getExistenceOnly()) {
          boolean exists = r.getExists();
          if (region.getCoprocessorHost() != null) {
            exists = region.getCoprocessorHost().postExists(clientGet, exists);
          }
          existence = exists;
        }
      }
      if (existence != null) {
        ClientProtos.Result pbr =
            ProtobufUtil.toResult(existence, region.getRegionInfo().getReplicaId() != 0);
        builder.setResult(pbr);
      } else if (r != null) {
        ClientProtos.Result pbr;
        if (isClientCellBlockSupport(context) && controller instanceof HBaseRpcController
            && VersionInfoUtil.hasMinimumVersion(context.getClientVersionInfo(), 1, 3)) {
          pbr = ProtobufUtil.toResultNoData(r);
          ((HBaseRpcController) controller).setCellScanner(CellUtil.createCellScanner(r
              .rawCells()));
          addSize(context, r, null);
        } else {
          pbr = ProtobufUtil.toResult(r);
        }
        builder.setResult(pbr);
      }
      //r.cells is null when an table.exists(get) call
      if (r != null && r.rawCells() != null) {
        quota.addGetResult(r);
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = server.getMetrics();
      if (metricsRegionServer != null) {
        TableDescriptor td = region != null? region.getTableDescriptor(): null;
        if (td != null) {
          metricsRegionServer.updateGet(
              td.getTableName(), EnvironmentEdgeManager.currentTime() - before);
        }
      }
      if (quota != null) {
        quota.close();
      }
    }
  }

  private Result get(Get get, HRegion region, RegionScannersCloseCallBack closeCallBack,
      RpcCallContext context) throws IOException {
    region.prepareGet(get);
    boolean stale = region.getRegionInfo().getReplicaId() != 0;

    // This method is almost the same as HRegion#get.
    List<Cell> results = new ArrayList<>();
    long before = EnvironmentEdgeManager.currentTime();
    // pre-get CP hook
    if (region.getCoprocessorHost() != null) {
      if (region.getCoprocessorHost().preGet(get, results)) {
        region.metricsUpdateForGet(results, before);
        return Result
            .create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
      }
    }
    Scan scan = new Scan(get);
    if (scan.getLoadColumnFamiliesOnDemandValue() == null) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }
    RegionScannerImpl scanner = null;
    try {
      scanner = region.getScanner(scan);
      scanner.next(results);
    } finally {
      if (scanner != null) {
        if (closeCallBack == null) {
          // If there is a context then the scanner can be added to the current
          // RpcCallContext. The rpc callback will take care of closing the
          // scanner, for eg in case
          // of get()
          context.setCallBack(scanner);
        } else {
          // The call is from multi() where the results from the get() are
          // aggregated and then send out to the
          // rpc. The rpccall back will close all such scanners created as part
          // of multi().
          closeCallBack.addScanner(scanner);
        }
      }
    }

    // post-get CP hook
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postGet(get, results);
    }
    region.metricsUpdateForGet(results, before);

    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  private void checkBatchSizeAndLogLargeSize(MultiRequest request) throws ServiceException {
    int sum = 0;
    String firstRegionName = null;
    for (RegionAction regionAction : request.getRegionActionList()) {
      if (sum == 0) {
        firstRegionName = Bytes.toStringBinary(regionAction.getRegion().getValue().toByteArray());
      }
      sum += regionAction.getActionCount();
    }
    if (sum > rowSizeWarnThreshold) {
      LOG.warn("Large batch operation detected (greater than " + rowSizeWarnThreshold +
        ") (HBASE-18023)." + " Requested Number of Rows: " + sum + " Client: " +
        RpcServer.getRequestUserName().orElse(null) + "/" +
        RpcServer.getRemoteAddress().orElse(null) + " first region in multi=" + firstRegionName);
      if (rejectRowsWithSizeOverThreshold) {
        throw new ServiceException(
          "Rejecting large batch operation for current batch with firstRegionName: " +
            firstRegionName + " , Requested Number of Rows: " + sum + " , Size Threshold: " +
            rowSizeWarnThreshold);
      }
    }
  }

  private void failRegionAction(MultiResponse.Builder responseBuilder,
      RegionActionResult.Builder regionActionResultBuilder, RegionAction regionAction,
      CellScanner cellScanner, Throwable error) {
    rpcServer.getMetrics().exception(error);
    regionActionResultBuilder.setException(ResponseConverter.buildException(error));
    responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
    // All Mutations in this RegionAction not executed as we can not see the Region online here
    // in this RS. Will be retried from Client. Skipping all the Cells in CellScanner
    // corresponding to these Mutations.
    if (cellScanner != null) {
      skipCellsForMutations(regionAction.getActionList(), cellScanner);
    }
  }

  private boolean isReplicationRequest(Action action) {
    // replication request can only be put or delete.
    if (!action.hasMutation()) {
      return false;
    }
    MutationProto mutation = action.getMutation();
    MutationType type = mutation.getMutateType();
    if (type != MutationType.PUT && type != MutationType.DELETE) {
      return false;
    }
    // replication will set a special attribute so we can make use of it to decide whether a request
    // is for replication.
    return mutation.getAttributeList().stream().map(p -> p.getName())
      .filter(n -> n.equals(ReplicationUtils.REPLICATION_ATTR_NAME)).findAny().isPresent();
  }

  /**
   * Execute multiple actions on a table: get, mutate, and/or execCoprocessor
   * @param rpcc the RPC controller
   * @param request the multi request
   * @throws ServiceException
   */
  @Override
  public MultiResponse multi(final RpcController rpcc, final MultiRequest request)
      throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    checkBatchSizeAndLogLargeSize(request);

    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController)rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner(): null;
    if (controller != null) {
      controller.setCellScanner(null);
    }

    long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;

    MultiResponse.Builder responseBuilder = MultiResponse.newBuilder();
    RegionActionResult.Builder regionActionResultBuilder = RegionActionResult.newBuilder();
    this.rpcMultiRequestCount.increment();
    this.requestCount.increment();
    ActivePolicyEnforcement spaceQuotaEnforcement = getSpaceQuotaManager().getActiveEnforcements();

    // We no longer use MultiRequest#condition. Instead, we use RegionAction#condition. The
    // following logic is for backward compatibility as old clients still use
    // MultiRequest#condition in case of checkAndMutate with RowMutations.
    if (request.hasCondition()) {
      if (request.getRegionActionList().isEmpty()) {
        // If the region action list is empty, do nothing.
        responseBuilder.setProcessed(true);
        return responseBuilder.build();
      }

      RegionAction regionAction = request.getRegionAction(0);

      // When request.hasCondition() is true, regionAction.getAtomic() should be always true. So
      // we can assume regionAction.getAtomic() is true here.
      assert regionAction.getAtomic();

      OperationQuota quota;
      HRegion region;
      RegionSpecifier regionSpecifier = regionAction.getRegion();

      try {
        region = getRegion(regionSpecifier);
        quota = getRpcQuotaManager().checkQuota(region, regionAction.getActionList());
      } catch (IOException e) {
        failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner, e);
        return responseBuilder.build();
      }

      try {
        boolean rejectIfFromClient = shouldRejectRequestsFromClient(region);
        // We only allow replication in standby state and it will not set the atomic flag.
        if (rejectIfFromClient) {
          failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner,
            new DoNotRetryIOException(region.getRegionInfo().getRegionNameAsString()
              + " is in STANDBY state"));
          return responseBuilder.build();
        }

        try {
          CheckAndMutateResult result = checkAndMutate(region, regionAction.getActionList(),
            cellScanner, request.getCondition(), nonceGroup, spaceQuotaEnforcement);
          responseBuilder.setProcessed(result.isSuccess());
          ClientProtos.ResultOrException.Builder resultOrExceptionOrBuilder =
            ClientProtos.ResultOrException.newBuilder();
          for (int i = 0; i < regionAction.getActionCount(); i++) {
            // To unify the response format with doNonAtomicRegionMutation and read through
            // client's AsyncProcess we have to add an empty result instance per operation
            resultOrExceptionOrBuilder.clear();
            resultOrExceptionOrBuilder.setIndex(i);
            regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
          }
        } catch (IOException e) {
          rpcServer.getMetrics().exception(e);
          // As it's an atomic operation with a condition, we may expect it's a global failure.
          regionActionResultBuilder.setException(ResponseConverter.buildException(e));
        }
      } finally {
        quota.close();
      }

      responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
      ClientProtos.RegionLoadStats regionLoadStats = region.getLoadStatistics();
      if (regionLoadStats != null) {
        responseBuilder.setRegionStatistics(MultiRegionLoadStats.newBuilder()
          .addRegion(regionSpecifier).addStat(regionLoadStats).build());
      }
      return responseBuilder.build();
    }

    // this will contain all the cells that we need to return. It's created later, if needed.
    List<CellScannable> cellsToReturn = null;
    RegionScannersCloseCallBack closeCallBack = null;
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    Map<RegionSpecifier, ClientProtos.RegionLoadStats> regionStats = new HashMap<>(request
      .getRegionActionCount());

    for (RegionAction regionAction : request.getRegionActionList()) {
      OperationQuota quota;
      HRegion region;
      RegionSpecifier regionSpecifier = regionAction.getRegion();
      regionActionResultBuilder.clear();

      try {
        region = getRegion(regionSpecifier);
        quota = getRpcQuotaManager().checkQuota(region, regionAction.getActionList());
      } catch (IOException e) {
        failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner, e);
        continue;  // For this region it's a failure.
      }

      try {
        boolean rejectIfFromClient = shouldRejectRequestsFromClient(region);

        if (regionAction.hasCondition()) {
          // We only allow replication in standby state and it will not set the atomic flag.
          if (rejectIfFromClient) {
            failRegionAction(responseBuilder, regionActionResultBuilder, regionAction,
              cellScanner, new DoNotRetryIOException(region.getRegionInfo()
                .getRegionNameAsString() + " is in STANDBY state"));
            continue;
          }

          try {
            ClientProtos.ResultOrException.Builder resultOrExceptionOrBuilder =
              ClientProtos.ResultOrException.newBuilder();
            if (regionAction.getActionCount() == 1) {
              CheckAndMutateResult result = checkAndMutate(region, quota,
                regionAction.getAction(0).getMutation(), cellScanner,
                regionAction.getCondition(), nonceGroup, spaceQuotaEnforcement);
              regionActionResultBuilder.setProcessed(result.isSuccess());
              resultOrExceptionOrBuilder.setIndex(0);
              if (result.getResult() != null) {
                resultOrExceptionOrBuilder.setResult(ProtobufUtil.toResult(result.getResult()));
              }
              regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
            } else {
              CheckAndMutateResult result = checkAndMutate(region, regionAction.getActionList(),
                cellScanner, regionAction.getCondition(), nonceGroup, spaceQuotaEnforcement);
              regionActionResultBuilder.setProcessed(result.isSuccess());
              for (int i = 0; i < regionAction.getActionCount(); i++) {
                if (i == 0 && result.getResult() != null) {
                  // Set the result of the Increment/Append operations to the first element of the
                  // ResultOrException list
                  resultOrExceptionOrBuilder.setIndex(i);
                  regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder
                    .setResult(ProtobufUtil.toResult(result.getResult())).build());
                  continue;
                }
                // To unify the response format with doNonAtomicRegionMutation and read through
                // client's AsyncProcess we have to add an empty result instance per operation
                resultOrExceptionOrBuilder.clear();
                resultOrExceptionOrBuilder.setIndex(i);
                regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
              }
            }
          } catch (IOException e) {
            rpcServer.getMetrics().exception(e);
            // As it's an atomic operation with a condition, we may expect it's a global failure.
            regionActionResultBuilder.setException(ResponseConverter.buildException(e));
          }
        } else if (regionAction.hasAtomic() && regionAction.getAtomic()) {
          // We only allow replication in standby state and it will not set the atomic flag.
          if (rejectIfFromClient) {
            failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner,
              new DoNotRetryIOException(region.getRegionInfo().getRegionNameAsString()
                + " is in STANDBY state"));
            continue;
          }
          try {
            doAtomicBatchOp(regionActionResultBuilder, region, quota, regionAction.getActionList(),
              cellScanner, nonceGroup, spaceQuotaEnforcement);
            regionActionResultBuilder.setProcessed(true);
            // We no longer use MultiResponse#processed. Instead, we use
            // RegionActionResult#processed. This is for backward compatibility for old clients.
            responseBuilder.setProcessed(true);
          } catch (IOException e) {
            rpcServer.getMetrics().exception(e);
            // As it's atomic, we may expect it's a global failure.
            regionActionResultBuilder.setException(ResponseConverter.buildException(e));
          }
        } else {
          if (rejectIfFromClient && regionAction.getActionCount() > 0 && !isReplicationRequest(
            regionAction.getAction(0))) {
            // fail if it is not a replication request
            failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner,
              new DoNotRetryIOException(region.getRegionInfo().getRegionNameAsString()
                + " is in STANDBY state"));
            continue;
          }
          // doNonAtomicRegionMutation manages the exception internally
          if (context != null && closeCallBack == null) {
            // An RpcCallBack that creates a list of scanners that needs to perform callBack
            // operation on completion of multiGets.
            // Set this only once
            closeCallBack = new RegionScannersCloseCallBack();
            context.setCallBack(closeCallBack);
          }
          cellsToReturn = doNonAtomicRegionMutation(region, quota, regionAction, cellScanner,
            regionActionResultBuilder, cellsToReturn, nonceGroup, closeCallBack, context,
            spaceQuotaEnforcement);
        }
      } finally {
        quota.close();
      }

      responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
      ClientProtos.RegionLoadStats regionLoadStats = region.getLoadStatistics();
      if (regionLoadStats != null) {
        regionStats.put(regionSpecifier, regionLoadStats);
      }
    }
    // Load the controller with the Cells to return.
    if (cellsToReturn != null && !cellsToReturn.isEmpty() && controller != null) {
      controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
    }

    MultiRegionLoadStats.Builder builder = MultiRegionLoadStats.newBuilder();
    for(Entry<RegionSpecifier, ClientProtos.RegionLoadStats> stat: regionStats.entrySet()){
      builder.addRegion(stat.getKey());
      builder.addStat(stat.getValue());
    }
    responseBuilder.setRegionStatistics(builder);
    return responseBuilder.build();
  }

  private void skipCellsForMutations(List<Action> actions, CellScanner cellScanner) {
    if (cellScanner == null) {
      return;
    }
    for (Action action : actions) {
      skipCellsForMutation(action, cellScanner);
    }
  }

  private void skipCellsForMutation(Action action, CellScanner cellScanner) {
    if (cellScanner == null) {
      return;
    }
    try {
      if (action.hasMutation()) {
        MutationProto m = action.getMutation();
        if (m.hasAssociatedCellCount()) {
          for (int i = 0; i < m.getAssociatedCellCount(); i++) {
            cellScanner.advance();
          }
        }
      }
    } catch (IOException e) {
      // No need to handle these Individual Muatation level issue. Any way this entire RegionAction
      // marked as failed as we could not see the Region here. At client side the top level
      // RegionAction exception will be considered first.
      LOG.error("Error while skipping Cells in CellScanner for invalid Region Mutations", e);
    }
  }

  /**
   * Mutate data in a table.
   *
   * @param rpcc the RPC controller
   * @param request the mutate request
   */
  @Override
  public MutateResponse mutate(final RpcController rpcc, final MutateRequest request)
      throws ServiceException {
    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController)rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner() : null;
    OperationQuota quota = null;
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    // Clear scanner so we are not holding on to reference across call.
    if (controller != null) {
      controller.setCellScanner(null);
    }
    try {
      checkOpen();
      requestCount.increment();
      rpcMutateRequestCount.increment();
      HRegion region = getRegion(request.getRegion());
      rejectIfInStandByState(region);
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      MutationProto mutation = request.getMutation();
      if (!region.getRegionInfo().isMetaRegion()) {
        server.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;
      quota = getRpcQuotaManager().checkQuota(region, OperationQuota.OperationType.MUTATE);
      ActivePolicyEnforcement spaceQuotaEnforcement = getSpaceQuotaManager()
        .getActiveEnforcements();

      if (request.hasCondition()) {
        CheckAndMutateResult result = checkAndMutate(region, quota, mutation, cellScanner,
          request.getCondition(), nonceGroup, spaceQuotaEnforcement);
        builder.setProcessed(result.isSuccess());
        boolean clientCellBlockSupported = isClientCellBlockSupport(context);
        addResult(builder, result.getResult(), controller, clientCellBlockSupported);
        if (clientCellBlockSupported) {
          addSize(context, result.getResult(), null);
        }
      } else {
        Result r = null;
        Boolean processed = null;
        MutationType type = mutation.getMutateType();
        switch (type) {
          case APPEND:
            // TODO: this doesn't actually check anything.
            r = append(region, quota, mutation, cellScanner, nonceGroup, spaceQuotaEnforcement);
            break;
          case INCREMENT:
            // TODO: this doesn't actually check anything.
            r = increment(region, quota, mutation, cellScanner, nonceGroup, spaceQuotaEnforcement);
            break;
          case PUT:
            put(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
            processed = Boolean.TRUE;
            break;
          case DELETE:
            delete(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
            processed = Boolean.TRUE;
            break;
          default:
            throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
        }
        if (processed != null) {
          builder.setProcessed(processed);
        }
        boolean clientCellBlockSupported = isClientCellBlockSupport(context);
        addResult(builder, r, controller, clientCellBlockSupported);
        if (clientCellBlockSupported) {
          addSize(context, r, null);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      server.checkFileSystem();
      throw new ServiceException(ie);
    } finally {
      if (quota != null) {
        quota.close();
      }
    }
  }

  private void put(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    Put put = ProtobufUtil.toPut(mutation, cellScanner);
    checkCellSizeLimit(region, put);
    spaceQuota.getPolicyEnforcement(region).check(put);
    quota.addMutation(put);
    region.put(put);

    MetricsRegionServer metricsRegionServer = server.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updatePut(region.getRegionInfo().getTable(), after - before);
    }
  }

  private void delete(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    Delete delete = ProtobufUtil.toDelete(mutation, cellScanner);
    checkCellSizeLimit(region, delete);
    spaceQuota.getPolicyEnforcement(region).check(delete);
    quota.addMutation(delete);
    region.delete(delete);

    MetricsRegionServer metricsRegionServer = server.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updateDelete(region.getRegionInfo().getTable(), after - before);
    }
  }

  private CheckAndMutateResult checkAndMutate(HRegion region, OperationQuota quota,
    MutationProto mutation, CellScanner cellScanner, Condition condition, long nonceGroup,
    ActivePolicyEnforcement spaceQuota) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    CheckAndMutate checkAndMutate = ProtobufUtil.toCheckAndMutate(condition, mutation,
      cellScanner);
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    checkCellSizeLimit(region, (Mutation) checkAndMutate.getAction());
    spaceQuota.getPolicyEnforcement(region).check((Mutation) checkAndMutate.getAction());
    quota.addMutation((Mutation) checkAndMutate.getAction());

    CheckAndMutateResult result = null;
    if (region.getCoprocessorHost() != null) {
      result = region.getCoprocessorHost().preCheckAndMutate(checkAndMutate);
    }
    if (result == null) {
      result = region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
      if (region.getCoprocessorHost() != null) {
        result = region.getCoprocessorHost().postCheckAndMutate(checkAndMutate, result);
      }
    }
    MetricsRegionServer metricsRegionServer = server.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updateCheckAndMutate(
        region.getRegionInfo().getTable(), after - before);

      MutationType type = mutation.getMutateType();
      switch (type) {
        case PUT:
          metricsRegionServer.updateCheckAndPut(
            region.getRegionInfo().getTable(), after - before);
          break;
        case DELETE:
          metricsRegionServer.updateCheckAndDelete(
            region.getRegionInfo().getTable(), after - before);
          break;
        default:
          break;
      }
    }
    return result;
  }

  // This is used to keep compatible with the old client implementation. Consider remove it if we
  // decide to drop the support of the client that still sends close request to a region scanner
  // which has already been exhausted.
  @Deprecated
  private static final IOException SCANNER_ALREADY_CLOSED = new IOException() {

    private static final long serialVersionUID = -4305297078988180130L;

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  };

  private RegionScannerHolder getRegionScanner(ScanRequest request) throws IOException {
    String scannerName = toScannerName(request.getScannerId());
    RegionScannerHolder rsh = this.scanners.get(scannerName);
    if (rsh == null) {
      // just ignore the next or close request if scanner does not exists.
      if (closedScanners.getIfPresent(scannerName) != null) {
        throw SCANNER_ALREADY_CLOSED;
      } else {
        LOG.warn("Client tried to access missing scanner " + scannerName);
        throw new UnknownScannerException(
            "Unknown scanner '" + scannerName + "'. This can happen due to any of the following " +
                "reasons: a) Scanner id given is wrong, b) Scanner lease expired because of " +
                "long wait between consecutive client checkins, c) Server may be closing down, " +
                "d) RegionServer restart during upgrade.\nIf the issue is due to reason (b), a " +
                "possible fix would be increasing the value of" +
                "'hbase.client.scanner.timeout.period' configuration.");
      }
    }
    rejectIfInStandByState(rsh.r);
    RegionInfo hri = rsh.s.getRegionInfo();
    // Yes, should be the same instance
    if (server.getOnlineRegion(hri.getRegionName()) != rsh.r) {
      String msg = "Region has changed on the scanner " + scannerName + ": regionName="
          + hri.getRegionNameAsString() + ", scannerRegionName=" + rsh.r;
      LOG.warn(msg + ", closing...");
      scanners.remove(scannerName);
      try {
        rsh.s.close();
      } catch (IOException e) {
        LOG.warn("Getting exception closing " + scannerName, e);
      } finally {
        try {
          server.getLeaseManager().cancelLease(scannerName);
        } catch (LeaseException e) {
          LOG.warn("Getting exception closing " + scannerName, e);
        }
      }
      throw new NotServingRegionException(msg);
    }
    return rsh;
  }

  /**
   * @return Pair with scannerName key to use with this new Scanner and its RegionScannerHolder
   *    value.
   */
  private Pair<String, RegionScannerHolder> newRegionScanner(ScanRequest request,
      ScanResponse.Builder builder) throws IOException {
    HRegion region = getRegion(request.getRegion());
    rejectIfInStandByState(region);
    ClientProtos.Scan protoScan = request.getScan();
    boolean isLoadingCfsOnDemandSet = protoScan.hasLoadColumnFamiliesOnDemand();
    Scan scan = ProtobufUtil.toScan(protoScan);
    // if the request doesn't set this, get the default region setting.
    if (!isLoadingCfsOnDemandSet) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }

    if (!scan.hasFamilies()) {
      // Adding all families to scanner
      for (byte[] family : region.getTableDescriptor().getColumnFamilyNames()) {
        scan.addFamily(family);
      }
    }
    if (region.getCoprocessorHost() != null) {
      // preScannerOpen is not allowed to return a RegionScanner. Only post hook can create a
      // wrapper for the core created RegionScanner
      region.getCoprocessorHost().preScannerOpen(scan);
    }
    RegionScannerImpl coreScanner = region.getScanner(scan);
    Shipper shipper = coreScanner;
    RegionScanner scanner = coreScanner;
    try {
      if (region.getCoprocessorHost() != null) {
        scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
      }
    } catch (Exception e) {
      // Although region coprocessor is for advanced users and they should take care of the
      // implementation to not damage the HBase system, closing the scanner on exception here does
      // not have any bad side effect, so let's do it
      scanner.close();
      throw e;
    }
    long scannerId = scannerIdGenerator.generateNewScannerId();
    builder.setScannerId(scannerId);
    builder.setMvccReadPoint(scanner.getMvccReadPoint());
    builder.setTtl(scannerLeaseTimeoutPeriod);
    String scannerName = toScannerName(scannerId);

    boolean fullRegionScan = !region.getRegionInfo().getTable().isSystemTable() &&
      isFullRegionScan(scan, region);

    return new Pair<String, RegionScannerHolder>(scannerName,
      addScanner(scannerName, scanner, shipper, region, scan.isNeedCursorResult(),
      fullRegionScan));
  }

  /**
   * The returned String is used as key doing look up of outstanding Scanners in this Servers'
   * this.scanners, the Map of outstanding scanners and their current state.
   * @param scannerId A scanner long id.
   * @return The long id as a String.
   */
  private static String toScannerName(long scannerId) {
    return Long.toString(scannerId);
  }

  private void checkScanNextCallSeq(ScanRequest request, RegionScannerHolder rsh)
      throws OutOfOrderScannerNextException {
    // if nextCallSeq does not match throw Exception straight away. This needs to be
    // performed even before checking of Lease.
    // See HBASE-5974
    if (request.hasNextCallSeq()) {
      long callSeq = request.getNextCallSeq();
      if (!rsh.incNextCallSeq(callSeq)) {
        throw new OutOfOrderScannerNextException("Expected nextCallSeq: " + rsh.getNextCallSeq()
            + " But the nextCallSeq got from client: " + request.getNextCallSeq() + "; request="
            + TextFormat.shortDebugString(request));
      }
    }
  }

  private void addScannerLeaseBack(LeaseManager.Lease lease) {
    try {
      server.getLeaseManager().addLease(lease);
    } catch (LeaseStillHeldException e) {
      // should not happen as the scanner id is unique.
      throw new AssertionError(e);
    }
  }

  private long getTimeLimit(HBaseRpcController controller, boolean allowHeartbeatMessages) {
    // Set the time limit to be half of the more restrictive timeout value (one of the
    // timeout values must be positive). In the event that both values are positive, the
    // more restrictive of the two is used to calculate the limit.
    if (allowHeartbeatMessages && (scannerLeaseTimeoutPeriod > 0 || rpcTimeout > 0)) {
      long timeLimitDelta;
      if (scannerLeaseTimeoutPeriod > 0 && rpcTimeout > 0) {
        timeLimitDelta = Math.min(scannerLeaseTimeoutPeriod, rpcTimeout);
      } else {
        timeLimitDelta = scannerLeaseTimeoutPeriod > 0 ? scannerLeaseTimeoutPeriod : rpcTimeout;
      }
      if (controller != null && controller.getCallTimeout() > 0) {
        timeLimitDelta = Math.min(timeLimitDelta, controller.getCallTimeout());
      }
      // Use half of whichever timeout value was more restrictive... But don't allow
      // the time limit to be less than the allowable minimum (could cause an
      // immediatate timeout before scanning any data).
      timeLimitDelta = Math.max(timeLimitDelta / 2, minimumScanTimeLimitDelta);
      // XXX: Can not use EnvironmentEdge here because TestIncrementTimeRange use a
      // ManualEnvironmentEdge. Consider using System.nanoTime instead.
      return EnvironmentEdgeManager.currentTime() + timeLimitDelta;
    }
    // Default value of timeLimit is negative to indicate no timeLimit should be
    // enforced.
    return -1L;
  }

  private void checkLimitOfRows(int numOfCompleteRows, int limitOfRows, boolean moreRows,
      ScannerContext scannerContext, ScanResponse.Builder builder) {
    if (numOfCompleteRows >= limitOfRows) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Done scanning, limit of rows reached, moreRows: " + moreRows +
            " scannerContext: " + scannerContext);
      }
      builder.setMoreResults(false);
    }
  }

  // return whether we have more results in region.
  private void scan(HBaseRpcController controller, ScanRequest request, RegionScannerHolder rsh,
      long maxQuotaResultSize, int maxResults, int limitOfRows, List<Result> results,
      ScanResponse.Builder builder, MutableObject<Object> lastBlock, RpcCallContext context)
      throws IOException {
    HRegion region = rsh.r;
    RegionScanner scanner = rsh.s;
    long maxResultSize;
    if (scanner.getMaxResultSize() > 0) {
      maxResultSize = Math.min(scanner.getMaxResultSize(), maxQuotaResultSize);
    } else {
      maxResultSize = maxQuotaResultSize;
    }
    // This is cells inside a row. Default size is 10 so if many versions or many cfs,
    // then we'll resize. Resizings show in profiler. Set it higher than 10. For now
    // arbitrary 32. TODO: keep record of general size of results being returned.
    ArrayList<Cell> values = new ArrayList<>(32);
    region.startRegionOperation(Operation.SCAN);
    long before = EnvironmentEdgeManager.currentTime();
    // Used to check if we've matched the row limit set on the Scan
    int numOfCompleteRows = 0;
    // Count of times we call nextRaw; can be > numOfCompleteRows.
    int numOfNextRawCalls = 0;
    try {
      int numOfResults = 0;
      synchronized (scanner) {
        boolean stale = (region.getRegionInfo().getReplicaId() != 0);
        boolean clientHandlesPartials =
            request.hasClientHandlesPartials() && request.getClientHandlesPartials();
        boolean clientHandlesHeartbeats =
            request.hasClientHandlesHeartbeats() && request.getClientHandlesHeartbeats();

        // On the server side we must ensure that the correct ordering of partial results is
        // returned to the client to allow them to properly reconstruct the partial results.
        // If the coprocessor host is adding to the result list, we cannot guarantee the
        // correct ordering of partial results and so we prevent partial results from being
        // formed.
        boolean serverGuaranteesOrderOfPartials = results.isEmpty();
        boolean allowPartialResults = clientHandlesPartials && serverGuaranteesOrderOfPartials;
        boolean moreRows = false;

        // Heartbeat messages occur when the processing of the ScanRequest is exceeds a
        // certain time threshold on the server. When the time threshold is exceeded, the
        // server stops the scan and sends back whatever Results it has accumulated within
        // that time period (may be empty). Since heartbeat messages have the potential to
        // create partial Results (in the event that the timeout occurs in the middle of a
        // row), we must only generate heartbeat messages when the client can handle both
        // heartbeats AND partials
        boolean allowHeartbeatMessages = clientHandlesHeartbeats && allowPartialResults;

        long timeLimit = getTimeLimit(controller, allowHeartbeatMessages);

        final LimitScope sizeScope =
            allowPartialResults ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;
        final LimitScope timeScope =
            allowHeartbeatMessages ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;

        boolean trackMetrics = request.hasTrackScanMetrics() && request.getTrackScanMetrics();

        // Configure with limits for this RPC. Set keep progress true since size progress
        // towards size limit should be kept between calls to nextRaw
        ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
        // maxResultSize - either we can reach this much size for all cells(being read) data or sum
        // of heap size occupied by cells(being read). Cell data means its key and value parts.
        contextBuilder.setSizeLimit(sizeScope, maxResultSize, maxResultSize);
        contextBuilder.setBatchLimit(scanner.getBatch());
        contextBuilder.setTimeLimit(timeScope, timeLimit);
        contextBuilder.setTrackMetrics(trackMetrics);
        ScannerContext scannerContext = contextBuilder.build();
        boolean limitReached = false;
        while (numOfResults < maxResults) {
          // Reset the batch progress to 0 before every call to RegionScanner#nextRaw. The
          // batch limit is a limit on the number of cells per Result. Thus, if progress is
          // being tracked (i.e. scannerContext.keepProgress() is true) then we need to
          // reset the batch progress between nextRaw invocations since we don't want the
          // batch progress from previous calls to affect future calls
          scannerContext.setBatchProgress(0);
          assert values.isEmpty();

          // Collect values to be returned here
          moreRows = scanner.nextRaw(values, scannerContext);
          if (context == null) {
            // When there is no RpcCallContext,copy EC to heap, then the scanner would close,
            // This can be an EXPENSIVE call. It may make an extra copy from offheap to onheap
            // buffers.See more details in HBASE-26036.
            CellUtil.cloneIfNecessary(values);
          }
          numOfNextRawCalls++;

          if (!values.isEmpty()) {
            if (limitOfRows > 0) {
              // First we need to check if the last result is partial and we have a row change. If
              // so then we need to increase the numOfCompleteRows.
              if (results.isEmpty()) {
                if (rsh.rowOfLastPartialResult != null &&
                    !CellUtil.matchingRows(values.get(0), rsh.rowOfLastPartialResult)) {
                  numOfCompleteRows++;
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              } else {
                Result lastResult = results.get(results.size() - 1);
                if (lastResult.mayHaveMoreCellsInRow() &&
                    !CellUtil.matchingRows(values.get(0), lastResult.getRow())) {
                  numOfCompleteRows++;
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              }
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
                break;
              }
            }
            boolean mayHaveMoreCellsInRow = scannerContext.mayHaveMoreCellsInRow();
            Result r = Result.create(values, null, stale, mayHaveMoreCellsInRow);
            lastBlock.setValue(addSize(context, r, lastBlock.getValue()));
            results.add(r);
            numOfResults++;
            if (!mayHaveMoreCellsInRow && limitOfRows > 0) {
              numOfCompleteRows++;
              checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder);
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
                break;
              }
            }
          } else if (!moreRows && !results.isEmpty()) {
            // No more cells for the scan here, we need to ensure that the mayHaveMoreCellsInRow of
            // last result is false. Otherwise it's possible that: the first nextRaw returned
            // because BATCH_LIMIT_REACHED (BTW it happen to exhaust all cells of the scan),so the
            // last result's mayHaveMoreCellsInRow will be true. while the following nextRaw will
            // return with moreRows=false, which means moreResultsInRegion would be false, it will
            // be a contradictory state (HBASE-21206).
            int lastIdx = results.size() - 1;
            Result r = results.get(lastIdx);
            if (r.mayHaveMoreCellsInRow()) {
              results.set(lastIdx, Result.create(r.rawCells(), r.getExists(), r.isStale(), false));
            }
          }
          boolean sizeLimitReached = scannerContext.checkSizeLimit(LimitScope.BETWEEN_ROWS);
          boolean timeLimitReached = scannerContext.checkTimeLimit(LimitScope.BETWEEN_ROWS);
          boolean resultsLimitReached = numOfResults >= maxResults;
          limitReached = sizeLimitReached || timeLimitReached || resultsLimitReached;

          if (limitReached || !moreRows) {
            // We only want to mark a ScanResponse as a heartbeat message in the event that
            // there are more values to be read server side. If there aren't more values,
            // marking it as a heartbeat is wasteful because the client will need to issue
            // another ScanRequest only to realize that they already have all the values
            if (moreRows && timeLimitReached) {
              // Heartbeat messages occur when the time limit has been reached.
              builder.setHeartbeatMessage(true);
              if (rsh.needCursor) {
                Cell cursorCell = scannerContext.getLastPeekedCell();
                if (cursorCell != null) {
                  builder.setCursor(ProtobufUtil.toCursor(cursorCell));
                }
              }
            }
            break;
          }
          values.clear();
        }
        builder.setMoreResultsInRegion(moreRows);
        // Check to see if the client requested that we track metrics server side. If the
        // client requested metrics, retrieve the metrics from the scanner context.
        if (trackMetrics) {
          Map<String, Long> metrics = scannerContext.getMetrics().getMetricsMap();
          ScanMetrics.Builder metricBuilder = ScanMetrics.newBuilder();
          NameInt64Pair.Builder pairBuilder = NameInt64Pair.newBuilder();

          for (Entry<String, Long> entry : metrics.entrySet()) {
            pairBuilder.setName(entry.getKey());
            pairBuilder.setValue(entry.getValue());
            metricBuilder.addMetrics(pairBuilder.build());
          }

          builder.setScanMetrics(metricBuilder.build());
        }
      }
    } finally {
      region.closeRegionOperation();
      // Update serverside metrics, even on error.
      long end = EnvironmentEdgeManager.currentTime();
      long responseCellSize = context != null ? context.getResponseCellSize() : 0;
      region.getMetrics().updateScanTime(end - before);
      final MetricsRegionServer metricsRegionServer = server.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateScanSize(
          region.getTableDescriptor().getTableName(), responseCellSize);
        metricsRegionServer.updateScanTime(
          region.getTableDescriptor().getTableName(), end - before);
        metricsRegionServer.updateReadQueryMeter(region.getRegionInfo().getTable(),
          numOfNextRawCalls);
      }
    }
    // coprocessor postNext hook
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postScannerNext(scanner, results, maxResults, true);
    }
  }

  /**
   * Scan data in a table.
   *
   * @param controller the RPC controller
   * @param request the scan request
   * @throws ServiceException
   */
  @Override
  public ScanResponse scan(final RpcController controller, final ScanRequest request)
      throws ServiceException {
    if (controller != null && !(controller instanceof HBaseRpcController)) {
      throw new UnsupportedOperationException(
          "We only do " + "HBaseRpcControllers! FIX IF A PROBLEM: " + controller);
    }
    if (!request.hasScannerId() && !request.hasScan()) {
      throw new ServiceException(
          new DoNotRetryIOException("Missing required input: scannerId or scan"));
    }
    try {
      checkOpen();
    } catch (IOException e) {
      if (request.hasScannerId()) {
        String scannerName = toScannerName(request.getScannerId());
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Server shutting down and client tried to access missing scanner " + scannerName);
        }
        final LeaseManager leaseManager = server.getLeaseManager();
        if (leaseManager != null) {
          try {
            leaseManager.cancelLease(scannerName);
          } catch (LeaseException le) {
            // No problem, ignore
            if (LOG.isTraceEnabled()) {
              LOG.trace("Un-able to cancel lease of scanner. It could already be closed.");
            }
          }
        }
      }
      throw new ServiceException(e);
    }
    requestCount.increment();
    rpcScanRequestCount.increment();
    RegionScannerHolder rsh;
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    String scannerName;
    try {
      if (request.hasScannerId()) {
        // The downstream projects such as AsyncHBase in OpenTSDB need this value. See HBASE-18000
        // for more details.
        long scannerId = request.getScannerId();
        builder.setScannerId(scannerId);
        scannerName = toScannerName(scannerId);
        rsh = getRegionScanner(request);
      } else {
        Pair<String, RegionScannerHolder> scannerNameAndRSH = newRegionScanner(request, builder);
        scannerName = scannerNameAndRSH.getFirst();
        rsh = scannerNameAndRSH.getSecond();
      }
    } catch (IOException e) {
      if (e == SCANNER_ALREADY_CLOSED) {
        // Now we will close scanner automatically if there are no more results for this region but
        // the old client will still send a close request to us. Just ignore it and return.
        return builder.build();
      }
      throw new ServiceException(e);
    }
    if (rsh.fullRegionScan) {
      rpcFullScanRequestCount.increment();
    }
    HRegion region = rsh.r;
    LeaseManager.Lease lease;
    try {
      // Remove lease while its being processed in server; protects against case
      // where processing of request takes > lease expiration time. or null if none found.
      lease = server.getLeaseManager().removeLease(scannerName);
    } catch (LeaseException e) {
      throw new ServiceException(e);
    }
    if (request.hasRenew() && request.getRenew()) {
      // add back and return
      addScannerLeaseBack(lease);
      try {
        checkScanNextCallSeq(request, rsh);
      } catch (OutOfOrderScannerNextException e) {
        throw new ServiceException(e);
      }
      return builder.build();
    }
    OperationQuota quota;
    try {
      quota = getRpcQuotaManager().checkQuota(region, OperationQuota.OperationType.SCAN);
    } catch (IOException e) {
      addScannerLeaseBack(lease);
      throw new ServiceException(e);
    }
    try {
      checkScanNextCallSeq(request, rsh);
    } catch (OutOfOrderScannerNextException e) {
      addScannerLeaseBack(lease);
      throw new ServiceException(e);
    }
    // Now we have increased the next call sequence. If we give client an error, the retry will
    // never success. So we'd better close the scanner and return a DoNotRetryIOException to client
    // and then client will try to open a new scanner.
    boolean closeScanner = request.hasCloseScanner() ? request.getCloseScanner() : false;
    int rows; // this is scan.getCaching
    if (request.hasNumberOfRows()) {
      rows = request.getNumberOfRows();
    } else {
      rows = closeScanner ? 0 : 1;
    }
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    // now let's do the real scan.
    long maxQuotaResultSize = Math.min(maxScannerResultSize, quota.getReadAvailable());
    RegionScanner scanner = rsh.s;
    // this is the limit of rows for this scan, if we the number of rows reach this value, we will
    // close the scanner.
    int limitOfRows;
    if (request.hasLimitOfRows()) {
      limitOfRows = request.getLimitOfRows();
    } else {
      limitOfRows = -1;
    }
    MutableObject<Object> lastBlock = new MutableObject<>();
    boolean scannerClosed = false;
    try {
      List<Result> results = new ArrayList<>(Math.min(rows, 512));
      if (rows > 0) {
        boolean done = false;
        // Call coprocessor. Get region info from scanner.
        if (region.getCoprocessorHost() != null) {
          Boolean bypass = region.getCoprocessorHost().preScannerNext(scanner, results, rows);
          if (!results.isEmpty()) {
            for (Result r : results) {
              lastBlock.setValue(addSize(context, r, lastBlock.getValue()));
            }
          }
          if (bypass != null && bypass.booleanValue()) {
            done = true;
          }
        }
        if (!done) {
          scan((HBaseRpcController) controller, request, rsh, maxQuotaResultSize, rows, limitOfRows,
            results, builder, lastBlock, context);
        } else {
          builder.setMoreResultsInRegion(!results.isEmpty());
        }
      } else {
        // This is a open scanner call with numberOfRow = 0, so set more results in region to true.
        builder.setMoreResultsInRegion(true);
      }

      quota.addScanResult(results);
      addResults(builder, results, (HBaseRpcController) controller,
        RegionReplicaUtil.isDefaultReplica(region.getRegionInfo()),
        isClientCellBlockSupport(context));
      if (scanner.isFilterDone() && results.isEmpty()) {
        // If the scanner's filter - if any - is done with the scan
        // only set moreResults to false if the results is empty. This is used to keep compatible
        // with the old scan implementation where we just ignore the returned results if moreResults
        // is false. Can remove the isEmpty check after we get rid of the old implementation.
        builder.setMoreResults(false);
      }
      // Later we may close the scanner depending on this flag so here we need to make sure that we
      // have already set this flag.
      assert builder.hasMoreResultsInRegion();
      // we only set moreResults to false in the above code, so set it to true if we haven't set it
      // yet.
      if (!builder.hasMoreResults()) {
        builder.setMoreResults(true);
      }
      if (builder.getMoreResults() && builder.getMoreResultsInRegion() && !results.isEmpty()) {
        // Record the last cell of the last result if it is a partial result
        // We need this to calculate the complete rows we have returned to client as the
        // mayHaveMoreCellsInRow is true does not mean that there will be extra cells for the
        // current row. We may filter out all the remaining cells for the current row and just
        // return the cells of the nextRow when calling RegionScanner.nextRaw. So here we need to
        // check for row change.
        Result lastResult = results.get(results.size() - 1);
        if (lastResult.mayHaveMoreCellsInRow()) {
          rsh.rowOfLastPartialResult = lastResult.getRow();
        } else {
          rsh.rowOfLastPartialResult = null;
        }
      }
      if (!builder.getMoreResults() || !builder.getMoreResultsInRegion() || closeScanner) {
        scannerClosed = true;
        closeScanner(region, scanner, scannerName, context);
      }
      return builder.build();
    } catch (IOException e) {
      try {
        // scanner is closed here
        scannerClosed = true;
        // The scanner state might be left in a dirty state, so we will tell the Client to
        // fail this RPC and close the scanner while opening up another one from the start of
        // row that the client has last seen.
        closeScanner(region, scanner, scannerName, context);

        // If it is a DoNotRetryIOException already, throw as it is. Unfortunately, DNRIOE is
        // used in two different semantics.
        // (1) The first is to close the client scanner and bubble up the exception all the way
        // to the application. This is preferred when the exception is really un-recoverable
        // (like CorruptHFileException, etc). Plain DoNotRetryIOException also falls into this
        // bucket usually.
        // (2) Second semantics is to close the current region scanner only, but continue the
        // client scanner by overriding the exception. This is usually UnknownScannerException,
        // OutOfOrderScannerNextException, etc where the region scanner has to be closed, but the
        // application-level ClientScanner has to continue without bubbling up the exception to
        // the client. See ClientScanner code to see how it deals with these special exceptions.
        if (e instanceof DoNotRetryIOException) {
          throw e;
        }

        // If it is a FileNotFoundException, wrap as a
        // DoNotRetryIOException. This can avoid the retry in ClientScanner.
        if (e instanceof FileNotFoundException) {
          throw new DoNotRetryIOException(e);
        }

        // We closed the scanner already. Instead of throwing the IOException, and client
        // retrying with the same scannerId only to get USE on the next RPC, we directly throw
        // a special exception to save an RPC.
        if (VersionInfoUtil.hasMinimumVersion(context.getClientVersionInfo(), 1, 4)) {
          // 1.4.0+ clients know how to handle
          throw new ScannerResetException("Scanner is closed on the server-side", e);
        } else {
          // older clients do not know about SRE. Just throw USE, which they will handle
          throw new UnknownScannerException("Throwing UnknownScannerException to reset the client"
              + " scanner state for clients older than 1.3.", e);
        }
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    } finally {
      if (!scannerClosed) {
        // Adding resets expiration time on lease.
        // the closeCallBack will be set in closeScanner so here we only care about shippedCallback
        if (context != null) {
          context.setCallBack(rsh.shippedCallback);
        } else {
          // If context is null,here we call rsh.shippedCallback directly to reuse the logic in
          // rsh.shippedCallback to release the internal resources in rsh,and lease is also added
          // back to regionserver's LeaseManager in rsh.shippedCallback.
          runShippedCallback(rsh);
        }
      }
      quota.close();
    }
  }

  private void runShippedCallback(RegionScannerHolder rsh) throws ServiceException {
    assert rsh.shippedCallback != null;
    try {
      rsh.shippedCallback.run();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private void closeScanner(HRegion region, RegionScanner scanner, String scannerName,
      RpcCallContext context) throws IOException {
    if (region.getCoprocessorHost() != null) {
      if (region.getCoprocessorHost().preScannerClose(scanner)) {
        // bypass the actual close.
        return;
      }
    }
    RegionScannerHolder rsh = scanners.remove(scannerName);
    if (rsh != null) {
      if (context != null) {
        context.setCallBack(rsh.closeCallBack);
      } else {
        rsh.s.close();
      }
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postScannerClose(scanner);
      }
      closedScanners.put(scannerName, scannerName);
    }
  }

  @Override
  public CoprocessorServiceResponse execRegionServerService(RpcController controller,
      CoprocessorServiceRequest request) throws ServiceException {
    rpcPreCheck("execRegionServerService");
    return server.execRegionServerService(controller, request);
  }

  @Override
  public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
      RpcController controller, GetSpaceQuotaSnapshotsRequest request) throws ServiceException {
    try {
      final RegionServerSpaceQuotaManager manager =
          server.getRegionServerSpaceQuotaManager();
      final GetSpaceQuotaSnapshotsResponse.Builder builder =
          GetSpaceQuotaSnapshotsResponse.newBuilder();
      if (manager != null) {
        final Map<TableName,SpaceQuotaSnapshot> snapshots = manager.copyQuotaSnapshots();
        for (Entry<TableName,SpaceQuotaSnapshot> snapshot : snapshots.entrySet()) {
          builder.addSnapshots(TableQuotaSnapshot.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName(snapshot.getKey()))
              .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(snapshot.getValue()))
              .build());
        }
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
      ClearRegionBlockCacheRequest request) throws ServiceException {
    rpcPreCheck("clearRegionBlockCache");
    ClearRegionBlockCacheResponse.Builder builder =
        ClearRegionBlockCacheResponse.newBuilder();
    CacheEvictionStatsBuilder stats = CacheEvictionStats.builder();
    List<HRegion> regions = getRegions(request.getRegionList(), stats);
    for (HRegion region : regions) {
      try {
        stats = stats.append(this.server.clearRegionBlockCache(region));
      } catch (Exception e) {
        stats.addException(region.getRegionInfo().getRegionName(), e);
      }
    }
    stats.withMaxCacheSize(server.getBlockCache().map(BlockCache::getMaxSize).orElse(0L));
    return builder.setStats(ProtobufUtil.toCacheEvictionStats(stats.build())).build();
  }

  private void executeOpenRegionProcedures(OpenRegionRequest request,
      Map<TableName, TableDescriptor> tdCache) {
    long masterSystemTime = request.hasMasterSystemTime() ? request.getMasterSystemTime() : -1;
    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      RegionInfo regionInfo = ProtobufUtil.toRegionInfo(regionOpenInfo.getRegion());
      TableName tableName = regionInfo.getTable();
      TableDescriptor tableDesc = tdCache.get(tableName);
      if (tableDesc == null) {
        try {
          tableDesc = server.getTableDescriptors().get(regionInfo.getTable());
        } catch (IOException e) {
          // Here we do not fail the whole method since we also need deal with other
          // procedures, and we can not ignore this one, so we still schedule a
          // AssignRegionHandler and it will report back to master if we still can not get the
          // TableDescriptor.
          LOG.warn("Failed to get TableDescriptor of {}, will try again in the handler",
            regionInfo.getTable(), e);
        }
        if(tableDesc != null) {
          tdCache.put(tableName, tableDesc);
        }
      }
      if (regionOpenInfo.getFavoredNodesCount() > 0) {
        server.updateRegionFavoredNodesMapping(regionInfo.getEncodedName(),
          regionOpenInfo.getFavoredNodesList());
      }
      long procId = regionOpenInfo.getOpenProcId();
      if (server.submitRegionProcedure(procId)) {
        server.getExecutorService().submit(AssignRegionHandler
            .create(server, regionInfo, procId, tableDesc,
                masterSystemTime));
      }
    }
  }

  private void executeCloseRegionProcedures(CloseRegionRequest request) {
    String encodedName;
    try {
      encodedName = ProtobufUtil.getRegionEncodedName(request.getRegion());
    } catch (DoNotRetryIOException e) {
      throw new UncheckedIOException("Should not happen", e);
    }
    ServerName destination = request.hasDestinationServer() ?
        ProtobufUtil.toServerName(request.getDestinationServer()) :
        null;
    long procId = request.getCloseProcId();
    if (server.submitRegionProcedure(procId)) {
      server.getExecutorService().submit(UnassignRegionHandler
          .create(server, encodedName, procId, false, destination));
    }
  }

  private void executeProcedures(RemoteProcedureRequest request) {
    RSProcedureCallable callable;
    try {
      callable = Class.forName(request.getProcClass()).asSubclass(RSProcedureCallable.class)
        .getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Failed to instantiating remote procedure {}, pid={}", request.getProcClass(),
        request.getProcId(), e);
      server.remoteProcedureComplete(request.getProcId(), e);
      return;
    }
    callable.init(request.getProcData().toByteArray(), server);
    LOG.debug("Executing remote procedure {}, pid={}", callable.getClass(), request.getProcId());
    server.executeProcedure(request.getProcId(), callable);
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public ExecuteProceduresResponse executeProcedures(RpcController controller,
      ExecuteProceduresRequest request) throws ServiceException {
    try {
      checkOpen();
      throwOnWrongStartCode(request);
      server.getRegionServerCoprocessorHost().preExecuteProcedures();
      if (request.getOpenRegionCount() > 0) {
        // Avoid reading from the TableDescritor every time(usually it will read from the file
        // system)
        Map<TableName, TableDescriptor> tdCache = new HashMap<>();
        request.getOpenRegionList().forEach(req -> executeOpenRegionProcedures(req, tdCache));
      }
      if (request.getCloseRegionCount() > 0) {
        request.getCloseRegionList().forEach(this::executeCloseRegionProcedures);
      }
      if (request.getProcCount() > 0) {
        request.getProcList().forEach(this::executeProcedures);
      }
      server.getRegionServerCoprocessorHost().postExecuteProcedures();
      return ExecuteProceduresResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetAllBootstrapNodesResponse getAllBootstrapNodes(RpcController controller,
    GetAllBootstrapNodesRequest request) throws ServiceException {
    GetAllBootstrapNodesResponse.Builder builder = GetAllBootstrapNodesResponse.newBuilder();
    server.getBootstrapNodes()
      .forEachRemaining(server -> builder.addNode(ProtobufUtil.toServerName(server)));
    return builder.build();
  }
}
