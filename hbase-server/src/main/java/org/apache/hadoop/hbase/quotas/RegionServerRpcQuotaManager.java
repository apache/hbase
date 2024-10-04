/*
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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Region Server Quota Manager. It is responsible to provide access to the quota information of each
 * user/table. The direct user of this class is the RegionServer that will get and check the
 * user/table quota for each operation (put, get, scan). For system tables and user/table with a
 * quota specified, the quota check will be a noop.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerRpcQuotaManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerRpcQuotaManager.class);

  private final RegionServerServices rsServices;

  private QuotaCache quotaCache = null;
  private volatile boolean rpcThrottleEnabled;
  // Storage for quota rpc throttle
  private RpcThrottleStorage rpcThrottleStorage;

  public RegionServerRpcQuotaManager(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    rpcThrottleStorage =
      new RpcThrottleStorage(rsServices.getZooKeeper(), rsServices.getConfiguration());
  }

  public void start(final RpcScheduler rpcScheduler) throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    LOG.info("Initializing RPC quota support");

    // Initialize quota cache
    quotaCache = new QuotaCache(rsServices);
    quotaCache.start();
    rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
    LOG.info("Start rpc quota manager and rpc throttle enabled is {}", rpcThrottleEnabled);
  }

  public void stop() {
    if (isQuotaEnabled()) {
      quotaCache.stop("shutdown");
    }
  }

  protected boolean isRpcThrottleEnabled() {
    return rpcThrottleEnabled;
  }

  private boolean isQuotaEnabled() {
    return quotaCache != null;
  }

  public void switchRpcThrottle(boolean enable) throws IOException {
    if (isQuotaEnabled()) {
      if (rpcThrottleEnabled != enable) {
        boolean previousEnabled = rpcThrottleEnabled;
        rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
        LOG.info("Switch rpc throttle from {} to {}", previousEnabled, rpcThrottleEnabled);
      } else {
        LOG.warn(
          "Skip switch rpc throttle because previous value {} is the same as current value {}",
          rpcThrottleEnabled, enable);
      }
    } else {
      LOG.warn("Skip switch rpc throttle to {} because rpc quota is disabled", enable);
    }
  }

  QuotaCache getQuotaCache() {
    return quotaCache;
  }

  /**
   * Returns the quota for an operation.
   * @param ugi   the user that is executing the operation
   * @param table the table where the operation will be executed
   * @return the OperationQuota
   */
  public OperationQuota getQuota(final UserGroupInformation ugi, final TableName table,
    final int blockSizeBytes) {
    if (isQuotaEnabled() && !table.isSystemTable() && isRpcThrottleEnabled()) {
      UserQuotaState userQuotaState = quotaCache.getUserQuotaState(ugi);
      QuotaLimiter userLimiter = userQuotaState.getTableLimiter(table);
      boolean useNoop = userLimiter.isBypass();
      if (userQuotaState.hasBypassGlobals()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" + userLimiter);
        }
        if (!useNoop) {
          return new DefaultOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
            userLimiter);
        }
      } else {
        QuotaLimiter nsLimiter = quotaCache.getNamespaceLimiter(table.getNamespaceAsString());
        QuotaLimiter tableLimiter = quotaCache.getTableLimiter(table);
        QuotaLimiter rsLimiter =
          quotaCache.getRegionServerQuotaLimiter(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY);
        useNoop &= tableLimiter.isBypass() && nsLimiter.isBypass() && rsLimiter.isBypass();
        boolean exceedThrottleQuotaEnabled = quotaCache.isExceedThrottleQuotaEnabled();
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" + userLimiter
            + " tableLimiter=" + tableLimiter + " nsLimiter=" + nsLimiter + " rsLimiter="
            + rsLimiter + " exceedThrottleQuotaEnabled=" + exceedThrottleQuotaEnabled);
        }
        if (!useNoop) {
          if (exceedThrottleQuotaEnabled) {
            return new ExceedOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
              rsLimiter, userLimiter, tableLimiter, nsLimiter);
          } else {
            return new DefaultOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
              userLimiter, tableLimiter, nsLimiter, rsLimiter);
          }
        }
      }
    }
    return NoopOperationQuota.get();
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method is specific to scans
   * because estimating a scan's workload is more complicated than estimating the workload of a
   * get/put.
   * @param region                          the region where the operation will be performed
   * @param scanRequest                     the scan to be estimated against the quota
   * @param maxScannerResultSize            the maximum bytes to be returned by the scanner
   * @param maxBlockBytesScanned            the maximum bytes scanned in a single RPC call by the
   *                                        scanner
   * @param prevBlockBytesScannedDifference the difference between BBS of the previous two next
   *                                        calls
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkScanQuota(final Region region,
    final ClientProtos.ScanRequest scanRequest, long maxScannerResultSize,
    long maxBlockBytesScanned, long prevBlockBytesScannedDifference)
    throws IOException, RpcThrottlingException {
    Optional<User> user = RpcServer.getRequestUser();
    UserGroupInformation ugi;
    if (user.isPresent()) {
      ugi = user.get().getUGI();
    } else {
      ugi = User.getCurrent().getUGI();
    }
    TableDescriptor tableDescriptor = region.getTableDescriptor();
    TableName table = tableDescriptor.getTableName();

    OperationQuota quota = getQuota(ugi, table, region.getMinBlockSizeBytes());
    try {
      quota.checkScanQuota(scanRequest, maxScannerResultSize, maxBlockBytesScanned,
        prevBlockBytesScannedDifference);
    } catch (RpcThrottlingException e) {
      LOG.debug("Throttling exception for user=" + ugi.getUserName() + " table=" + table + " scan="
        + scanRequest.getScannerId() + ": " + e.getMessage());
      throw e;
    }
    return quota;
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method does not support
   * scans because estimating a scan's workload is more complicated than estimating the workload of
   * a get/put.
   * @param region the region where the operation will be performed
   * @param type   the operation type
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkBatchQuota(final Region region,
    final OperationQuota.OperationType type) throws IOException, RpcThrottlingException {
    switch (type) {
      case GET:
        return this.checkBatchQuota(region, 0, 1);
      case MUTATE:
        return this.checkBatchQuota(region, 1, 0);
      case CHECK_AND_MUTATE:
        return this.checkBatchQuota(region, 1, 1);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method does not support
   * scans because estimating a scan's workload is more complicated than estimating the workload of
   * a get/put.
   * @param region       the region where the operation will be performed
   * @param actions      the "multi" actions to perform
   * @param hasCondition whether the RegionAction has a condition
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkBatchQuota(final Region region,
    final List<ClientProtos.Action> actions, boolean hasCondition)
    throws IOException, RpcThrottlingException {
    int numWrites = 0;
    int numReads = 0;
    for (final ClientProtos.Action action : actions) {
      if (action.hasMutation()) {
        numWrites++;
        OperationQuota.OperationType operationType =
          QuotaUtil.getQuotaOperationType(action, hasCondition);
        if (operationType == OperationQuota.OperationType.CHECK_AND_MUTATE) {
          numReads++;
        }
      } else if (action.hasGet()) {
        numReads++;
      }
    }
    return checkBatchQuota(region, numWrites, numReads);
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation.
   * @param region    the region where the operation will be performed
   * @param numWrites number of writes to perform
   * @param numReads  number of short-reads to perform
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  private OperationQuota checkBatchQuota(final Region region, final int numWrites,
    final int numReads) throws IOException, RpcThrottlingException {
    Optional<User> user = RpcServer.getRequestUser();
    UserGroupInformation ugi;
    if (user.isPresent()) {
      ugi = user.get().getUGI();
    } else {
      ugi = User.getCurrent().getUGI();
    }
    TableDescriptor tableDescriptor = region.getTableDescriptor();
    TableName table = tableDescriptor.getTableName();

    OperationQuota quota = getQuota(ugi, table, region.getMinBlockSizeBytes());
    try {
      quota.checkBatchQuota(numWrites, numReads);
    } catch (RpcThrottlingException e) {
      LOG.debug("Throttling exception for user=" + ugi.getUserName() + " table=" + table
        + " numWrites=" + numWrites + " numReads=" + numReads + ": " + e.getMessage());
      throw e;
    }
    return quota;
  }
}
