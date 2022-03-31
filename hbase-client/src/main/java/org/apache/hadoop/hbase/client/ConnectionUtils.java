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
package org.apache.hadoop.hbase.client;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility used by client connections.
 */
@InterfaceAudience.Private
public final class ConnectionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtils.class);

  private ConnectionUtils() {
  }

  /**
   * Calculate pause time. Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause time to pause
   * @param tries amount of tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }
    if (ntries < 0) {
      ntries = 0;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    // 1% possible jitter
    long jitter = (long) (normalPause * ThreadLocalRandom.current().nextFloat() * 0.01f);
    return normalPause + jitter;
  }

  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  public static NonceGenerator injectNonceGeneratorForTesting(ClusterConnection conn,
      NonceGenerator cnm) {
    return ConnectionImplementation.injectNonceGeneratorForTesting(conn, cnm);
  }

  /**
   * Changes the configuration to set the number of retries needed when using Connection internally,
   * e.g. for updating catalog tables, etc. Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetriesConfig(final Configuration c, final String sn,
      final Logger log) {
    // TODO: Fix this. Not all connections from server side should have 10 times the retries.
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big. Multiply by 10. If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
      HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.info(sn + " server-side Connection retries=" + retries);
  }

  /**
   * Setup the connection class, so that it will not depend on master being online. Used for testing
   * @param conf configuration to set
   */
  public static void setupMasterlessConnection(Configuration conf) {
    conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, MasterlessConnection.class.getName());
  }

  /**
   * Some tests shut down the master. But table availability is a master RPC which is performed on
   * region re-lookups.
   */
  static class MasterlessConnection extends ConnectionImplementation {
    MasterlessConnection(Configuration conf, ExecutorService pool, User user) throws IOException {
      super(conf, pool, user);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
      // treat all tables as enabled
      return false;
    }
  }

  /**
   * Return retires + 1. The returned value will be in range [1, Integer.MAX_VALUE].
   */
  static int retries2Attempts(int retries) {
    return Math.max(1, retries == Integer.MAX_VALUE ? Integer.MAX_VALUE : retries + 1);
  }

  /**
   * Get a unique key for the rpc stub to the given server.
   */
  static String getStubKey(String serviceName, ServerName serverName, boolean hostnameCanChange) {
    // Sometimes, servers go down and they come back up with the same hostname but a different
    // IP address. Force a resolution of the rsHostname by trying to instantiate an
    // InetSocketAddress, and this way we will rightfully get a new stubKey.
    // Also, include the hostname in the key so as to take care of those cases where the
    // DNS name is different but IP address remains the same.
    String hostname = serverName.getHostname();
    int port = serverName.getPort();
    if (hostnameCanChange) {
      try {
        InetAddress ip = InetAddress.getByName(hostname);
        return serviceName + "@" + hostname + "-" + ip.getHostAddress() + ":" + port;
      } catch (UnknownHostException e) {
        LOG.warn("Can not resolve " + hostname + ", please check your network", e);
      }
    }
    return serviceName + "@" + hostname + ":" + port;
  }

  static void checkHasFamilies(Mutation mutation) {
    Preconditions.checkArgument(mutation.numFamilies() > 0,
      "Invalid arguments to %s, zero columns specified", mutation.toString());
  }

  /** Dummy nonce generator for disabled nonces. */
  static final NonceGenerator NO_NONCE_GENERATOR = new NonceGenerator() {

    @Override
    public long newNonce() {
      return HConstants.NO_NONCE;
    }

    @Override
    public long getNonceGroup() {
      return HConstants.NO_NONCE;
    }
  };

  // A byte array in which all elements are the max byte, and it is used to
  // construct closest front row
  static final byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);

  /**
   * Create the closest row after the specified row
   */
  static byte[] createClosestRowAfter(byte[] row) {
    return Arrays.copyOf(row, row.length + 1);
  }

  /**
   * Create a row before the specified row and very close to the specified row.
   */
  static byte[] createCloseRowBefore(byte[] row) {
    if (row.length == 0) {
      return MAX_BYTE_ARRAY;
    }
    if (row[row.length - 1] == 0) {
      return Arrays.copyOf(row, row.length - 1);
    } else {
      byte[] nextRow = new byte[row.length + MAX_BYTE_ARRAY.length];
      System.arraycopy(row, 0, nextRow, 0, row.length - 1);
      nextRow[row.length - 1] = (byte) ((row[row.length - 1] & 0xFF) - 1);
      System.arraycopy(MAX_BYTE_ARRAY, 0, nextRow, row.length, MAX_BYTE_ARRAY.length);
      return nextRow;
    }
  }

  static boolean isEmptyStartRow(byte[] row) {
    return Bytes.equals(row, EMPTY_START_ROW);
  }

  static boolean isEmptyStopRow(byte[] row) {
    return Bytes.equals(row, EMPTY_END_ROW);
  }

  static void resetController(HBaseRpcController controller, long timeoutNs, int priority) {
    controller.reset();
    if (timeoutNs >= 0) {
      controller.setCallTimeout(
        (int) Math.min(Integer.MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(timeoutNs)));
    }
    controller.setPriority(priority);
  }

  static Throwable translateException(Throwable t) {
    if (t instanceof UndeclaredThrowableException && t.getCause() != null) {
      t = t.getCause();
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof ServiceException && t.getCause() != null) {
      t = translateException(t.getCause());
    }
    return t;
  }

  static long calcEstimatedSize(Result rs) {
    long estimatedHeapSizeOfResult = 0;
    // We don't make Iterator here
    for (Cell cell : rs.rawCells()) {
      estimatedHeapSizeOfResult += cell.heapSize();
    }
    return estimatedHeapSizeOfResult;
  }

  static Result filterCells(Result result, Cell keepCellsAfter) {
    if (keepCellsAfter == null) {
      // do not need to filter
      return result;
    }
    // not the same row
    if (!PrivateCellUtil.matchingRows(keepCellsAfter, result.getRow(), 0, result.getRow().length)) {
      return result;
    }
    Cell[] rawCells = result.rawCells();
    int index = Arrays.binarySearch(rawCells, keepCellsAfter,
      CellComparator.getInstance()::compareWithoutRow);
    if (index < 0) {
      index = -index - 1;
    } else {
      index++;
    }
    if (index == 0) {
      return result;
    }
    if (index == rawCells.length) {
      return null;
    }
    return Result.create(Arrays.copyOfRange(rawCells, index, rawCells.length), null,
      result.isStale(), result.mayHaveMoreCellsInRow());
  }

  // Add a delta to avoid timeout immediately after a retry sleeping.
  static final long SLEEP_DELTA_NS = TimeUnit.MILLISECONDS.toNanos(1);

  static Get toCheckExistenceOnly(Get get) {
    if (get.isCheckExistenceOnly()) {
      return get;
    }
    return ReflectionUtils.newInstance(get.getClass(), get).setCheckExistenceOnly(true);
  }

  static List<Get> toCheckExistenceOnly(List<Get> gets) {
    return gets.stream().map(ConnectionUtils::toCheckExistenceOnly).collect(toList());
  }

  static RegionLocateType getLocateType(Scan scan) {
    if (scan.isReversed()) {
      if (isEmptyStartRow(scan.getStartRow())) {
        return RegionLocateType.BEFORE;
      } else {
        return scan.includeStartRow() ? RegionLocateType.CURRENT : RegionLocateType.BEFORE;
      }
    } else {
      return scan.includeStartRow() ? RegionLocateType.CURRENT : RegionLocateType.AFTER;
    }
  }

  static boolean noMoreResultsForScan(Scan scan, RegionInfo info) {
    if (isEmptyStopRow(info.getEndKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    int c = Bytes.compareTo(info.getEndKey(), scan.getStopRow());
    // 1. if our stop row is less than the endKey of the region
    // 2. if our stop row is equal to the endKey of the region and we do not include the stop row
    // for scan.
    return c > 0 || (c == 0 && !scan.includeStopRow());
  }

  static boolean noMoreResultsForReverseScan(Scan scan, RegionInfo info) {
    if (isEmptyStartRow(info.getStartKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    // no need to test the inclusive of the stop row as the start key of a region is included in
    // the region.
    return Bytes.compareTo(info.getStartKey(), scan.getStopRow()) <= 0;
  }

  static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
      .thenApply(v -> futures.stream().map(f -> f.getNow(null)).collect(toList()));
  }

  public static ScanResultCache createScanResultCache(Scan scan) {
    if (scan.getAllowPartialResults()) {
      return new AllowPartialScanResultCache();
    } else if (scan.getBatch() > 0) {
      return new BatchScanResultCache(scan.getBatch());
    } else {
      return new CompleteScanResultCache();
    }
  }

  private static final String MY_ADDRESS = getMyAddress();

  private static String getMyAddress() {
    try {
      return DNS.getDefaultHost("default", "default");
    } catch (UnknownHostException uhe) {
      LOG.error("cannot determine my address", uhe);
      return null;
    }
  }

  static boolean isRemote(String host) {
    return !host.equalsIgnoreCase(MY_ADDRESS);
  }

  static void incRPCCallsMetrics(ScanMetrics scanMetrics, boolean isRegionServerRemote) {
    if (scanMetrics == null) {
      return;
    }
    scanMetrics.countOfRPCcalls.incrementAndGet();
    if (isRegionServerRemote) {
      scanMetrics.countOfRemoteRPCcalls.incrementAndGet();
    }
  }

  static void incRPCRetriesMetrics(ScanMetrics scanMetrics, boolean isRegionServerRemote) {
    if (scanMetrics == null) {
      return;
    }
    scanMetrics.countOfRPCRetries.incrementAndGet();
    if (isRegionServerRemote) {
      scanMetrics.countOfRemoteRPCRetries.incrementAndGet();
    }
  }

  static void updateResultsMetrics(ScanMetrics scanMetrics, Result[] rrs,
      boolean isRegionServerRemote) {
    if (scanMetrics == null || rrs == null || rrs.length == 0) {
      return;
    }
    long resultSize = 0;
    for (Result rr : rrs) {
      for (Cell cell : rr.rawCells()) {
        resultSize += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
    }
    scanMetrics.countOfBytesInResults.addAndGet(resultSize);
    if (isRegionServerRemote) {
      scanMetrics.countOfBytesInRemoteResults.addAndGet(resultSize);
    }
  }

  /**
   * Use the scan metrics returned by the server to add to the identically named counters in the
   * client side metrics. If a counter does not exist with the same name as the server side metric,
   * the attempt to increase the counter will fail.
   */
  static void updateServerSideMetrics(ScanMetrics scanMetrics, ScanResponse response) {
    if (scanMetrics == null || response == null || !response.hasScanMetrics()) {
      return;
    }
    ResponseConverter.getScanMetrics(response).forEach(scanMetrics::addToCounter);
  }

  static void incRegionCountMetrics(ScanMetrics scanMetrics) {
    if (scanMetrics == null) {
      return;
    }
    scanMetrics.countOfRegions.incrementAndGet();
  }

  /**
   * Connect the two futures, if the src future is done, then mark the dst future as done. And if
   * the dst future is done, then cancel the src future. This is used for timeline consistent read.
   * <p/>
   * Pass empty metrics if you want to link the primary future and the dst future so we will not
   * increase the hedge read related metrics.
   */
  private static <T> void connect(CompletableFuture<T> srcFuture, CompletableFuture<T> dstFuture,
      Optional<MetricsConnection> metrics) {
    addListener(srcFuture, (r, e) -> {
      if (e != null) {
        dstFuture.completeExceptionally(e);
      } else {
        if (dstFuture.complete(r)) {
          metrics.ifPresent(MetricsConnection::incrHedgedReadWin);
        }
      }
    });
    // The cancellation may be a dummy one as the dstFuture may be completed by this srcFuture.
    // Notice that this is a bit tricky, as the execution chain maybe 'complete src -> complete dst
    // -> cancel src', for now it seems to be fine, as the will use CAS to set the result first in
    // CompletableFuture. If later this causes problems, we could use whenCompleteAsync to break the
    // tie.
    addListener(dstFuture, (r, e) -> srcFuture.cancel(false));
  }

  private static <T> void sendRequestsToSecondaryReplicas(
      Function<Integer, CompletableFuture<T>> requestReplica, RegionLocations locs,
      CompletableFuture<T> future, Optional<MetricsConnection> metrics) {
    if (future.isDone()) {
      // do not send requests to secondary replicas if the future is done, i.e, the primary request
      // has already been finished.
      return;
    }
    for (int replicaId = 1, n = locs.size(); replicaId < n; replicaId++) {
      CompletableFuture<T> secondaryFuture = requestReplica.apply(replicaId);
      metrics.ifPresent(MetricsConnection::incrHedgedReadOps);
      connect(secondaryFuture, future, metrics);
    }
  }

  static <T> CompletableFuture<T> timelineConsistentRead(AsyncRegionLocator locator,
      TableName tableName, Query query, byte[] row, RegionLocateType locateType,
      Function<Integer, CompletableFuture<T>> requestReplica, long rpcTimeoutNs,
      long primaryCallTimeoutNs, Timer retryTimer, Optional<MetricsConnection> metrics) {
    if (query.getConsistency() != Consistency.TIMELINE) {
      return requestReplica.apply(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    }
    // user specifies a replica id explicitly, just send request to the specific replica
    if (query.getReplicaId() >= 0) {
      return requestReplica.apply(query.getReplicaId());
    }
    // Timeline consistent read, where we may send requests to other region replicas
    CompletableFuture<T> primaryFuture = requestReplica.apply(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    CompletableFuture<T> future = new CompletableFuture<>();
    connect(primaryFuture, future, Optional.empty());
    long startNs = System.nanoTime();
    // after the getRegionLocations, all the locations for the replicas of this region should have
    // been cached, so it is not big deal to locate them again when actually sending requests to
    // these replicas.
    addListener(locator.getRegionLocations(tableName, row, locateType, false, rpcTimeoutNs),
      (locs, error) -> {
        if (error != null) {
          LOG.warn(
            "Failed to locate all the replicas for table={}, row='{}', locateType={}" +
              " give up timeline consistent read",
            tableName, Bytes.toStringBinary(row), locateType, error);
          return;
        }
        if (locs.size() <= 1) {
          LOG.warn(
            "There are no secondary replicas for region {}, give up timeline consistent read",
            locs.getDefaultRegionLocation().getRegion());
          return;
        }
        long delayNs = primaryCallTimeoutNs - (System.nanoTime() - startNs);
        if (delayNs <= 0) {
          sendRequestsToSecondaryReplicas(requestReplica, locs, future, metrics);
        } else {
          retryTimer.newTimeout(
            timeout -> sendRequestsToSecondaryReplicas(requestReplica, locs, future, metrics),
            delayNs, TimeUnit.NANOSECONDS);
        }
      });
    return future;
  }

  // validate for well-formedness
  static void validatePut(Put put, int maxKeyValueSize) {
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<Cell> list : put.getFamilyCellMap().values()) {
        for (Cell cell : list) {
          if (cell.getSerializedSize() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  static void validatePutsInRowMutations(RowMutations rowMutations, int maxKeyValueSize) {
    for (Mutation mutation : rowMutations.getMutations()) {
      if (mutation instanceof Put) {
        validatePut((Put) mutation, maxKeyValueSize);
      }
    }
  }

  /**
   * Select the priority for the rpc call.
   * <p/>
   * The rules are:
   * <ol>
   * <li>If user set a priority explicitly, then just use it.</li>
   * <li>For system table, use {@link HConstants#SYSTEMTABLE_QOS}.</li>
   * <li>For other tables, use {@link HConstants#NORMAL_QOS}.</li>
   * </ol>
   * @param priority the priority set by user, can be {@link HConstants#PRIORITY_UNSET}.
   * @param tableName the table we operate on
   */
  static int calcPriority(int priority, TableName tableName) {
    if (priority != HConstants.PRIORITY_UNSET) {
      return priority;
    } else {
      return getPriority(tableName);
    }
  }

  static int getPriority(TableName tableName) {
    if (tableName.isSystemTable()) {
      return HConstants.SYSTEMTABLE_QOS;
    } else {
      return HConstants.NORMAL_QOS;
    }
  }

  static <T> CompletableFuture<T> getOrFetch(AtomicReference<T> cacheRef,
      AtomicReference<CompletableFuture<T>> futureRef, boolean reload,
      Supplier<CompletableFuture<T>> fetch, Predicate<T> validator, String type) {
    for (;;) {
      if (!reload) {
        T value = cacheRef.get();
        if (value != null && validator.test(value)) {
          return CompletableFuture.completedFuture(value);
        }
      }
      LOG.trace("{} cache is null, try fetching from registry", type);
      if (futureRef.compareAndSet(null, new CompletableFuture<>())) {
        LOG.debug("Start fetching {} from registry", type);
        CompletableFuture<T> future = futureRef.get();
        addListener(fetch.get(), (value, error) -> {
          if (error != null) {
            LOG.debug("Failed to fetch {} from registry", type, error);
            futureRef.getAndSet(null).completeExceptionally(error);
            return;
          }
          LOG.debug("The fetched {} is {}", type, value);
          // Here we update cache before reset future, so it is possible that someone can get a
          // stale value. Consider this:
          // 1. update cacheRef
          // 2. someone clears the cache and relocates again
          // 3. the futureRef is not null so the old future is used.
          // 4. we clear futureRef and complete the future in it with the value being
          // cleared in step 2.
          // But we do not think it is a big deal as it rarely happens, and even if it happens, the
          // caller will retry again later, no correctness problems.
          cacheRef.set(value);
          futureRef.set(null);
          future.complete(value);
        });
        return future;
      } else {
        CompletableFuture<T> future = futureRef.get();
        if (future != null) {
          return future;
        }
      }
    }
  }

  static void updateStats(Optional<ServerStatisticTracker> optStats,
      Optional<MetricsConnection> optMetrics, ServerName serverName, MultiResponse resp) {
    if (!optStats.isPresent() && !optMetrics.isPresent()) {
      // ServerStatisticTracker and MetricsConnection are both not present, just return
      return;
    }
    resp.getResults().forEach((regionName, regionResult) -> {
      ClientProtos.RegionLoadStats stat = regionResult.getStat();
      if (stat == null) {
        LOG.error("No ClientProtos.RegionLoadStats found for server={}, region={}", serverName,
          Bytes.toStringBinary(regionName));
        return;
      }
      RegionLoadStats regionLoadStats = ProtobufUtil.createRegionLoadStats(stat);
      optStats.ifPresent(
        stats -> ResultStatsUtil.updateStats(stats, serverName, regionName, regionLoadStats));
      optMetrics.ifPresent(
        metrics -> ResultStatsUtil.updateStats(metrics, serverName, regionName, regionLoadStats));
    });
  }
}
