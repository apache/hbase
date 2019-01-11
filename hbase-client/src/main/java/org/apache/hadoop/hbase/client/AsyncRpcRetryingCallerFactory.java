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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

/**
 * Factory to create an AsyncRpcRetryCaller.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class AsyncRpcRetryingCallerFactory {

  private final AsyncConnectionImpl conn;

  private final Timer retryTimer;

  public AsyncRpcRetryingCallerFactory(AsyncConnectionImpl conn, Timer retryTimer) {
    this.conn = conn;
    this.retryTimer = retryTimer;
  }

  private abstract class BuilderBase {

    protected long pauseNs = conn.connConf.getPauseNs();

    protected int maxAttempts = retries2Attempts(conn.connConf.getMaxRetries());

    protected int startLogErrorsCnt = conn.connConf.getStartLogErrorsCnt();
  }

  public class SingleRequestCallerBuilder<T> extends BuilderBase {

    private TableName tableName;

    private byte[] row;

    private AsyncSingleRequestRpcRetryingCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    private RegionLocateType locateType = RegionLocateType.CURRENT;

    private int replicaId = RegionReplicaUtil.DEFAULT_REPLICA_ID;

    public SingleRequestCallerBuilder<T> table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SingleRequestCallerBuilder<T> row(byte[] row) {
      this.row = row;
      return this;
    }

    public SingleRequestCallerBuilder<T> action(
        AsyncSingleRequestRpcRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public SingleRequestCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public SingleRequestCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public SingleRequestCallerBuilder<T> locateType(RegionLocateType locateType) {
      this.locateType = locateType;
      return this;
    }

    public SingleRequestCallerBuilder<T> pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public SingleRequestCallerBuilder<T> maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public SingleRequestCallerBuilder<T> startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public SingleRequestCallerBuilder<T> replicaId(int replicaId) {
      this.replicaId = replicaId;
      return this;
    }

    public AsyncSingleRequestRpcRetryingCaller<T> build() {
      checkArgument(replicaId >= 0, "invalid replica id %s", replicaId);
      return new AsyncSingleRequestRpcRetryingCaller<>(retryTimer, conn,
        checkNotNull(tableName, "tableName is null"), checkNotNull(row, "row is null"), replicaId,
        checkNotNull(locateType, "locateType is null"), checkNotNull(callable, "action is null"),
        pauseNs, maxAttempts, operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
    }

    /**
     * Shortcut for {@code build().call()}
     */
    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  /**
   * Create retry caller for single action, such as get, put, delete, etc.
   */
  public <T> SingleRequestCallerBuilder<T> single() {
    return new SingleRequestCallerBuilder<>();
  }

  public class ScanSingleRegionCallerBuilder extends BuilderBase {

    private Long scannerId = null;

    private Scan scan;

    private ScanMetrics scanMetrics;

    private ScanResultCache resultCache;

    private AdvancedScanResultConsumer consumer;

    private ClientService.Interface stub;

    private HRegionLocation loc;

    private boolean isRegionServerRemote;

    private long scannerLeaseTimeoutPeriodNs;

    private long scanTimeoutNs;

    private long rpcTimeoutNs;

    public ScanSingleRegionCallerBuilder id(long scannerId) {
      this.scannerId = scannerId;
      return this;
    }

    public ScanSingleRegionCallerBuilder setScan(Scan scan) {
      this.scan = scan;
      return this;
    }

    public ScanSingleRegionCallerBuilder metrics(ScanMetrics scanMetrics) {
      this.scanMetrics = scanMetrics;
      return this;
    }

    public ScanSingleRegionCallerBuilder remote(boolean isRegionServerRemote) {
      this.isRegionServerRemote = isRegionServerRemote;
      return this;
    }

    public ScanSingleRegionCallerBuilder resultCache(ScanResultCache resultCache) {
      this.resultCache = resultCache;
      return this;
    }

    public ScanSingleRegionCallerBuilder consumer(AdvancedScanResultConsumer consumer) {
      this.consumer = consumer;
      return this;
    }

    public ScanSingleRegionCallerBuilder stub(ClientService.Interface stub) {
      this.stub = stub;
      return this;
    }

    public ScanSingleRegionCallerBuilder location(HRegionLocation loc) {
      this.loc = loc;
      return this;
    }

    public ScanSingleRegionCallerBuilder scannerLeaseTimeoutPeriod(long scannerLeaseTimeoutPeriod,
        TimeUnit unit) {
      this.scannerLeaseTimeoutPeriodNs = unit.toNanos(scannerLeaseTimeoutPeriod);
      return this;
    }

    public ScanSingleRegionCallerBuilder scanTimeout(long scanTimeout, TimeUnit unit) {
      this.scanTimeoutNs = unit.toNanos(scanTimeout);
      return this;
    }

    public ScanSingleRegionCallerBuilder rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public ScanSingleRegionCallerBuilder pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public ScanSingleRegionCallerBuilder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public ScanSingleRegionCallerBuilder startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public AsyncScanSingleRegionRpcRetryingCaller build() {
      checkArgument(scannerId != null, "invalid scannerId %d", scannerId);
      return new AsyncScanSingleRegionRpcRetryingCaller(retryTimer, conn,
        checkNotNull(scan, "scan is null"), scanMetrics, scannerId,
        checkNotNull(resultCache, "resultCache is null"),
        checkNotNull(consumer, "consumer is null"), checkNotNull(stub, "stub is null"),
        checkNotNull(loc, "location is null"), isRegionServerRemote, scannerLeaseTimeoutPeriodNs,
        pauseNs, maxAttempts, scanTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
    }

    /**
     * Short cut for {@code build().start(HBaseRpcController, ScanResponse)}.
     */
    public CompletableFuture<Boolean> start(HBaseRpcController controller,
        ScanResponse respWhenOpen) {
      return build().start(controller, respWhenOpen);
    }
  }

  /**
   * Create retry caller for scanning a region.
   */
  public ScanSingleRegionCallerBuilder scanSingleRegion() {
    return new ScanSingleRegionCallerBuilder();
  }

  public class BatchCallerBuilder extends BuilderBase {

    private TableName tableName;

    private List<? extends Row> actions;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    public BatchCallerBuilder table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public BatchCallerBuilder actions(List<? extends Row> actions) {
      this.actions = actions;
      return this;
    }

    public BatchCallerBuilder operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public BatchCallerBuilder rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public BatchCallerBuilder pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public BatchCallerBuilder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public BatchCallerBuilder startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public <T> AsyncBatchRpcRetryingCaller<T> build() {
      return new AsyncBatchRpcRetryingCaller<>(retryTimer, conn, tableName, actions, pauseNs,
        maxAttempts, operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
    }

    public <T> List<CompletableFuture<T>> call() {
      return this.<T> build().call();
    }
  }

  public BatchCallerBuilder batch() {
    return new BatchCallerBuilder();
  }

  public class MasterRequestCallerBuilder<T> extends BuilderBase {
    private AsyncMasterRequestRpcRetryingCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    public MasterRequestCallerBuilder<T> action(
        AsyncMasterRequestRpcRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public MasterRequestCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public MasterRequestCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public MasterRequestCallerBuilder<T> pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public MasterRequestCallerBuilder<T> maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public MasterRequestCallerBuilder<T> startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public AsyncMasterRequestRpcRetryingCaller<T> build() {
      return new AsyncMasterRequestRpcRetryingCaller<T>(retryTimer, conn,
        checkNotNull(callable, "action is null"), pauseNs, maxAttempts, operationTimeoutNs,
        rpcTimeoutNs, startLogErrorsCnt);
    }

    /**
     * Shortcut for {@code build().call()}
     */
    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  public <T> MasterRequestCallerBuilder<T> masterRequest() {
    return new MasterRequestCallerBuilder<>();
  }

  public class AdminRequestCallerBuilder<T> extends BuilderBase {
    // TODO: maybe we can reuse AdminRequestCallerBuild, MasterRequestCallerBuild etc.

    private AsyncAdminRequestRetryingCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    private ServerName serverName;

    public AdminRequestCallerBuilder<T> action(
        AsyncAdminRequestRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public AdminRequestCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public AdminRequestCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public AdminRequestCallerBuilder<T> pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public AdminRequestCallerBuilder<T> maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public AdminRequestCallerBuilder<T> startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public AdminRequestCallerBuilder<T> serverName(ServerName serverName) {
      this.serverName = serverName;
      return this;
    }

    public AsyncAdminRequestRetryingCaller<T> build() {
      return new AsyncAdminRequestRetryingCaller<T>(retryTimer, conn, pauseNs, maxAttempts,
        operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt,
        checkNotNull(serverName, "serverName is null"), checkNotNull(callable, "action is null"));
    }

    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  public <T> AdminRequestCallerBuilder<T> adminRequest() {
    return new AdminRequestCallerBuilder<>();
  }

  public class ServerRequestCallerBuilder<T> extends BuilderBase {

    private AsyncServerRequestRpcRetryingCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    private ServerName serverName;

    public ServerRequestCallerBuilder<T> action(
        AsyncServerRequestRpcRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public ServerRequestCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public ServerRequestCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public ServerRequestCallerBuilder<T> pause(long pause, TimeUnit unit) {
      this.pauseNs = unit.toNanos(pause);
      return this;
    }

    public ServerRequestCallerBuilder<T> maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public ServerRequestCallerBuilder<T> startLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public ServerRequestCallerBuilder<T> serverName(ServerName serverName) {
      this.serverName = serverName;
      return this;
    }

    public AsyncServerRequestRpcRetryingCaller<T> build() {
      return new AsyncServerRequestRpcRetryingCaller<T>(retryTimer, conn, pauseNs, maxAttempts,
        operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt,
        checkNotNull(serverName, "serverName is null"), checkNotNull(callable, "action is null"));
    }

    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  public <T> ServerRequestCallerBuilder<T> serverRequest() {
    return new ServerRequestCallerBuilder<>();
  }
}
