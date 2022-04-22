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

import static org.apache.hadoop.hbase.CellUtil.createCellScanner;
import static org.apache.hadoop.hbase.client.ConnectionUtils.SLEEP_DELTA_NS;
import static org.apache.hadoop.hbase.client.ConnectionUtils.calcPriority;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getPauseTime;
import static org.apache.hadoop.hbase.client.ConnectionUtils.resetController;
import static org.apache.hadoop.hbase.client.ConnectionUtils.translateException;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.util.FutureUtils.unwrapCompletionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MultiResponse.RegionResult;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.util.Timer;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

/**
 * Retry caller for batch.
 * <p>
 * Notice that, the {@link #operationTimeoutNs} is the total time limit now which is the same with
 * other single operations
 * <p>
 * And the {@link #maxAttempts} is a limit for each single operation in the batch logically. In the
 * implementation, we will record a {@code tries} parameter for each operation group, and if it is
 * split to several groups when retrying, the sub groups will inherit the {@code tries}. You can
 * imagine that the whole retrying process is a tree, and the {@link #maxAttempts} is the limit of
 * the depth of the tree.
 */
@InterfaceAudience.Private
class AsyncBatchRpcRetryingCaller<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncBatchRpcRetryingCaller.class);

  private final Timer retryTimer;

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final List<Action> actions;

  private final List<CompletableFuture<T>> futures;

  private final IdentityHashMap<Action, CompletableFuture<T>> action2Future;

  private final IdentityHashMap<Action, List<ThrowableWithExtraContext>> action2Errors;

  private final long pauseNs;

  private final long pauseNsForServerOverloaded;

  private final int maxAttempts;

  private final long operationTimeoutNs;

  private final long rpcTimeoutNs;

  private final int startLogErrorsCnt;

  private final long startNs;

  // we can not use HRegionLocation as the map key because the hashCode and equals method of
  // HRegionLocation only consider serverName.
  private static final class RegionRequest {

    public final HRegionLocation loc;

    public final ConcurrentLinkedQueue<Action> actions = new ConcurrentLinkedQueue<>();

    public RegionRequest(HRegionLocation loc) {
      this.loc = loc;
    }
  }

  private static final class ServerRequest {

    public final ConcurrentMap<byte[], RegionRequest> actionsByRegion =
      new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

    public void addAction(HRegionLocation loc, Action action) {
      computeIfAbsent(actionsByRegion, loc.getRegion().getRegionName(),
        () -> new RegionRequest(loc)).actions.add(action);
    }

    public void setRegionRequest(byte[] regionName, RegionRequest regionReq) {
      actionsByRegion.put(regionName, regionReq);
    }

    public int getPriority() {
      return actionsByRegion.values().stream().flatMap(rr -> rr.actions.stream())
        .mapToInt(Action::getPriority).max().orElse(HConstants.PRIORITY_UNSET);
    }
  }

  public AsyncBatchRpcRetryingCaller(Timer retryTimer, AsyncConnectionImpl conn,
      TableName tableName, List<? extends Row> actions, long pauseNs,
      long pauseNsForServerOverloaded, int maxAttempts, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.conn = conn;
    this.tableName = tableName;
    this.pauseNs = pauseNs;
    this.pauseNsForServerOverloaded = pauseNsForServerOverloaded;
    this.maxAttempts = maxAttempts;
    this.operationTimeoutNs = operationTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.actions = new ArrayList<>(actions.size());
    this.futures = new ArrayList<>(actions.size());
    this.action2Future = new IdentityHashMap<>(actions.size());
    for (int i = 0, n = actions.size(); i < n; i++) {
      Row rawAction = actions.get(i);
      Action action;
      if (rawAction instanceof OperationWithAttributes) {
        action = new Action(rawAction, i, ((OperationWithAttributes) rawAction).getPriority());
      } else {
        action = new Action(rawAction, i);
      }
      if (hasIncrementOrAppend(rawAction)) {
        action.setNonce(conn.getNonceGenerator().newNonce());
      }
      this.actions.add(action);
      CompletableFuture<T> future = new CompletableFuture<>();
      futures.add(future);
      action2Future.put(action, future);
    }
    this.action2Errors = new IdentityHashMap<>();
    this.startNs = System.nanoTime();
  }

  private static boolean hasIncrementOrAppend(Row action) {
    if (action instanceof Append || action instanceof Increment) {
      return true;
    } else if (action instanceof RowMutations) {
      return hasIncrementOrAppend((RowMutations) action);
    } else if (action instanceof CheckAndMutate) {
      return hasIncrementOrAppend(((CheckAndMutate) action).getAction());
    }
    return false;
  }

  private static boolean hasIncrementOrAppend(RowMutations mutations) {
    for (Mutation mutation : mutations.getMutations()) {
      if (mutation instanceof Append || mutation instanceof Increment) {
        return true;
      }
    }
    return false;
  }

  private long remainingTimeNs() {
    return operationTimeoutNs - (System.nanoTime() - startNs);
  }

  private List<ThrowableWithExtraContext> removeErrors(Action action) {
    synchronized (action2Errors) {
      return action2Errors.remove(action);
    }
  }

  private void logException(int tries, Supplier<Stream<RegionRequest>> regionsSupplier,
      Throwable error, ServerName serverName) {
    if (tries > startLogErrorsCnt) {
      String regions =
        regionsSupplier.get().map(r -> "'" + r.loc.getRegion().getRegionNameAsString() + "'")
          .collect(Collectors.joining(",", "[", "]"));
      LOG.warn("Process batch for " + regions + " in " + tableName + " from " + serverName +
        " failed, tries=" + tries, error);
    }
  }

  private String getExtraContextForError(ServerName serverName) {
    return serverName != null ? serverName.getServerName() : "";
  }

  private void addError(Action action, Throwable error, ServerName serverName) {
    List<ThrowableWithExtraContext> errors;
    synchronized (action2Errors) {
      errors = action2Errors.computeIfAbsent(action, k -> new ArrayList<>());
    }
    errors.add(new ThrowableWithExtraContext(error, EnvironmentEdgeManager.currentTime(),
      getExtraContextForError(serverName)));
  }

  private void addError(Iterable<Action> actions, Throwable error, ServerName serverName) {
    actions.forEach(action -> addError(action, error, serverName));
  }

  private void failOne(Action action, int tries, Throwable error, long currentTime, String extras) {
    CompletableFuture<T> future = action2Future.get(action);
    if (future.isDone()) {
      return;
    }
    ThrowableWithExtraContext errorWithCtx =
      new ThrowableWithExtraContext(error, currentTime, extras);
    List<ThrowableWithExtraContext> errors = removeErrors(action);
    if (errors == null) {
      errors = Collections.singletonList(errorWithCtx);
    } else {
      errors.add(errorWithCtx);
    }
    future.completeExceptionally(new RetriesExhaustedException(tries - 1, errors));
  }

  private void failAll(Stream<Action> actions, int tries, Throwable error, ServerName serverName) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    String extras = getExtraContextForError(serverName);
    actions.forEach(action -> failOne(action, tries, error, currentTime, extras));
  }

  private void failAll(Stream<Action> actions, int tries) {
    actions.forEach(action -> {
      CompletableFuture<T> future = action2Future.get(action);
      if (future.isDone()) {
        return;
      }
      future.completeExceptionally(new RetriesExhaustedException(tries,
        Optional.ofNullable(removeErrors(action)).orElse(Collections.emptyList())));
    });
  }

  private ClientProtos.MultiRequest buildReq(Map<byte[], RegionRequest> actionsByRegion,
      List<CellScannable> cells, Map<Integer, Integer> indexMap) throws IOException {
    ClientProtos.MultiRequest.Builder multiRequestBuilder = ClientProtos.MultiRequest.newBuilder();
    ClientProtos.RegionAction.Builder regionActionBuilder = ClientProtos.RegionAction.newBuilder();
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    ClientProtos.MutationProto.Builder mutationBuilder = ClientProtos.MutationProto.newBuilder();
    for (Map.Entry<byte[], RegionRequest> entry : actionsByRegion.entrySet()) {
      long nonceGroup = conn.getNonceGenerator().getNonceGroup();
      // multiRequestBuilder will be populated with region actions.
      // indexMap will be non-empty after the call if there is RowMutations/CheckAndMutate in the
      // action list.
      RequestConverter.buildNoDataRegionActions(entry.getKey(),
        entry.getValue().actions.stream()
          .sorted((a1, a2) -> Integer.compare(a1.getOriginalIndex(), a2.getOriginalIndex()))
          .collect(Collectors.toList()),
        cells, multiRequestBuilder, regionActionBuilder, actionBuilder, mutationBuilder,
        nonceGroup, indexMap);
    }
    return multiRequestBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private void onComplete(Action action, RegionRequest regionReq, int tries, ServerName serverName,
      RegionResult regionResult, List<Action> failedActions, Throwable regionException,
      MutableBoolean retryImmediately) {
    Object result = regionResult.result.getOrDefault(action.getOriginalIndex(), regionException);
    if (result == null) {
      LOG.error("Server " + serverName + " sent us neither result nor exception for row '" +
        Bytes.toStringBinary(action.getAction().getRow()) + "' of " +
        regionReq.loc.getRegion().getRegionNameAsString());
      addError(action, new RuntimeException("Invalid response"), serverName);
      failedActions.add(action);
    } else if (result instanceof Throwable) {
      Throwable error = translateException((Throwable) result);
      logException(tries, () -> Stream.of(regionReq), error, serverName);
      conn.getLocator().updateCachedLocationOnError(regionReq.loc, error);
      if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
        failOne(action, tries, error, EnvironmentEdgeManager.currentTime(),
          getExtraContextForError(serverName));
      } else {
        if (!retryImmediately.booleanValue() && error instanceof RetryImmediatelyException) {
          retryImmediately.setTrue();
        }
        failedActions.add(action);
      }
    } else {
      action2Future.get(action).complete((T) result);
    }
  }

  private void onComplete(Map<byte[], RegionRequest> actionsByRegion, int tries,
      ServerName serverName, MultiResponse resp) {
    ConnectionUtils.updateStats(conn.getStatisticsTracker(), conn.getConnectionMetrics(),
      serverName, resp);
    List<Action> failedActions = new ArrayList<>();
    MutableBoolean retryImmediately = new MutableBoolean(false);
    actionsByRegion.forEach((rn, regionReq) -> {
      RegionResult regionResult = resp.getResults().get(rn);
      Throwable regionException = resp.getException(rn);
      if (regionResult != null) {
        regionReq.actions.forEach(action -> onComplete(action, regionReq, tries, serverName,
          regionResult, failedActions, regionException, retryImmediately));
      } else {
        Throwable error;
        if (regionException == null) {
          LOG.error("Server sent us neither results nor exceptions for {}",
            Bytes.toStringBinary(rn));
          error = new RuntimeException("Invalid response");
        } else {
          error = translateException(regionException);
        }
        logException(tries, () -> Stream.of(regionReq), error, serverName);
        conn.getLocator().updateCachedLocationOnError(regionReq.loc, error);
        if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
          failAll(regionReq.actions.stream(), tries, error, serverName);
          return;
        }
        if (!retryImmediately.booleanValue() && error instanceof RetryImmediatelyException) {
          retryImmediately.setTrue();
        }
        addError(regionReq.actions, error, serverName);
        failedActions.addAll(regionReq.actions);
      }
    });
    if (!failedActions.isEmpty()) {
      tryResubmit(failedActions.stream(), tries, retryImmediately.booleanValue(), false);
    }
  }

  private void sendToServer(ServerName serverName, ServerRequest serverReq, int tries) {
    long remainingNs;
    if (operationTimeoutNs > 0) {
      remainingNs = remainingTimeNs();
      if (remainingNs <= 0) {
        failAll(serverReq.actionsByRegion.values().stream().flatMap(r -> r.actions.stream()),
          tries);
        return;
      }
    } else {
      remainingNs = Long.MAX_VALUE;
    }
    ClientService.Interface stub;
    try {
      stub = conn.getRegionServerStub(serverName);
    } catch (IOException e) {
      onError(serverReq.actionsByRegion, tries, e, serverName);
      return;
    }
    ClientProtos.MultiRequest req;
    List<CellScannable> cells = new ArrayList<>();
    // Map from a created RegionAction to the original index for a RowMutations within
    // the original list of actions. This will be used to process the results when there
    // is RowMutations/CheckAndMutate in the action list.
    Map<Integer, Integer> indexMap = new HashMap<>();
    try {
      req = buildReq(serverReq.actionsByRegion, cells, indexMap);
    } catch (IOException e) {
      onError(serverReq.actionsByRegion, tries, e, serverName);
      return;
    }
    HBaseRpcController controller = conn.rpcControllerFactory.newController();
    resetController(controller, Math.min(rpcTimeoutNs, remainingNs),
      calcPriority(serverReq.getPriority(), tableName));
    if (!cells.isEmpty()) {
      controller.setCellScanner(createCellScanner(cells));
    }
    stub.multi(controller, req, resp -> {
      if (controller.failed()) {
        onError(serverReq.actionsByRegion, tries, controller.getFailed(), serverName);
      } else {
        try {
          onComplete(serverReq.actionsByRegion, tries, serverName, ResponseConverter.getResults(req,
            indexMap, resp, controller.cellScanner()));
        } catch (Exception e) {
          onError(serverReq.actionsByRegion, tries, e, serverName);
          return;
        }
      }
    });
  }

  // We will make use of the ServerStatisticTracker to determine whether we need to delay a bit,
  // based on the load of the region server and the region.
  private void sendOrDelay(Map<ServerName, ServerRequest> actionsByServer, int tries) {
    Optional<MetricsConnection> metrics = conn.getConnectionMetrics();
    Optional<ServerStatisticTracker> optStats = conn.getStatisticsTracker();
    if (!optStats.isPresent()) {
      actionsByServer.forEach((serverName, serverReq) -> {
        metrics.ifPresent(MetricsConnection::incrNormalRunners);
        sendToServer(serverName, serverReq, tries);
      });
      return;
    }
    ServerStatisticTracker stats = optStats.get();
    ClientBackoffPolicy backoffPolicy = conn.getBackoffPolicy();
    actionsByServer.forEach((serverName, serverReq) -> {
      ServerStatistics serverStats = stats.getStats(serverName);
      Map<Long, ServerRequest> groupByBackoff = new HashMap<>();
      serverReq.actionsByRegion.forEach((regionName, regionReq) -> {
        long backoff = backoffPolicy.getBackoffTime(serverName, regionName, serverStats);
        groupByBackoff.computeIfAbsent(backoff, k -> new ServerRequest())
          .setRegionRequest(regionName, regionReq);
      });
      groupByBackoff.forEach((backoff, sr) -> {
        if (backoff > 0) {
          metrics.ifPresent(m -> m.incrDelayRunnersAndUpdateDelayInterval(backoff));
          retryTimer.newTimeout(timer -> sendToServer(serverName, sr, tries), backoff,
            TimeUnit.MILLISECONDS);
        } else {
          metrics.ifPresent(MetricsConnection::incrNormalRunners);
          sendToServer(serverName, sr, tries);
        }
      });
    });
  }

  private void onError(Map<byte[], RegionRequest> actionsByRegion, int tries, Throwable t,
      ServerName serverName) {
    Throwable error = translateException(t);
    logException(tries, () -> actionsByRegion.values().stream(), error, serverName);
    actionsByRegion.forEach(
      (rn, regionReq) -> conn.getLocator().updateCachedLocationOnError(regionReq.loc, error));
    if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
      failAll(actionsByRegion.values().stream().flatMap(r -> r.actions.stream()), tries, error,
        serverName);
      return;
    }
    List<Action> copiedActions = actionsByRegion.values().stream().flatMap(r -> r.actions.stream())
      .collect(Collectors.toList());
    addError(copiedActions, error, serverName);
    tryResubmit(copiedActions.stream(), tries, error instanceof RetryImmediatelyException,
      HBaseServerException.isServerOverloaded(error));
  }

  private void tryResubmit(Stream<Action> actions, int tries, boolean immediately,
      boolean isServerOverloaded) {
    if (immediately) {
      groupAndSend(actions, tries);
      return;
    }
    long delayNs;
    long pauseNsToUse = isServerOverloaded ? pauseNsForServerOverloaded : pauseNs;
    if (operationTimeoutNs > 0) {
      long maxDelayNs = remainingTimeNs() - SLEEP_DELTA_NS;
      if (maxDelayNs <= 0) {
        failAll(actions, tries);
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNsToUse, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNsToUse, tries - 1);
    }
    retryTimer.newTimeout(t -> groupAndSend(actions, tries + 1), delayNs, TimeUnit.NANOSECONDS);
  }

  private void groupAndSend(Stream<Action> actions, int tries) {
    long locateTimeoutNs;
    if (operationTimeoutNs > 0) {
      locateTimeoutNs = remainingTimeNs();
      if (locateTimeoutNs <= 0) {
        failAll(actions, tries);
        return;
      }
    } else {
      locateTimeoutNs = -1L;
    }
    ConcurrentMap<ServerName, ServerRequest> actionsByServer = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<Action> locateFailed = new ConcurrentLinkedQueue<>();
    addListener(CompletableFuture.allOf(actions
      .map(action -> conn.getLocator().getRegionLocation(tableName, action.getAction().getRow(),
        RegionLocateType.CURRENT, locateTimeoutNs).whenComplete((loc, error) -> {
          if (error != null) {
            error = unwrapCompletionException(translateException(error));
            if (error instanceof DoNotRetryIOException) {
              failOne(action, tries, error, EnvironmentEdgeManager.currentTime(), "");
              return;
            }
            addError(action, error, null);
            locateFailed.add(action);
          } else {
            computeIfAbsent(actionsByServer, loc.getServerName(), ServerRequest::new).addAction(loc,
              action);
          }
        }))
      .toArray(CompletableFuture[]::new)), (v, r) -> {
        if (!actionsByServer.isEmpty()) {
          sendOrDelay(actionsByServer, tries);
        }
        if (!locateFailed.isEmpty()) {
          tryResubmit(locateFailed.stream(), tries, false, false);
        }
      });
  }

  public List<CompletableFuture<T>> call() {
    groupAndSend(actions.stream(), 1);
    return futures;
  }
}
