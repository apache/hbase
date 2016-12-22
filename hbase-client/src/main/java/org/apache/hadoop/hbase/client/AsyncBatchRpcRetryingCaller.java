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
import static org.apache.hadoop.hbase.client.ConnectionUtils.getPauseTime;
import static org.apache.hadoop.hbase.client.ConnectionUtils.resetController;
import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;
import static org.apache.hadoop.hbase.client.ConnectionUtils.translateException;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MultiResponse.RegionResult;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

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

  private static final Log LOG = LogFactory.getLog(AsyncBatchRpcRetryingCaller.class);

  private final HashedWheelTimer retryTimer;

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final List<Action> actions;

  private final List<CompletableFuture<T>> futures;

  private final IdentityHashMap<Action, CompletableFuture<T>> action2Future;

  private final IdentityHashMap<Action, List<ThrowableWithExtraContext>> action2Errors;

  private final long pauseNs;

  private final int maxAttempts;

  private final long operationTimeoutNs;

  private final long readRpcTimeoutNs;

  private final long writeRpcTimeoutNs;

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

    public final AtomicLong rpcTimeoutNs;

    public ServerRequest(long defaultRpcTimeoutNs) {
      this.rpcTimeoutNs = new AtomicLong(defaultRpcTimeoutNs);
    }

    public void addAction(HRegionLocation loc, Action action, long rpcTimeoutNs) {
      computeIfAbsent(actionsByRegion, loc.getRegionInfo().getRegionName(),
        () -> new RegionRequest(loc)).actions.add(action);
      // try update the timeout to a larger value
      if (this.rpcTimeoutNs.get() <= 0) {
        return;
      }
      if (rpcTimeoutNs <= 0) {
        this.rpcTimeoutNs.set(-1L);
        return;
      }
      AtomicUtils.updateMax(this.rpcTimeoutNs, rpcTimeoutNs);
    }
  }

  public AsyncBatchRpcRetryingCaller(HashedWheelTimer retryTimer, AsyncConnectionImpl conn,
      TableName tableName, List<? extends Row> actions, long pauseNs, int maxRetries,
      long operationTimeoutNs, long readRpcTimeoutNs, long writeRpcTimeoutNs,
      int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.conn = conn;
    this.tableName = tableName;
    this.pauseNs = pauseNs;
    this.maxAttempts = retries2Attempts(maxRetries);
    this.operationTimeoutNs = operationTimeoutNs;
    this.readRpcTimeoutNs = readRpcTimeoutNs;
    this.writeRpcTimeoutNs = writeRpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;

    this.actions = new ArrayList<>(actions.size());
    this.futures = new ArrayList<>(actions.size());
    this.action2Future = new IdentityHashMap<>(actions.size());
    for (int i = 0, n = actions.size(); i < n; i++) {
      Row rawAction = actions.get(i);
      Action action = new Action(rawAction, i);
      if (rawAction instanceof Append || rawAction instanceof Increment) {
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
          regionsSupplier.get().map(r -> "'" + r.loc.getRegionInfo().getRegionNameAsString() + "'")
              .collect(Collectors.joining(",", "[", "]"));
      LOG.warn("Process batch for " + regions + " in " + tableName + " from " + serverName
          + " failed, tries=" + tries,
        error);
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
      List<CellScannable> cells) throws IOException {
    ClientProtos.MultiRequest.Builder multiRequestBuilder = ClientProtos.MultiRequest.newBuilder();
    ClientProtos.RegionAction.Builder regionActionBuilder = ClientProtos.RegionAction.newBuilder();
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    ClientProtos.MutationProto.Builder mutationBuilder = ClientProtos.MutationProto.newBuilder();
    for (Map.Entry<byte[], RegionRequest> entry : actionsByRegion.entrySet()) {
      // TODO: remove the extra for loop as we will iterate it in mutationBuilder.
      if (!multiRequestBuilder.hasNonceGroup()) {
        for (Action action : entry.getValue().actions) {
          if (action.hasNonce()) {
            multiRequestBuilder.setNonceGroup(conn.getNonceGenerator().getNonceGroup());
            break;
          }
        }
      }
      regionActionBuilder.clear();
      regionActionBuilder.setRegion(
        RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, entry.getKey()));
      regionActionBuilder = RequestConverter.buildNoDataRegionAction(entry.getKey(),
        entry.getValue().actions, cells, regionActionBuilder, actionBuilder, mutationBuilder);
      multiRequestBuilder.addRegionAction(regionActionBuilder.build());
    }
    return multiRequestBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private void onComplete(Action action, RegionRequest regionReq, int tries, ServerName serverName,
      RegionResult regionResult, List<Action> failedActions) {
    Object result = regionResult.result.get(action.getOriginalIndex());
    if (result == null) {
      LOG.error("Server " + serverName + " sent us neither result nor exception for row '"
          + Bytes.toStringBinary(action.getAction().getRow()) + "' of "
          + regionReq.loc.getRegionInfo().getRegionNameAsString());
      addError(action, new RuntimeException("Invalid response"), serverName);
      failedActions.add(action);
    } else if (result instanceof Throwable) {
      Throwable error = translateException((Throwable) result);
      logException(tries, () -> Stream.of(regionReq), error, serverName);
      if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
        failOne(action, tries, error, EnvironmentEdgeManager.currentTime(),
          getExtraContextForError(serverName));
      } else {
        failedActions.add(action);
      }
    } else {
      action2Future.get(action).complete((T) result);
    }
  }

  private void onComplete(Map<byte[], RegionRequest> actionsByRegion, int tries,
      ServerName serverName, MultiResponse resp) {
    List<Action> failedActions = new ArrayList<>();
    actionsByRegion.forEach((rn, regionReq) -> {
      RegionResult regionResult = resp.getResults().get(rn);
      if (regionResult != null) {
        regionReq.actions.forEach(
          action -> onComplete(action, regionReq, tries, serverName, regionResult, failedActions));
      } else {
        Throwable t = resp.getException(rn);
        Throwable error;
        if (t == null) {
          LOG.error(
            "Server sent us neither results nor exceptions for " + Bytes.toStringBinary(rn));
          error = new RuntimeException("Invalid response");
        } else {
          error = translateException(t);
          logException(tries, () -> Stream.of(regionReq), error, serverName);
          conn.getLocator().updateCachedLocation(regionReq.loc, error);
          if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
            failAll(regionReq.actions.stream(), tries, error, serverName);
            return;
          }
          addError(regionReq.actions, error, serverName);
          failedActions.addAll(regionReq.actions);
        }
      }
    });
    if (!failedActions.isEmpty()) {
      tryResubmit(failedActions.stream(), tries);
    }
  }

  private void send(Map<ServerName, ServerRequest> actionsByServer, int tries) {
    long remainingNs;
    if (operationTimeoutNs > 0) {
      remainingNs = remainingTimeNs();
      if (remainingNs <= 0) {
        failAll(actionsByServer.values().stream().flatMap(m -> m.actionsByRegion.values().stream())
            .flatMap(r -> r.actions.stream()),
          tries);
        return;
      }
    } else {
      remainingNs = Long.MAX_VALUE;
    }
    actionsByServer.forEach((sn, serverReq) -> {
      ClientService.Interface stub;
      try {
        stub = conn.getRegionServerStub(sn);
      } catch (IOException e) {
        onError(serverReq.actionsByRegion, tries, e, sn);
        return;
      }
      ClientProtos.MultiRequest req;
      List<CellScannable> cells = new ArrayList<>();
      try {
        req = buildReq(serverReq.actionsByRegion, cells);
      } catch (IOException e) {
        onError(serverReq.actionsByRegion, tries, e, sn);
        return;
      }
      HBaseRpcController controller = conn.rpcControllerFactory.newController();
      resetController(controller, Math.min(serverReq.rpcTimeoutNs.get(), remainingNs));
      if (!cells.isEmpty()) {
        controller.setCellScanner(createCellScanner(cells));
      }
      stub.multi(controller, req, resp -> {
        if (controller.failed()) {
          onError(serverReq.actionsByRegion, tries, controller.getFailed(), sn);
        } else {
          try {
            onComplete(serverReq.actionsByRegion, tries, sn,
              ResponseConverter.getResults(req, resp, controller.cellScanner()));
          } catch (Exception e) {
            onError(serverReq.actionsByRegion, tries, e, sn);
            return;
          }
        }
      });
    });
  }

  private void onError(Map<byte[], RegionRequest> actionsByRegion, int tries, Throwable t,
      ServerName serverName) {
    Throwable error = translateException(t);
    logException(tries, () -> actionsByRegion.values().stream(), error, serverName);
    if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
      failAll(actionsByRegion.values().stream().flatMap(r -> r.actions.stream()), tries, error,
        serverName);
      return;
    }
    List<Action> copiedActions = actionsByRegion.values().stream().flatMap(r -> r.actions.stream())
        .collect(Collectors.toList());
    addError(copiedActions, error, serverName);
    tryResubmit(copiedActions.stream(), tries);
  }

  private void tryResubmit(Stream<Action> actions, int tries) {
    long delayNs;
    if (operationTimeoutNs > 0) {
      long maxDelayNs = remainingTimeNs() - SLEEP_DELTA_NS;
      if (maxDelayNs <= 0) {
        failAll(actions, tries);
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNs, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNs, tries - 1);
    }
    retryTimer.newTimeout(t -> groupAndSend(actions, tries + 1), delayNs, TimeUnit.NANOSECONDS);
  }

  private long getRpcTimeoutNs(Action action) {
    return action.getAction() instanceof Get ? readRpcTimeoutNs : writeRpcTimeoutNs;
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
    // use the small one as the default timeout value, and increase the timeout value if we have an
    // action in the group needs a larger timeout value.
    long defaultRpcTimeoutNs;
    if (readRpcTimeoutNs > 0) {
      defaultRpcTimeoutNs =
          writeRpcTimeoutNs > 0 ? Math.min(readRpcTimeoutNs, writeRpcTimeoutNs) : readRpcTimeoutNs;
    } else {
      defaultRpcTimeoutNs = writeRpcTimeoutNs > 0 ? writeRpcTimeoutNs : -1L;
    }
    CompletableFuture.allOf(actions
        .map(action -> conn.getLocator().getRegionLocation(tableName, action.getAction().getRow(),
          RegionLocateType.CURRENT, locateTimeoutNs).whenComplete((loc, error) -> {
            if (error != null) {
              error = translateException(error);
              if (error instanceof DoNotRetryIOException) {
                failOne(action, tries, error, EnvironmentEdgeManager.currentTime(), "");
                return;
              }
              addError(action, error, null);
              locateFailed.add(action);
            } else {
              computeIfAbsent(actionsByServer, loc.getServerName(),
                () -> new ServerRequest(defaultRpcTimeoutNs)).addAction(loc, action,
                  getRpcTimeoutNs(action));
            }
          }))
        .toArray(CompletableFuture[]::new)).whenComplete((v, r) -> {
          if (!actionsByServer.isEmpty()) {
            send(actionsByServer, tries);
          }
          if (!locateFailed.isEmpty()) {
            tryResubmit(locateFailed.stream(), tries);
          }
        });
  }

  public List<CompletableFuture<T>> call() {
    groupAndSend(actions.stream(), 1);
    return futures;
  }
}
