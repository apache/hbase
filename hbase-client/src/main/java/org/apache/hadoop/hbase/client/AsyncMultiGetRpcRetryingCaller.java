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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MultiResponse.RegionResult;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Retry caller for multi get.
 * <p>
 * Notice that, the {@link #operationTimeoutNs} is the total time limit now which is the same with
 * other single operations
 * <p>
 * And the {@link #maxAttempts} is a limit for each single get in the batch logically. In the
 * implementation, we will record a {@code tries} parameter for each operation group, and if it is
 * split to several groups when retrying, the sub groups will inherit {@code tries}. You can imagine
 * that the whole retrying process is a tree, and the {@link #maxAttempts} is the limit of the depth
 * of the tree.
 */
@InterfaceAudience.Private
class AsyncMultiGetRpcRetryingCaller {

  private static final Log LOG = LogFactory.getLog(AsyncMultiGetRpcRetryingCaller.class);

  private final HashedWheelTimer retryTimer;

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final List<Get> gets;

  private final List<CompletableFuture<Result>> futures;

  private final IdentityHashMap<Get, CompletableFuture<Result>> get2Future;

  private final IdentityHashMap<Get, List<ThrowableWithExtraContext>> get2Errors;

  private final long pauseNs;

  private final int maxAttempts;

  private final long operationTimeoutNs;

  private final long rpcTimeoutNs;

  private final int startLogErrorsCnt;

  private final long startNs;

  // we can not use HRegionLocation as the map key because the hashCode and equals method of
  // HRegionLocation only consider serverName.
  private static final class RegionRequest {

    public final HRegionLocation loc;

    public final ConcurrentLinkedQueue<Get> gets = new ConcurrentLinkedQueue<>();

    public RegionRequest(HRegionLocation loc) {
      this.loc = loc;
    }
  }

  public AsyncMultiGetRpcRetryingCaller(HashedWheelTimer retryTimer, AsyncConnectionImpl conn,
      TableName tableName, List<Get> gets, long pauseNs, int maxRetries, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.conn = conn;
    this.tableName = tableName;
    this.gets = gets;
    this.pauseNs = pauseNs;
    this.maxAttempts = retries2Attempts(maxRetries);
    this.operationTimeoutNs = operationTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;

    this.futures = new ArrayList<>(gets.size());
    this.get2Future = new IdentityHashMap<>(gets.size());
    gets.forEach(
      get -> futures.add(get2Future.computeIfAbsent(get, k -> new CompletableFuture<>())));
    this.get2Errors = new IdentityHashMap<>();
    this.startNs = System.nanoTime();
  }

  private long remainingTimeNs() {
    return operationTimeoutNs - (System.nanoTime() - startNs);
  }

  private List<ThrowableWithExtraContext> removeErrors(Get get) {
    synchronized (get2Errors) {
      return get2Errors.remove(get);
    }
  }

  private void logException(int tries, Supplier<Stream<RegionRequest>> regionsSupplier,
      Throwable error, ServerName serverName) {
    if (tries > startLogErrorsCnt) {
      String regions =
          regionsSupplier.get().map(r -> "'" + r.loc.getRegionInfo().getRegionNameAsString() + "'")
              .collect(Collectors.joining(",", "[", "]"));
      LOG.warn("Get data for " + regions + " in " + tableName + " from " + serverName
          + " failed, tries=" + tries,
        error);
    }
  }

  private String getExtras(ServerName serverName) {
    return serverName != null ? serverName.getServerName() : "";
  }

  private void addError(Get get, Throwable error, ServerName serverName) {
    List<ThrowableWithExtraContext> errors;
    synchronized (get2Errors) {
      errors = get2Errors.computeIfAbsent(get, k -> new ArrayList<>());
    }
    errors.add(new ThrowableWithExtraContext(error, EnvironmentEdgeManager.currentTime(),
        serverName != null ? serverName.toString() : ""));
  }

  private void addError(Iterable<Get> gets, Throwable error, ServerName serverName) {
    gets.forEach(get -> addError(get, error, serverName));
  }

  private void failOne(Get get, int tries, Throwable error, long currentTime, String extras) {
    CompletableFuture<Result> future = get2Future.get(get);
    if (future.isDone()) {
      return;
    }
    ThrowableWithExtraContext errorWithCtx =
        new ThrowableWithExtraContext(error, currentTime, extras);
    List<ThrowableWithExtraContext> errors = removeErrors(get);
    if (errors == null) {
      errors = Collections.singletonList(errorWithCtx);
    } else {
      errors.add(errorWithCtx);
    }
    future.completeExceptionally(new RetriesExhaustedException(tries, errors));
  }

  private void failAll(Stream<Get> gets, int tries, Throwable error, ServerName serverName) {
    long currentTime = System.currentTimeMillis();
    String extras = getExtras(serverName);
    gets.forEach(get -> failOne(get, tries, error, currentTime, extras));
  }

  private void failAll(Stream<Get> gets, int tries) {
    gets.forEach(get -> {
      CompletableFuture<Result> future = get2Future.get(get);
      if (future.isDone()) {
        return;
      }
      future.completeExceptionally(new RetriesExhaustedException(tries,
          Optional.ofNullable(removeErrors(get)).orElse(Collections.emptyList())));
    });
  }

  private ClientProtos.MultiRequest buildReq(Map<byte[], RegionRequest> getsByRegion)
      throws IOException {
    ClientProtos.MultiRequest.Builder multiRequestBuilder = ClientProtos.MultiRequest.newBuilder();
    for (Map.Entry<byte[], RegionRequest> entry : getsByRegion.entrySet()) {
      ClientProtos.RegionAction.Builder regionActionBuilder =
          ClientProtos.RegionAction.newBuilder().setRegion(
            RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, entry.getKey()));
      int index = 0;
      for (Get get : entry.getValue().gets) {
        regionActionBuilder.addAction(
          ClientProtos.Action.newBuilder().setIndex(index).setGet(ProtobufUtil.toGet(get)));
        index++;
      }
      multiRequestBuilder.addRegionAction(regionActionBuilder);
    }
    return multiRequestBuilder.build();
  }

  private void onComplete(Map<byte[], RegionRequest> getsByRegion, int tries, ServerName serverName,
      MultiResponse resp) {
    List<Get> failedGets = new ArrayList<>();
    getsByRegion.forEach((rn, regionReq) -> {
      RegionResult regionResult = resp.getResults().get(rn);
      if (regionResult != null) {
        int index = 0;
        for (Get get : regionReq.gets) {
          Object result = regionResult.result.get(index);
          if (result == null) {
            LOG.error("Server sent us neither result nor exception for row '"
                + Bytes.toStringBinary(get.getRow()) + "' of " + Bytes.toStringBinary(rn));
            addError(get, new RuntimeException("Invalid response"), serverName);
            failedGets.add(get);
          } else if (result instanceof Throwable) {
            Throwable error = translateException((Throwable) result);
            logException(tries, () -> Stream.of(regionReq), error, serverName);
            if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
              failOne(get, tries, error, EnvironmentEdgeManager.currentTime(),
                getExtras(serverName));
            } else {
              failedGets.add(get);
            }
          } else {
            get2Future.get(get).complete((Result) result);
          }
          index++;
        }
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
            failAll(regionReq.gets.stream(), tries, error, serverName);
            return;
          }
          addError(regionReq.gets, error, serverName);
          failedGets.addAll(regionReq.gets);
        }
      }
    });
    if (!failedGets.isEmpty()) {
      tryResubmit(failedGets.stream(), tries);
    }
  }

  private void send(Map<ServerName, ? extends Map<byte[], RegionRequest>> getsByServer, int tries) {
    long callTimeoutNs;
    if (operationTimeoutNs > 0) {
      long remainingNs = remainingTimeNs();
      if (remainingNs <= 0) {
        failAll(getsByServer.values().stream().flatMap(m -> m.values().stream())
            .flatMap(r -> r.gets.stream()),
          tries);
        return;
      }
      callTimeoutNs = Math.min(remainingNs, rpcTimeoutNs);
    } else {
      callTimeoutNs = rpcTimeoutNs;
    }
    getsByServer.forEach((sn, getsByRegion) -> {
      ClientService.Interface stub;
      try {
        stub = conn.getRegionServerStub(sn);
      } catch (IOException e) {
        onError(getsByRegion, tries, e, sn);
        return;
      }
      ClientProtos.MultiRequest req;
      try {
        req = buildReq(getsByRegion);
      } catch (IOException e) {
        onError(getsByRegion, tries, e, sn);
        return;
      }
      HBaseRpcController controller = conn.rpcControllerFactory.newController();
      resetController(controller, callTimeoutNs);
      stub.multi(controller, req, resp -> {
        if (controller.failed()) {
          onError(getsByRegion, tries, controller.getFailed(), sn);
        } else {
          try {
            onComplete(getsByRegion, tries, sn,
              ResponseConverter.getResults(req, resp, controller.cellScanner()));
          } catch (Exception e) {
            onError(getsByRegion, tries, e, sn);
            return;
          }
        }
      });
    });
  }

  private void onError(Map<byte[], RegionRequest> getsByRegion, int tries, Throwable t,
      ServerName serverName) {
    Throwable error = translateException(t);
    logException(tries, () -> getsByRegion.values().stream(), error, serverName);
    if (error instanceof DoNotRetryIOException || tries >= maxAttempts) {
      failAll(getsByRegion.values().stream().flatMap(r -> r.gets.stream()), tries, error,
        serverName);
      return;
    }
    List<Get> copiedGets =
        getsByRegion.values().stream().flatMap(r -> r.gets.stream()).collect(Collectors.toList());
    addError(copiedGets, error, serverName);
    tryResubmit(copiedGets.stream(), tries);
  }

  private void tryResubmit(Stream<Get> gets, int tries) {
    long delayNs;
    if (operationTimeoutNs > 0) {
      long maxDelayNs = remainingTimeNs() - SLEEP_DELTA_NS;
      if (maxDelayNs <= 0) {
        failAll(gets, tries);
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNs, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNs, tries - 1);
    }
    retryTimer.newTimeout(t -> groupAndSend(gets, tries + 1), delayNs, TimeUnit.NANOSECONDS);
  }

  private void groupAndSend(Stream<Get> gets, int tries) {
    long locateTimeoutNs;
    if (operationTimeoutNs > 0) {
      locateTimeoutNs = remainingTimeNs();
      if (locateTimeoutNs <= 0) {
        failAll(gets, tries);
        return;
      }
    } else {
      locateTimeoutNs = -1L;
    }
    ConcurrentMap<ServerName, ConcurrentMap<byte[], RegionRequest>> getsByServer =
        new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<Get> locateFailed = new ConcurrentLinkedQueue<>();
    CompletableFuture.allOf(gets.map(get -> conn.getLocator()
        .getRegionLocation(tableName, get.getRow(), locateTimeoutNs).whenComplete((loc, error) -> {
          if (error != null) {
            error = translateException(error);
            if (error instanceof DoNotRetryIOException) {
              failOne(get, tries, error, EnvironmentEdgeManager.currentTime(), "");
              return;
            }
            addError(get, error, null);
            locateFailed.add(get);
          } else {
            ConcurrentMap<byte[], RegionRequest> getsByRegion = computeIfAbsent(getsByServer,
              loc.getServerName(), () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
            computeIfAbsent(getsByRegion, loc.getRegionInfo().getRegionName(),
              () -> new RegionRequest(loc)).gets.add(get);
          }
        })).toArray(CompletableFuture[]::new)).whenComplete((v, r) -> {
          if (!getsByServer.isEmpty()) {
            send(getsByServer, tries);
          }
          if (!locateFailed.isEmpty()) {
            tryResubmit(locateFailed.stream(), tries);
          }
        });
  }

  public List<CompletableFuture<Result>> call() {
    groupAndSend(gets.stream(), 1);
    return futures;
  }
}
