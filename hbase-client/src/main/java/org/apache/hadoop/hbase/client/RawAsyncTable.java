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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A low level asynchronous table.
 * <p>
 * The implementation is required to be thread safe.
 * <p>
 * The returned {@code CompletableFuture} will be finished directly in the rpc framework's callback
 * thread, so typically you should not do any time consuming work inside these methods, otherwise
 * you will be likely to block at least one connection to RS(even more if the rpc framework uses
 * NIO).
 * <p>
 * So, only experts that want to build high performance service should use this interface directly,
 * especially for the {@link #scan(Scan, RawScanResultConsumer)} below.
 * <p>
 * TODO: For now the only difference between this interface and {@link AsyncTable} is the scan
 * method. The {@link RawScanResultConsumer} exposes the implementation details of a scan(heartbeat)
 * so it is not suitable for a normal user. If it is still the only difference after we implement
 * most features of AsyncTable, we can think about merge these two interfaces.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface RawAsyncTable extends AsyncTableBase {

  /**
   * The basic scan API uses the observer pattern. All results that match the given scan object will
   * be passed to the given {@code consumer} by calling {@code RawScanResultConsumer.onNext}.
   * {@code RawScanResultConsumer.onComplete} means the scan is finished, and
   * {@code RawScanResultConsumer.onError} means we hit an unrecoverable error and the scan is
   * terminated. {@code RawScanResultConsumer.onHeartbeat} means the RS is still working but we can
   * not get a valid result to call {@code RawScanResultConsumer.onNext}. This is usually because
   * the matched results are too sparse, for example, a filter which almost filters out everything
   * is specified.
   * <p>
   * Notice that, the methods of the given {@code consumer} will be called directly in the rpc
   * framework's callback thread, so typically you should not do any time consuming work inside
   * these methods, otherwise you will be likely to block at least one connection to RS(even more if
   * the rpc framework uses NIO).
   * @param scan A configured {@link Scan} object.
   * @param consumer the consumer used to receive results.
   */
  void scan(Scan scan, RawScanResultConsumer consumer);

  /**
   * Delegate to a protobuf rpc call.
   * <p>
   * Usually, it is just a simple lambda expression, like:
   *
   * <pre>
   * <code>
   * (stub, controller, rpcCallback) -> {
   *   XXXRequest request = ...; // prepare the request
   *   stub.xxx(controller, request, rpcCallback);
   * }
   * </code>
   * </pre>
   *
   * And if you can prepare the {@code request} before calling the coprocessorService method, the
   * lambda expression will be:
   *
   * <pre>
   * <code>
   * (stub, controller, rpcCallback) -> stub.xxx(controller, request, rpcCallback)
   * </code>
   * </pre>
   */
  @InterfaceAudience.Public
  @FunctionalInterface
  interface CoprocessorCallable<S, R> {

    /**
     * Represent the actual protobuf rpc call.
     * @param stub the asynchronous stub
     * @param controller the rpc controller, has already been prepared for you
     * @param rpcCallback the rpc callback, has already been prepared for you
     */
    void call(S stub, RpcController controller, RpcCallback<R> rpcCallback);
  }

  /**
   * Execute the given coprocessor call on the region which contains the given {@code row}.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code newStub} call. Usually it is only a
   * one line lambda expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   *
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link CoprocessorCallable} for more details.
   * @param row The row key used to identify the remote region location
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see CoprocessorCallable
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable, byte[] row);

  /**
   * The callback when we want to execute a coprocessor call on a range of regions.
   * <p>
   * As the locating itself also takes some time, the implementation may want to send rpc calls on
   * the fly, which means we do not know how many regions we have when we get the return value of
   * the rpc calls, so we need an {@link #onComplete()} which is used to tell you that we have
   * passed all the return values to you(through the {@link #onRegionComplete(HRegionInfo, Object)}
   * or {@link #onRegionError(HRegionInfo, Throwable)} calls), i.e, there will be no
   * {@link #onRegionComplete(HRegionInfo, Object)} or
   * {@link #onRegionError(HRegionInfo, Throwable)} calls in the future.
   * <p>
   * Here is a pseudo code to describe a typical implementation of a range coprocessor service
   * method to help you better understand how the {@link CoprocessorCallback} will be called. The
   * {@code callback} in the pseudo code is our {@link CoprocessorCallback}. And notice that the
   * {@code whenComplete} is {@code CompletableFuture.whenComplete}.
   *
   * <pre>
   * locateThenCall(byte[] row) {
   *   locate(row).whenComplete((location, locateError) -> {
   *     if (locateError != null) {
   *       callback.onError(locateError);
   *       return;
   *     }
   *     incPendingCall();
   *     region = location.getRegion();
   *     if (region.getEndKey() > endKey) {
   *       locateEnd = true;
   *     } else {
   *       locateThenCall(region.getEndKey());
   *     }
   *     sendCall().whenComplete((resp, error) -> {
   *       if (error != null) {
   *         callback.onRegionError(region, error);
   *       } else {
   *         callback.onRegionComplete(region, resp);
   *       }
   *       if (locateEnd && decPendingCallAndGet() == 0) {
   *         callback.onComplete();
   *       }
   *     });
   *   });
   * }
   * </pre>
   */
  @InterfaceAudience.Public
  interface CoprocessorCallback<R> {

    /**
     * @param region the region that the response belongs to
     * @param resp the response of the coprocessor call
     */
    void onRegionComplete(HRegionInfo region, R resp);

    /**
     * @param region the region that the error belongs to
     * @param error the response error of the coprocessor call
     */
    void onRegionError(HRegionInfo region, Throwable error);

    /**
     * Indicate that all responses of the regions have been notified by calling
     * {@link #onRegionComplete(HRegionInfo, Object)} or
     * {@link #onRegionError(HRegionInfo, Throwable)}.
     */
    void onComplete();

    /**
     * Indicate that we got an error which does not belong to any regions. Usually a locating error.
     */
    void onError(Throwable error);
  }

  /**
   * Execute the given coprocessor call on the regions which are covered by the range from
   * {@code startKey} inclusive and {@code endKey} exclusive. See the comment of
   * {@link #coprocessorService(Function, CoprocessorCallable, byte[], boolean, byte[], boolean, CoprocessorCallback)}
   * for more details.
   * @see #coprocessorService(Function, CoprocessorCallable, byte[], boolean, byte[], boolean,
   *      CoprocessorCallback)
   */
  default <S, R> void coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable, byte[] startKey, byte[] endKey,
      CoprocessorCallback<R> callback) {
    coprocessorService(stubMaker, callable, startKey, true, endKey, false, callback);
  }

  /**
   * Execute the given coprocessor call on the regions which are covered by the range from
   * {@code startKey} and {@code endKey}. The inclusive of boundaries are specified by
   * {@code startKeyInclusive} and {@code endKeyInclusive}. The {@code stubMaker} is just a
   * delegation to the {@code xxxService.newStub} call. Usually it is only a one line lambda
   * expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   *
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link CoprocessorCallable} for more details.
   * @param startKey start region selection with region containing this row. If {@code null}, the
   *          selection will start with the first table region.
   * @param startKeyInclusive whether to include the startKey
   * @param endKey select regions up to and including the region containing this row. If
   *          {@code null}, selection will continue through the last table region.
   * @param endKeyInclusive whether to include the endKey
   * @param callback callback to get the response. See the comment of {@link CoprocessorCallback}
   *          for more details.
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @see CoprocessorCallable
   * @see CoprocessorCallback
   */
  <S, R> void coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable, byte[] startKey, boolean startKeyInclusive, byte[] endKey,
      boolean endKeyInclusive, CoprocessorCallback<R> callback);
}
