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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

/**
 * A non-blocking implementation of RpcChannel that hedges requests to multiple service end points.
 * First received response is returned to the caller. This abstracts out the logic needed to batch
 * requests to multiple end points underneath and presents itself as a single logical RpcChannel to
 * the client.
 *
 * Hedging Details:
 * ---------------
 * - Hedging of RPCs happens in multiple batches. In each iteration, we select a 'batch' of address
 * end points to make the call to. We do multiple iterations until we get a proper response to the
 * rpc call or all the service addresses are exhausted, which ever happens first. Size of each is
 * configurable and is also known as 'fanOutSize'.
 *
 * - We randomize the addresses up front so that the batch order per client is non deterministic.
 * This avoids hot spots on the service side. The size of each batch is controlled via 'fanOutSize'.
 * Higher fanOutSize implies we make more rpc calls in a single batch. One needs to mindful of the
 * load on the client and server side when configuring the fan out.
 *
 * - In a happy case, once we receive a response from one end point, we cancel all the
 * other inflight rpcs in the same batch and return the response to the caller. If we do not get a
 * valid response from any address end point, we propagate the error back to the caller.
 *
 * - Rpc timeouts are applied to every hedged rpc.
 *
 * - Callers need to be careful about what rpcs they are trying to hedge. Not every kind of call can
 * be hedged (for example: cluster state changing rpcs).
 *
 * (TODO) Retries and Adaptive hedging policy:
 * ------------------------------------------
 *
 * - No retries are handled at the channel level. Retries can be built in upper layers. However the
 * question is, do we even need retries? Hedging in fact is a substitute for retries.
 *
 * - Clearly hedging puts more load on the service side. To mitigate this, we can make the hedging
 * policy more adaptive. In most happy cases, the rpcs from the first few end points should return
 * right away (especially short lived rpcs, that do not take up much time). In such cases, hedging
 * is not needed. So, the idea is to make this request pattern pluggable so that the requests are
 * hedged only when needed.
 */
class HedgedRpcChannel implements RpcChannel {
  private static final Logger LOG = LoggerFactory.getLogger(HedgedRpcChannel.class);

  /**
   * Currently hedging is only supported for non-blocking connection implementation types because
   * the channel implementation inherently relies on the connection implementation being async.
   * Refer to the comments in doCallMethod().
   */
  private final NettyRpcClient rpcClient;
  // List of service addresses to hedge the requests to. These are chunked by 'fanOutSize'
  private final List<List<InetSocketAddress>> addrs;
  private final User ticket;
  private final int rpcTimeout;

  /**
   * A simple rpc call back implementation to notify the batch context if any rpc is successful.
   */
  private static class BatchRpcCtxCallBack implements RpcCallback<Message> {
    private  final BatchRpcCtx batchRpcCtx;
    private final HBaseRpcController rpcController;
    BatchRpcCtxCallBack(BatchRpcCtx batchRpcCtx, HBaseRpcController rpcController) {
      this.batchRpcCtx = batchRpcCtx;
      this.rpcController = rpcController;
    }
    @Override
    public void run(Message result) {
      batchRpcCtx.setResultIfNotSet(result, rpcController);
    }
  }

  /**
   * Shared Call state across all batches.
   */
  class CallDetails {
    private final Descriptors.MethodDescriptor method;
    private final RpcController controller;
    private final Message request;
    private final Message responsePrototype;
    // Queued batches of addresses that are attempted. Each batch, at the end of it's run kicks off
    // the next batch by popping one from this queue.
    private Queue<List<InetSocketAddress>> targetAddrs;

    CallDetails(Descriptors.MethodDescriptor method, RpcController controller, Message request,
        Message responsePrototype, Queue<List<InetSocketAddress>> targetAddrs) {
      this.method = method;
      this.controller = controller;
      this.request = request;
      this.responsePrototype = responsePrototype;
      this.targetAddrs = targetAddrs;
    }

    List<InetSocketAddress> popNextBatch() {
      return targetAddrs.poll();
    }

    Descriptors.MethodDescriptor getMethod() {
      return method;
    }

    RpcController getOriginalController() {
      return controller;
    }

    HBaseRpcController getController() {
      return applyRpcTimeout(controller);
    }

    Message getRequest() {
      return request;
    }

    Message getResponsePrototype() {
      return responsePrototype;
    }

    private HBaseRpcController applyRpcTimeout(RpcController controller) {
      HBaseRpcController hBaseRpcController = (HBaseRpcController) controller;
      int rpcTimeoutToSet =
          hBaseRpcController.hasCallTimeout() ? hBaseRpcController.getCallTimeout() : rpcTimeout;
      HBaseRpcController response = new HBaseRpcControllerImpl();
      response.setCallTimeout(rpcTimeoutToSet);
      return response;
    }
  }

  /**
   * A shared RPC context between a batch of hedged RPCs. Tracks the state and helpers needed to
   * synchronize on multiple RPCs to different end points fetching the result. All the methods are
   * thread-safe.
   */
  private class BatchRpcCtx {
    // Result set by the thread finishing first. Set only once.
    private final AtomicReference<Message> result = new AtomicReference<>();
    // Caller waits on this latch being set.
    // We set this to 1, so that the first successful RPC result is returned to the client.
    private CountDownLatch resultsReady = new CountDownLatch(1);
    // Failed rpc book-keeping.
    private AtomicInteger failedRpcCount = new AtomicInteger();
    // All the call handles for this batch.
    private final List<Call> callsInFlight = Collections.synchronizedList(new ArrayList<>());

    // Target addresses.
    private final List<InetSocketAddress> addresses;
    // Called when the result is ready.
    private final RpcCallback<Message> callBack;
    // Last failed rpc's exception. Used to propagate the reason to the controller.
    private IOException lastFailedRpcReason;
    private final CallDetails callDetails;

    BatchRpcCtx(CallDetails callDetails, List<InetSocketAddress> addresses,
        RpcCallback<Message> callBack) {
      this.addresses = addresses;
      this.callBack = Preconditions.checkNotNull(callBack);
      this.callDetails = callDetails;
    }

    /**
     * Sets the result only if it is not already set by another thread. Thread that successfully
     * sets the result also count downs the latch.
     * @param result Result to be set.
     */
    void setResultIfNotSet(Message result, HBaseRpcController rpcController) {
      if (rpcController.failed()) {
        LOG.debug("Failed :" + rpcController.getFailed());
        incrementFailedRpcs(rpcController.getFailed());
        return;
      }
      if (this.result.compareAndSet(null, result)) {
        LOG.debug("Calling with result: " + this.result.get());
        callBack.run(this.result.get());
        // Cancel all pending in flight calls.
        for (Call call: callsInFlight) {
          // It is ok to do it for all calls as it is a no-op if the call is already done.
          final String exceptionMsg = String.format("%s canceled because another hedged attempt " +
              "for the same rpc already succeeded. This is not needed anymore.", call);
          call.setException(new CallCancelledException(exceptionMsg));
        }
      }
    }

    void addCallInFlight(Call c) {
      callsInFlight.add(c);
    }

    void incrementFailedRpcs(IOException reason) {
      if (failedRpcCount.incrementAndGet() == addresses.size()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed RPCs for batch: " + toString());
        }
        lastFailedRpcReason = reason;
        if (!triggerBatch(rpcClient, callDetails, callBack)) {
          //  All batches finished. Mark the RPC call as failed.
          callDetails.getOriginalController().setFailed(lastFailedRpcReason.getMessage());
          callBack.run(null);
        }
      }
    }

    @Override
    public String toString() {
      return String.format("Batched rpc for target(s) %s", PrettyPrinter.toString(addresses));
    }
  }

  HedgedRpcChannel(NettyRpcClient rpcClient, Set<InetSocketAddress> addrs,
      User ticket, int rpcTimeout, int fanOutSize) {
    this.rpcClient = rpcClient;
    Preconditions.checkArgument(Preconditions.checkNotNull(addrs).size() >= 1);
    List addrList = new ArrayList<>(addrs);
    // For non-deterministic client query pattern. Not all clients want to hedge RPCs in the same
    // order, creating hot spots on the service end points.
    Collections.shuffle(addrList);
    this.addrs = Lists.partition(addrList, fanOutSize);
    this.ticket = ticket;
    this.rpcTimeout = rpcTimeout;
  }

  /**
   * Triggers a batch of async RPCs with the given callDetails. Returns true if a new batch has been
   * started, false otherwise.
   */
  private boolean triggerBatch(NettyRpcClient rpcClient,
      CallDetails callDetails, RpcCallback<Message> done) {
    List<InetSocketAddress> targetAddrs = callDetails.popNextBatch();
    if (targetAddrs == null) {
      return false;
    }
    BatchRpcCtx batchRpcCtx = new BatchRpcCtx(callDetails, targetAddrs, done);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting RPCs to batch: " + batchRpcCtx.toString());
    }
    for (InetSocketAddress address: batchRpcCtx.addresses) {
      // ** WARN ** This is a blocking call if the underlying connection for the rpc client is
      // a blocking implementation (ex: BlockingRpcConnection). That essentially serializes all
      // the write calls. Handling blocking connection means that this should be run in a separate
      // thread and hence more code complexity. Is it ok to handle only non-blocking connections?
      HBaseRpcController controller = callDetails.getController();
      batchRpcCtx.addCallInFlight(rpcClient.callMethod(callDetails.getMethod(),
          controller, callDetails.getRequest(), callDetails.getResponsePrototype(),
          ticket, address, new BatchRpcCtxCallBack(batchRpcCtx, controller)));
    }
    return true;
  }

  @Override
  public void callMethod(Descriptors.MethodDescriptor method, RpcController controller,
      Message request, Message responsePrototype, RpcCallback<Message> done) {
    Queue<List<InetSocketAddress>> batchedAddrs = new ConcurrentLinkedQueue<>(this.addrs);
    CallDetails callDetails =
        new CallDetails(method, controller, request, responsePrototype, batchedAddrs);
    Preconditions.checkState(triggerBatch(rpcClient, callDetails, done));
  }
}
