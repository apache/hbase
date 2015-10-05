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
package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.DefaultPromise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;

/**
 * Represents an Async Hbase call and its response.
 *
 * Responses are passed on to its given doneHandler and failures to the rpcController
 */
@InterfaceAudience.Private
public class AsyncCall extends DefaultPromise<Message> {
  private static final Log LOG = LogFactory.getLog(AsyncCall.class.getName());

  final int id;

  final Descriptors.MethodDescriptor method;
  final Message param;
  final PayloadCarryingRpcController controller;
  final Message responseDefaultType;
  final long startTime;
  final long rpcTimeout;
  final MetricsConnection.CallStats callStats;

  /**
   * Constructor
   *
   * @param eventLoop           for call
   * @param connectId           connection id
   * @param md                  the method descriptor
   * @param param               parameters to send to Server
   * @param controller          controller for response
   * @param responseDefaultType the default response type
   */
  public AsyncCall(EventLoop eventLoop, int connectId, Descriptors.MethodDescriptor md, Message
      param, PayloadCarryingRpcController controller, Message responseDefaultType,
      MetricsConnection.CallStats callStats) {
    super(eventLoop);

    this.id = connectId;

    this.method = md;
    this.param = param;
    this.controller = controller;
    this.responseDefaultType = responseDefaultType;

    this.startTime = EnvironmentEdgeManager.currentTime();
    this.rpcTimeout = controller.hasCallTimeout() ? controller.getCallTimeout() : 0;
    this.callStats = callStats;
  }

  /**
   * Get the start time
   *
   * @return start time for the call
   */
  public long getStartTime() {
    return this.startTime;
  }

  @Override
  public String toString() {
    return "callId=" + this.id + ", method=" + this.method.getName() +
      ", rpcTimeout=" + this.rpcTimeout + ", param {" +
      (this.param != null ? ProtobufUtil.getShortTextFormat(this.param) : "") + "}";
  }

  /**
   * Set success with a cellBlockScanner
   *
   * @param value            to set
   * @param cellBlockScanner to set
   */
  public void setSuccess(Message value, CellScanner cellBlockScanner) {
    if (cellBlockScanner != null) {
      controller.setCellScanner(cellBlockScanner);
    }

    if (LOG.isTraceEnabled()) {
      long callTime = EnvironmentEdgeManager.currentTime() - startTime;
      LOG.trace("Call: " + method.getName() + ", callTime: " + callTime + "ms");
    }

    this.setSuccess(value);
  }

  /**
   * Set failed
   *
   * @param exception to set
   */
  public void setFailed(IOException exception) {
    if (ExceptionUtil.isInterrupt(exception)) {
      exception = ExceptionUtil.asInterrupt(exception);
    }
    if (exception instanceof RemoteException) {
      exception = ((RemoteException) exception).unwrapRemoteException();
    }

    this.setFailure(exception);
  }

  /**
   * Get the rpc timeout
   *
   * @return current timeout for this call
   */
  public long getRpcTimeout() {
    return rpcTimeout;
  }
}
