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
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Represents an Async Hbase call and its response.
 *
 * Responses are passed on to its given doneHandler and failures to the rpcController
 *
 * @param <T> Type of message returned
 * @param <M> Message returned in communication to be converted
 */
@InterfaceAudience.Private
public class AsyncCall<M extends Message, T> extends Promise<T> {
  private static final Log LOG = LogFactory.getLog(AsyncCall.class.getName());

  final int id;

  private final AsyncRpcChannelImpl channel;

  final Descriptors.MethodDescriptor method;
  final Message param;
  final Message responseDefaultType;

  private final MessageConverter<M,T> messageConverter;
  final long startTime;
  final long rpcTimeout;
  private final IOExceptionConverter exceptionConverter;

  // For only the request
  private final CellScanner cellScanner;
  private final int priority;

  final MetricsConnection.CallStats callStats;

  /**
   * Constructor
   *
   * @param channel             which initiated call
   * @param connectId           connection id
   * @param md                  the method descriptor
   * @param param               parameters to send to Server
   * @param cellScanner         cellScanner containing cells to send as request
   * @param responseDefaultType the default response type
   * @param messageConverter    converts the messages to what is the expected output
   * @param rpcTimeout          timeout for this call in ms
   * @param priority            for this request
   */
  public AsyncCall(AsyncRpcChannelImpl channel, int connectId, Descriptors.MethodDescriptor
        md, Message param, CellScanner cellScanner, M responseDefaultType, MessageConverter<M, T>
        messageConverter, IOExceptionConverter exceptionConverter, long rpcTimeout, int priority,
      MetricsConnection.CallStats callStats) {
    super(channel.getEventExecutor());
    this.channel = channel;

    this.id = connectId;

    this.method = md;
    this.param = param;
    this.responseDefaultType = responseDefaultType;

    this.messageConverter = messageConverter;
    this.exceptionConverter = exceptionConverter;

    this.startTime = EnvironmentEdgeManager.currentTime();
    this.rpcTimeout = rpcTimeout;

    this.priority = priority;
    this.cellScanner = cellScanner;

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
  public void setSuccess(M value, CellScanner cellBlockScanner) {
    if (LOG.isTraceEnabled()) {
      long callTime = EnvironmentEdgeManager.currentTime() - startTime;
      LOG.trace("Call: " + method.getName() + ", callTime: " + callTime + "ms");
    }

    try {
      this.setSuccess(
          this.messageConverter.convert(value, cellBlockScanner)
      );
    } catch (IOException e) {
      this.setFailed(e);
    }
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

    if (this.exceptionConverter != null) {
      exception = this.exceptionConverter.convert(exception);
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


  /**
   * @return Priority for this call
   */
  public int getPriority() {
    return priority;
  }

  /**
   * Get the cellScanner for this request.
   * @return CellScanner
   */
  public CellScanner cellScanner() {
    return cellScanner;
  }

  @Override
  public boolean cancel(boolean mayInterupt){
    this.channel.removePendingCall(this.id);
    return super.cancel(mayInterupt);
  }

}
