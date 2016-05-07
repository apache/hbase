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

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/** A call waiting for a value. */
@InterfaceAudience.Private
public class Call {
  final int id;                                 // call id
  final Message param;                          // rpc request method param object
  /**
   * Optionally has cells when making call.  Optionally has cells set on response.  Used
   * passing cells to the rpc and receiving the response.
   */
  CellScanner cells;
  Message response;                             // value, null if error
  // The return type.  Used to create shell into which we deserialize the response if any.
  Message responseDefaultType;
  IOException error;                            // exception, null if value
  volatile boolean done;                                 // true when call is done
  final Descriptors.MethodDescriptor md;
  final int timeout; // timeout in millisecond for this call; 0 means infinite.
  final MetricsConnection.CallStats callStats;

  protected Call(int id, final Descriptors.MethodDescriptor md, Message param,
      final CellScanner cells, final Message responseDefaultType, int timeout,
      MetricsConnection.CallStats callStats) {
    this.param = param;
    this.md = md;
    this.cells = cells;
    this.callStats = callStats;
    this.callStats.setStartTime(EnvironmentEdgeManager.currentTime());
    this.responseDefaultType = responseDefaultType;
    this.id = id;
    this.timeout = timeout;
  }

  /**
   * Check if the call did timeout. Set an exception (includes a notify) if it's the case.
   * @return true if the call is on timeout, false otherwise.
   */
  public boolean checkAndSetTimeout() {
    if (timeout == 0){
      return false;
    }

    long waitTime = EnvironmentEdgeManager.currentTime() - getStartTime();
    if (waitTime >= timeout) {
      IOException ie = new CallTimeoutException("Call id=" + id +
          ", waitTime=" + waitTime + ", operationTimeout=" + timeout + " expired.");
      setException(ie); // includes a notify
      return true;
    } else {
      return false;
    }
  }

  public int remainingTime() {
    if (timeout == 0) {
      return Integer.MAX_VALUE;
    }

    int remaining = timeout - (int) (EnvironmentEdgeManager.currentTime() - getStartTime());
    return remaining > 0 ? remaining : 0;
  }

  @Override
  public String toString() {
    return "callId: " + this.id + " methodName: " + this.md.getName() + " param {" +
      (this.param != null? ProtobufUtil.getShortTextFormat(this.param): "") + "}";
  }

  /** Indicate when the call is complete and the
   * value or error are available.  Notifies by default.  */
  protected synchronized void callComplete() {
    this.done = true;
    notify();                                 // notify caller
  }

  /** Set the exception when there is an error.
   * Notify the caller the call is done.
   *
   * @param error exception thrown by the call; either local or remote
   */
  public void setException(IOException error) {
    this.error = error;
    callComplete();
  }

  /**
   * Set the return value when there is no error.
   * Notify the caller the call is done.
   *
   * @param response return value of the call.
   * @param cells Can be null
   */
  public void setResponse(Message response, final CellScanner cells) {
    this.response = response;
    this.cells = cells;
    callComplete();
  }

  public long getStartTime() {
    return this.callStats.getStartTime();
  }
}