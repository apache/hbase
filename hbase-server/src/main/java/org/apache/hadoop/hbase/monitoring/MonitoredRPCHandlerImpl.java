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
package org.apache.hadoop.hbase.monitoring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * A MonitoredTask implementation designed for use with RPC Handlers handling frequent, short
 * duration tasks. String concatenations and object allocations are avoided in methods that will be
 * hit by every RPC call.
 */
@InterfaceAudience.Private
public class MonitoredRPCHandlerImpl extends MonitoredTaskImpl implements MonitoredRPCHandler {
  private String clientAddress;
  private int remotePort;
  private long rpcQueueTime;
  private long rpcStartTime;
  private String methodName = "";
  private Object[] params = {};
  private Message packet;
  private boolean snapshot = false;
  private Map<String, Object> callInfoMap = new HashMap<>();

  public MonitoredRPCHandlerImpl() {
    super();
    // in this implementation, WAITING indicates that the handler is not
    // actively servicing an RPC call.
    setState(State.WAITING);
  }

  @Override
  public synchronized MonitoredRPCHandlerImpl clone() {
    MonitoredRPCHandlerImpl clone = (MonitoredRPCHandlerImpl) super.clone();
    clone.callInfoMap = generateCallInfoMap();
    clone.snapshot = true;
    return clone;
  }

  /**
   * Gets the status of this handler; if it is currently servicing an RPC, this status will include
   * the RPC information.
   * @return a String describing the current status.
   */
  @Override
  public String getStatus() {
    if (getState() != State.RUNNING) {
      return super.getStatus();
    }
    return super.getStatus() + " from " + getClient() + ": " + getRPC();
  }

  /**
   * Accesses the queue time for the currently running RPC on the monitored Handler.
   * @return the queue timestamp or -1 if there is no RPC currently running.
   */
  @Override
  public long getRPCQueueTime() {
    if (getState() != State.RUNNING) {
      return -1;
    }
    return rpcQueueTime;
  }

  /**
   * Accesses the start time for the currently running RPC on the monitored Handler.
   * @return the start timestamp or -1 if there is no RPC currently running.
   */
  @Override
  public long getRPCStartTime() {
    if (getState() != State.RUNNING) {
      return -1;
    }
    return rpcStartTime;
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @return a string representing the method call without parameters
   */
  @Override
  public synchronized String getRPC() {
    return getRPC(false);
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @param withParams toggle inclusion of parameters in the RPC String
   * @return A human-readable string representation of the method call.
   */
  @Override
  public synchronized String getRPC(boolean withParams) {
    if (getState() != State.RUNNING) {
      // no RPC is currently running
      return "";
    }
    StringBuilder buffer = new StringBuilder(256);
    buffer.append(methodName);
    if (withParams) {
      buffer.append("(");
      for (int i = 0; i < params.length; i++) {
        if (i != 0) buffer.append(", ");
        buffer.append(params[i]);
      }
      buffer.append(")");
    }
    return buffer.toString();
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @return A human-readable string representation of the method call.
   */
  @Override
  public long getRPCPacketLength() {
    if (getState() != State.RUNNING || packet == null) {
      // no RPC is currently running, or we don't have an RPC's packet info
      return -1L;
    }
    return packet.getSerializedSize();
  }

  /**
   * If an RPC call is currently running, produces a String representation of the connection from
   * which it was received.
   * @return A human-readable string representation of the address and port of the client.
   */
  @Override
  public String getClient() {
    return clientAddress + ":" + remotePort;
  }

  /**
   * Indicates to the client whether this task is monitoring a currently active RPC call.
   * @return true if the monitored handler is currently servicing an RPC call.
   */
  @Override
  public boolean isRPCRunning() {
    return getState() == State.RUNNING;
  }

  /**
   * Indicates to the client whether this task is monitoring a currently active RPC call to a
   * database command. (as defined by o.a.h.h.client.Operation)
   * @return true if the monitored handler is currently servicing an RPC call to a database command.
   */
  @Override
  public synchronized boolean isOperationRunning() {
    if (!isRPCRunning()) {
      return false;
    }
    for (Object param : params) {
      if (param instanceof Operation) {
        return true;
      }
    }
    return false;
  }

  /**
   * Tells this instance that it is monitoring a new RPC call.
   * @param methodName The name of the method that will be called by the RPC.
   * @param params     The parameters that will be passed to the indicated method.
   */
  @Override
  public synchronized void setRPC(String methodName, Object[] params, long queueTime) {
    this.methodName = methodName;
    this.params = params;
    long now = System.currentTimeMillis();
    this.rpcStartTime = now;
    setWarnTime(now);
    this.rpcQueueTime = queueTime;
    this.state = State.RUNNING;
  }

  /**
   * Gives this instance a reference to the protobuf received by the RPC, so that it can later
   * compute its size if asked for it.
   * @param param The protobuf received by the RPC for this call
   */
  @Override
  public void setRPCPacket(Message param) {
    this.packet = param;
  }

  /**
   * Registers current handler client details.
   * @param clientAddress the address of the current client
   * @param remotePort    the port from which the client connected
   */
  @Override
  public void setConnection(String clientAddress, int remotePort) {
    this.clientAddress = clientAddress;
    this.remotePort = remotePort;
  }

  @Override
  public synchronized void markComplete(String status) {
    super.markComplete(status);
    this.params = null;
    this.packet = null;
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    return this.snapshot ? this.callInfoMap : generateCallInfoMap();
  }

  private Map<String, Object> generateCallInfoMap() {
    // only include RPC info if the Handler is actively servicing an RPC call
    Map<String, Object> map = super.toMap();
    if (getState() != State.RUNNING) {
      return map;
    }
    Map<String, Object> rpcJSON = new HashMap<>();
    ArrayList paramList = new ArrayList();
    map.put("rpcCall", rpcJSON);
    rpcJSON.put("queuetimems", getRPCQueueTime());
    rpcJSON.put("starttimems", getRPCStartTime());
    rpcJSON.put("clientaddress", clientAddress);
    rpcJSON.put("remoteport", remotePort);
    rpcJSON.put("packetlength", getRPCPacketLength());
    rpcJSON.put("method", methodName);
    rpcJSON.put("params", paramList);
    for (Object param : params) {
      if (param instanceof byte[]) {
        paramList.add(Bytes.toStringBinary((byte[]) param));
      } else if (param instanceof Operation) {
        paramList.add(((Operation) param).toMap());
      } else {
        paramList.add(param.toString());
      }
    }
    return map;
  }

  @Override
  public String toString() {
    if (getState() != State.RUNNING) {
      return super.toString();
    }
    return super.toString() + ", queuetimems=" + getRPCQueueTime() + ", starttimems="
      + getRPCStartTime() + ", clientaddress=" + clientAddress + ", remoteport=" + remotePort
      + ", packetlength=" + getRPCPacketLength() + ", rpcMethod=" + getRPC();
  }
}
