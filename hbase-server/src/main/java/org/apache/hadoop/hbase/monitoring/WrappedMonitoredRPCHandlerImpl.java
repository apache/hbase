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

import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A wrap of MonitoredRPCHandlerImpl, it has the call info from the call param.
 * Please use this class to avoid read offheap param data.
 */
@InterfaceAudience.Private
public class WrappedMonitoredRPCHandlerImpl extends MonitoredRPCHandlerImpl
  implements MonitoredTask {
  private Map<String, Object> callInfoMap = new HashMap<>();
  private MonitoredRPCHandlerImpl monitoredRPCHandler;

  public WrappedMonitoredRPCHandlerImpl(MonitoredRPCHandlerImpl monitoredRPCHandler) {
    this.monitoredRPCHandler = monitoredRPCHandler;
  }

  public void setCallInfoMap(Map<String, Object> callInfoMap) {
    this.callInfoMap = callInfoMap;
  }

  @Override
  public long getStartTime() {
    return monitoredRPCHandler.getStartTime();
  }

  @Override
  public String getDescription() {
    return monitoredRPCHandler.getDescription();
  }

  @Override
  public String getStatus() {
    return monitoredRPCHandler.getStatus();
  }

  @Override
  public long getStatusTime() {
    return monitoredRPCHandler.getStatusTime();
  }

  @Override
  public State getState() {
    return monitoredRPCHandler.getState();
  }

  @Override
  public long getStateTime() {
    return monitoredRPCHandler.getStateTime();
  }

  @Override
  public long getCompletionTimestamp() {
    return monitoredRPCHandler.getCompletionTimestamp();
  }

  @Override
  public String getRPC() {
    return monitoredRPCHandler.getRPC(false);
  }

  @Override
  public String getRPC(boolean withParams) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRPCStartTime() {
    return monitoredRPCHandler.getRPCStartTime();
  }

  @Override
  public long getRPCQueueTime() {
    return monitoredRPCHandler.getRPCQueueTime();
  }

  @Override
  public boolean isOperationRunning() {
    return monitoredRPCHandler.isOperationRunning();
  }

  @Override
  public long getRPCPacketLength() {
    return monitoredRPCHandler.getRPCPacketLength();
  }

  @Override
  public String getClient() {
    return monitoredRPCHandler.getClient();
  }

  @Override
  public boolean isRPCRunning() {
    return monitoredRPCHandler.isRPCRunning();
  }

  @Override
  public Map<String, Object> toMap() {
    return callInfoMap;
  }

  @Override
  public String toString() {
    return monitoredRPCHandler.toString();
  }
}
