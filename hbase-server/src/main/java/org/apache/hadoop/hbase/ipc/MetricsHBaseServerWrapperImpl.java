/**
 *
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

import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHBaseServerWrapperImpl implements MetricsHBaseServerWrapper {

  private RpcServer server;

  MetricsHBaseServerWrapperImpl(RpcServer server) {
    this.server = server;
  }

  private boolean isServerStarted() {
    return this.server != null && this.server.isStarted();
  }

  @Override
  public long getTotalQueueSize() {
    if (!isServerStarted()) {
      return 0;
    }
    return server.callQueueSizeInBytes.sum();
  }

  @Override
  public int getGeneralQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getGeneralQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getReplicationQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getPriorityQueueLength();
  }

  @Override
  public int getMetaPriorityQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getMetaPriorityQueueLength();
  }

  @Override
  public int getNumOpenConnections() {
    if (!isServerStarted()) {
      return 0;
    }
    return server.getNumOpenConnections();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveRpcHandlerCount();
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveGeneralRpcHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActivePriorityRpcHandlerCount();
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveMetaPriorityRpcHandlerCount();
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveReplicationRpcHandlerCount();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveWriteRpcHandlerCount();
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveReadRpcHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveScanRpcHandlerCount();
  }

  @Override
  public long getNettyDmUsage() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0L;
    }

    return DirectMemoryUtils.getNettyDirectMemoryUsage();
  }
}
