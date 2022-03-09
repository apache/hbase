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

public class DelegatingRpcScheduler extends RpcScheduler {
  protected RpcScheduler delegate;

  public DelegatingRpcScheduler(RpcScheduler delegate) {
    this.delegate = delegate;
  }

  @Override
  public void stop() {
    delegate.stop();
  }
  @Override
  public void start() {
    delegate.start();
  }
  @Override
  public void init(Context context) {
    delegate.init(context);
  }
  @Override
  public int getReplicationQueueLength() {
    return delegate.getReplicationQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    return delegate.getPriorityQueueLength();
  }

  @Override
  public int getGeneralQueueLength() {
    return delegate.getGeneralQueueLength();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return delegate.getActiveRpcHandlerCount();
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
    return delegate.getActiveGeneralRpcHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    return delegate.getActivePriorityRpcHandlerCount();
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    return delegate.getActiveReplicationRpcHandlerCount();
  }

  @Override
  public boolean dispatch(CallRunner task) {
    return delegate.dispatch(task);
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    return delegate.getActiveMetaPriorityRpcHandlerCount();
  }

  @Override
  public int getMetaPriorityQueueLength() {
    return delegate.getMetaPriorityQueueLength();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return delegate.getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
    return delegate.getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    return 0;
  }

  @Override
  public int getReadQueueLength() {
    return 0;
  }

  @Override
  public int getScanQueueLength() {
    return 0;
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return 0;
  }

  @Override
  public CallQueueInfo getCallQueueInfo() {
    return delegate.getCallQueueInfo();
  }
}
