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

public class MetricsHBaseServerWrapperStub implements MetricsHBaseServerWrapper{
  @Override
  public long getTotalQueueSize() {
    return 101;
  }

  @Override
  public int getGeneralQueueLength() {
    return 102;
  }

  @Override
  public int getReplicationQueueLength() {
    return 103;
  }

  @Override
  public int getPriorityQueueLength() {
    return 104;
  }

  @Override
  public int getNumOpenConnections() {
    return 105;
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return 106;
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return 3;
  }

  @Override
  public long getNumLifoModeSwitches() {
    return 5;
  }

  @Override
  public int getWriteQueueLength() {
    return 50;
  }

  @Override
  public int getReadQueueLength() {
    return 50;
  }

  @Override
  public int getScanQueueLength() {
    return 2;
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return 50;
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return 50;
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return 6;
  }
}
