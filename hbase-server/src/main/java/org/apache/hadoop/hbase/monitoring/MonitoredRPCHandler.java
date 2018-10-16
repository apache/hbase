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
package org.apache.hadoop.hbase.monitoring;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * A MonitoredTask implementation optimized for use with RPC Handlers 
 * handling frequent, short duration tasks. String concatenations and object 
 * allocations are avoided in methods that will be hit by every RPC call.
 */
@InterfaceAudience.Private
public interface MonitoredRPCHandler extends MonitoredTask {
  String getRPC();
  String getRPC(boolean withParams);
  long getRPCPacketLength();
  String getClient();
  long getRPCStartTime();
  long getRPCQueueTime();
  boolean isRPCRunning();
  boolean isOperationRunning();

  void setRPC(String methodName, Object[] params, long queueTime);
  void setRPCPacket(Message param);
  void setConnection(String clientAddress, int remotePort);
}
