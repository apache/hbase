/*
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

package org.apache.hadoop.hbase.regionserver.slowlog;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * RpcCall details that would be passed on to ring buffer of slow log responses
 */
@InterfaceAudience.Private
public class RpcLogDetails {

  private RpcCall rpcCall;
  private String clientAddress;
  private long responseSize;
  private String className;

  public RpcLogDetails(RpcCall rpcCall, String clientAddress, long responseSize,
      String className) {
    this.rpcCall = rpcCall;
    this.clientAddress = clientAddress;
    this.responseSize = responseSize;
    this.className = className;
  }

  public RpcCall getRpcCall() {
    return rpcCall;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public long getResponseSize() {
    return responseSize;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("rpcCall", rpcCall)
      .append("clientAddress", clientAddress)
      .append("responseSize", responseSize)
      .append("className", className)
      .toString();
  }

}
