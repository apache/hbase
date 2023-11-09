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
package org.apache.hadoop.hbase.namequeues;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * RpcCall details that would be passed on to ring buffer of slow log responses
 */
@InterfaceAudience.Private
public class RpcLogDetails extends NamedQueuePayload {

  public static final int SLOW_LOG_EVENT = 0;

  private static final Logger LOG = LoggerFactory.getLogger(RpcLogDetails.class.getName());

  private final RpcCall rpcCall;
  private Message param;
  private final String clientAddress;
  private final long responseSize;
  private final String className;
  private final boolean isSlowLog;
  private final boolean isLargeLog;

  public RpcLogDetails(RpcCall rpcCall, Message param, String clientAddress, long responseSize,
    String className, boolean isSlowLog, boolean isLargeLog) {
    super(SLOW_LOG_EVENT);
    this.rpcCall = rpcCall;
    this.clientAddress = clientAddress;
    this.responseSize = responseSize;
    this.className = className;
    this.isSlowLog = isSlowLog;
    this.isLargeLog = isLargeLog;

    // We need to deep copy the message because the CodedInputStream may be
    // overwritten before this slow log is consumed. Such overwriting could
    // cause the slow log payload to be corrupt
    try {
      this.param = param.newBuilderForType().mergeFrom(param.toByteArray()).build();
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Failed to parse protobuf for message {}", param, e);
      this.param = param;
    }
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

  public boolean isSlowLog() {
    return isSlowLog;
  }

  public boolean isLargeLog() {
    return isLargeLog;
  }

  public Message getParam() {
    return param;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("rpcCall", rpcCall).append("param", param)
      .append("clientAddress", clientAddress).append("responseSize", responseSize)
      .append("className", className).append("isSlowLog", isSlowLog)
      .append("isLargeLog", isLargeLog).toString();
  }
}
