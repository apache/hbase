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

package org.apache.hadoop.hbase.namequeues;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * RpcCall details that would be passed on to ring buffer of slow log responses
 */
@InterfaceAudience.Private
public class RpcLogDetails extends NamedQueuePayload {

  private final Descriptors.MethodDescriptor methodDescriptor;
  private final Message param;
  private final String clientAddress;
  private final long responseSize;
  private final String className;
  private final boolean isSlowLog;
  private final boolean isLargeLog;
  private final long receiveTime;
  private final long startTime;
  private final String userName;

  public RpcLogDetails(Descriptors.MethodDescriptor methodDescriptor, Message param,
      String clientAddress, long responseSize, String className, boolean isSlowLog,
      boolean isLargeLog, long receiveTime, long startTime, String userName) {
    super(NamedQueueEvent.SLOW_LOG);
    this.methodDescriptor = methodDescriptor;
    this.param = param;
    this.clientAddress = clientAddress;
    this.responseSize = responseSize;
    this.className = className;
    this.isSlowLog = isSlowLog;
    this.isLargeLog = isLargeLog;
    this.receiveTime = receiveTime;
    this.startTime = startTime;
    this.userName = userName;
  }

  public Descriptors.MethodDescriptor getMethodDescriptor() {
    return methodDescriptor;
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

  public long getReceiveTime() {
    return receiveTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("methodDescriptor", methodDescriptor)
      .append("param", param)
      .append("clientAddress", clientAddress)
      .append("responseSize", responseSize)
      .append("className", className)
      .append("isSlowLog", isSlowLog)
      .append("isLargeLog", isLargeLog)
      .append("receiveTime", receiveTime)
      .append("startTime", startTime)
      .append("userName", userName)
      .toString();
  }
}
