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

import org.apache.yetus.audience.InterfaceAudience;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@InterfaceAudience.Private
public class CallQueueInfo {
  private final Map<String, Map<String, Long>> callQueueMethodCountsSummary;
  private final Map<String, Map<String, Long>> callQueueMethodSizeSummary;

  CallQueueInfo() {
    callQueueMethodCountsSummary = new HashMap<>();
    callQueueMethodSizeSummary = new HashMap<>();
  }

  public Set<String> getCallQueueNames() {
    return callQueueMethodCountsSummary.keySet();
  }

  public Set<String> getCalledMethodNames(String callQueueName) {
    return callQueueMethodCountsSummary.get(callQueueName).keySet();
  }

  public long getCallMethodCount(String callQueueName, String methodName) {
    long methodCount;

    Map<String, Long> methodCountMap = callQueueMethodCountsSummary.getOrDefault(callQueueName, null);

    if (null != methodCountMap) {
      methodCount = methodCountMap.getOrDefault(methodName, 0L);
    } else {
      methodCount = 0L;
    }

    return methodCount;
  }

  void setCallMethodCount(String callQueueName, Map<String, Long> methodCountMap) {
    callQueueMethodCountsSummary.put(callQueueName, methodCountMap);
  }

  public long getCallMethodSize(String callQueueName, String methodName) {
    long methodSize;

    Map<String, Long> methodSizeMap = callQueueMethodSizeSummary.getOrDefault(callQueueName, null);

    if (null != methodSizeMap) {
      methodSize = methodSizeMap.getOrDefault(methodName, 0L);
    } else {
      methodSize = 0L;
    }

    return methodSize;
  }

  void setCallMethodSize(String callQueueName, Map<String, Long> methodSizeMap) {
    callQueueMethodSizeSummary.put(callQueueName, methodSizeMap);
  }

}
