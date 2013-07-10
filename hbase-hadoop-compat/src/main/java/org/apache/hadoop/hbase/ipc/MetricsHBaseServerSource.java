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

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsHBaseServerSource extends BaseSource {
  public static final String AUTHORIZATION_SUCCESSES_NAME = "authorizationSuccesses";
  public static final String AUTHORIZATION_SUCCESSES_DESC =
      "Number of authorization successes.";
  public static final String AUTHORIZATION_FAILURES_NAME = "authorizationFailures";
  public static final String AUTHORIZATION_FAILURES_DESC =
      "Number of authorization failures.";
  public static final String AUTHENTICATION_SUCCESSES_NAME = "authenticationSuccesses";
  public static final String AUTHENTICATION_SUCCESSES_DESC =
      "Number of authentication successes.";
  public static final String AUTHENTICATION_FAILURES_NAME = "authenticationFailures";
  public static final String AUTHENTICATION_FAILURES_DESC =
      "Number of authentication failures.";
  public static final String SENT_BYTES_NAME = "sentBytes";
  public static final String SENT_BYTES_DESC = "Number of bytes sent.";
  public static final String RECEIVED_BYTES_NAME = "receivedBytes";
  public static final String RECEIVED_BYTES_DESC = "Number of bytes received.";
  public static final String QUEUE_CALL_TIME_NAME = "queueCallTime";
  public static final String QUEUE_CALL_TIME_DESC = "Queue Call Time.";
  public static final String PROCESS_CALL_TIME_NAME = "processCallTime";
  public static final String PROCESS_CALL_TIME_DESC = "Processing call time.";
  public static final String QUEUE_SIZE_NAME = "queueSize";
  public static final String QUEUE_SIZE_DESC = "Number of bytes in the call queues.";
  public static final String GENERAL_QUEUE_NAME = "numCallsInGeneralQueue";
  public static final String GENERAL_QUEUE_DESC = "Number of calls in the general call queue.";
  public static final String PRIORITY_QUEUE_NAME = "numCallsInPriorityQueue";
  public static final String REPLICATION_QUEUE_NAME = "numCallsInReplicationQueue";
  public static final String REPLICATION_QUEUE_DESC =
      "Number of calls in the replication call queue.";
  public static final String PRIORITY_QUEUE_DESC = "Number of calls in the priority call queue.";
  public static final String NUM_OPEN_CONNECTIONS_NAME = "numOpenConnections";
  public static final String NUM_OPEN_CONNECTIONS_DESC = "Number of open connections.";

  void authorizationSuccess();

  void authorizationFailure();

  void authenticationSuccess();

  void authenticationFailure();

  void sentBytes(int count);

  void receivedBytes(int count);

  void dequeuedCall(int qTime);

  void processedCall(int processingTime);
}
