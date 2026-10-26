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

import org.apache.hadoop.hbase.metrics.ExceptionTrackingSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsHBaseServerSource extends ExceptionTrackingSource {
  String AUTHORIZATION_SUCCESSES_NAME = "authorizationSuccesses";
  String AUTHORIZATION_SUCCESSES_DESC = "Number of authorization successes.";
  String AUTHORIZATION_FAILURES_NAME = "authorizationFailures";
  String AUTHORIZATION_FAILURES_DESC = "Number of authorization failures.";
  String AUTHENTICATION_SUCCESSES_NAME = "authenticationSuccesses";
  String AUTHENTICATION_SUCCESSES_DESC = "Number of authentication successes.";
  String AUTHENTICATION_FAILURES_NAME = "authenticationFailures";
  String AUTHENTICATION_FAILURES_DESC = "Number of authentication failures.";
  String AUTHENTICATION_FALLBACKS_NAME = "authenticationFallbacks";
  String AUTHENTICATION_FALLBACKS_DESC = "Number of fallbacks to insecure authentication.";
  String SENT_BYTES_NAME = "sentBytes";
  String SENT_BYTES_DESC = "Number of bytes sent.";
  String RECEIVED_BYTES_NAME = "receivedBytes";
  String RECEIVED_BYTES_DESC = "Number of bytes received.";
  String REQUEST_SIZE_NAME = "requestSize";
  String REQUEST_SIZE_DESC = "Request size in bytes.";
  String RESPONSE_SIZE_NAME = "responseSize";
  String RESPONSE_SIZE_DESC = "Response size in bytes.";
  String QUEUE_CALL_TIME_NAME = "queueCallTime";
  String QUEUE_CALL_TIME_DESC = "Queue Call Time.";
  String PROCESS_CALL_TIME_NAME = "processCallTime";
  String PROCESS_CALL_TIME_DESC = "Processing call time.";
  String TOTAL_CALL_TIME_NAME = "totalCallTime";
  String TOTAL_CALL_TIME_DESC = "Total call time, including both queued and processing time.";
  String QUEUE_READ_CALL_TIME_NAME = "queueReadCallTime";
  String QUEUE_READ_CALL_TIME_DESC = "Queue read call time.";
  String PROCESS_READ_CALL_TIME_NAME = "processReadCallTime";
  String PROCESS_READ_CALL_TIME_DESC = "Process read call time.";
  String TOTAL_READ_CALL_TIME_NAME = "totalReadCallTime";
  String TOTAL_READ_CALL_TIME_DESC =
    "Total read call time, including both queued and processing time.";
  String QUEUE_WRITE_CALL_TIME_NAME = "queueWriteCallTime";
  String QUEUE_WRITE_CALL_TIME_DESC = "Queue write call time.";
  String PROCESS_WRITE_CALL_TIME_NAME = "processWriteCallTime";
  String PROCESS_WRITE_CALL_TIME_DESC = "Process write call time.";
  String TOTAL_WRITE_CALL_TIME_NAME = "totalWriteCallTime";
  String TOTAL_WRITE_CALL_TIME_DESC =
    "Total write call time, including both queued and processing time.";
  String QUEUE_SCAN_CALL_TIME_NAME = "queueScanCallTime";
  String QUEUE_SCAN_CALL_TIME_DESC = "Queue scan call time.";
  String PROCESS_SCAN_CALL_TIME_NAME = "processScanCallTime";
  String PROCESS_SCAN_CALL_TIME_DESC = "Process scan call time.";
  String TOTAL_SCAN_CALL_TIME_NAME = "totalScanCallTime";
  String TOTAL_SCAN_CALL_TIME_DESC =
    "Total scan call time, including both queued and processing time.";

  String UNWRITABLE_TIME_NAME = "unwritableTime";
  String UNWRITABLE_TIME_DESC =
    "Time where an channel was unwritable due to having too many outbound bytes";
  String MAX_OUTBOUND_BYTES_EXCEEDED_NAME = "maxOutboundBytesExceeded";
  String MAX_OUTBOUND_BYTES_EXCEEDED_DESC =
    "Number of times a connection was closed because the channel outbound "
      + "bytes exceeded the configured max.";
  String QUEUE_SIZE_NAME = "queueSize";
  String QUEUE_SIZE_DESC = "Number of bytes in the call queues; request has been read and "
    + "parsed and is waiting to run or is currently being executed.";
  String GENERAL_QUEUE_NAME = "numCallsInGeneralQueue";
  String GENERAL_QUEUE_DESC = "Number of calls in the general call queue; "
    + "parsed requests waiting in scheduler to be executed";
  String PRIORITY_QUEUE_NAME = "numCallsInPriorityQueue";
  String METAPRIORITY_QUEUE_NAME = "numCallsInMetaPriorityQueue";
  String REPLICATION_QUEUE_NAME = "numCallsInReplicationQueue";
  String REPLICATION_QUEUE_DESC = "Number of calls in the replication call queue waiting to be run";
  String BULKLOAD_QUEUE_NAME = "numCallsInBulkLoadQueue";
  String BULKLOAD_QUEUE_DESC = "Number of calls in the bulkload call queue waiting to be run";
  String PRIORITY_QUEUE_DESC = "Number of calls in the priority call queue waiting to be run";
  String METAPRIORITY_QUEUE_DESC = "Number of calls in the priority call queue waiting to be run";
  String WRITE_QUEUE_NAME = "numCallsInWriteQueue";
  String WRITE_QUEUE_DESC = "Number of calls in the write call queue; "
    + "parsed requests waiting in scheduler to be executed";
  String READ_QUEUE_NAME = "numCallsInReadQueue";
  String READ_QUEUE_DESC = "Number of calls in the read call queue; "
    + "parsed requests waiting in scheduler to be executed";
  String SCAN_QUEUE_NAME = "numCallsInScanQueue";
  String SCAN_QUEUE_DESC = "Number of calls in the scan call queue; "
    + "parsed requests waiting in scheduler to be executed";
  String NUM_OPEN_CONNECTIONS_NAME = "numOpenConnections";
  String NUM_OPEN_CONNECTIONS_DESC = "Number of open connections.";
  String NUM_ACTIVE_HANDLER_NAME = "numActiveHandler";
  String NUM_ACTIVE_HANDLER_DESC = "Total number of active rpc handlers.";
  String NUM_ACTIVE_GENERAL_HANDLER_NAME = "numActiveGeneralHandler";
  String NUM_ACTIVE_GENERAL_HANDLER_DESC = "Number of active general rpc handlers.";
  String NUM_ACTIVE_PRIORITY_HANDLER_NAME = "numActivePriorityHandler";
  String NUM_ACTIVE_PRIORITY_HANDLER_DESC = "Number of active priority rpc handlers.";
  String NUM_ACTIVE_REPLICATION_HANDLER_NAME = "numActiveReplicationHandler";
  String NUM_ACTIVE_REPLICATION_HANDLER_DESC = "Number of active replication rpc handlers.";
  String NUM_ACTIVE_BULKLOAD_HANDLER_NAME = "numActiveBulkLoadHandler";
  String NUM_ACTIVE_BULKLOAD_HANDLER_DESC = "Number of active bulkload rpc handlers.";
  String NUM_ACTIVE_WRITE_HANDLER_NAME = "numActiveWriteHandler";
  String NUM_ACTIVE_WRITE_HANDLER_DESC = "Number of active write rpc handlers.";
  String NUM_ACTIVE_READ_HANDLER_NAME = "numActiveReadHandler";
  String NUM_ACTIVE_READ_HANDLER_DESC = "Number of active read rpc handlers.";
  String NUM_ACTIVE_SCAN_HANDLER_NAME = "numActiveScanHandler";
  String NUM_ACTIVE_SCAN_HANDLER_DESC = "Number of active scan rpc handlers.";
  String NUM_GENERAL_CALLS_DROPPED_NAME = "numGeneralCallsDropped";
  String NUM_GENERAL_CALLS_DROPPED_DESC =
    "Total number of calls in general queue which " + "were dropped by CoDel RPC executor";
  String NUM_LIFO_MODE_SWITCHES_NAME = "numLifoModeSwitches";
  String NUM_LIFO_MODE_SWITCHES_DESC =
    "Total number of calls in general queue which " + "were served from the tail of the queue";
  // Direct Memory Usage metrics
  String NETTY_DM_USAGE_NAME = "nettyDirectMemoryUsage";

  String NETTY_DM_USAGE_DESC = "Current Netty direct memory usage.";
  String NETTY_TOTAL_PENDING_OUTBOUND_NAME = "nettyTotalPendingOutboundBytes";
  String NETTY_TOTAL_PENDING_OUTBOUND_DESC = "Current total bytes pending write to all channel";
  String NETTY_MAX_PENDING_OUTBOUND_NAME = "nettyMaxPendingOutboundBytes";
  String NETTY_MAX_PENDING_OUTBOUND_DESC = "Current maximum bytes pending write to any channel";

  void authorizationSuccess();

  void authorizationFailure();

  void authenticationSuccess();

  void authenticationFailure();

  void authenticationFallback();

  void sentBytes(long count);

  void receivedBytes(int count);

  void sentResponse(long count);

  void receivedRequest(long count);

  void dequeuedCall(int qTime);

  void processedCall(int processingTime);

  void queuedAndProcessedCall(int totalTime);

  void unwritableTime(long unwritableTime);

  void maxOutboundBytesExceeded();

  void dequeuedReadCall(int qTime);

  void processReadCall(int processingTime);

  void queuedAndProcessedReadCall(int totalTime);

  void dequeuedWriteCall(int qTime);

  void processWriteCall(int processingTime);

  void queuedAndProcessedWriteCall(int totalTime);

  void dequeuedScanCall(int qTime);

  void processScanCall(int processingTime);

  void queuedAndProcessedScanCall(int totalTime);
}
