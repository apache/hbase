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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface of the source that will export metrics about the ZooKeeper.
 */
@InterfaceAudience.Private
public interface MetricsZooKeeperSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "ZOOKEEPER";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "zookeeper";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about ZooKeeper";

  /**
   * The name of the metrics context that metrics will be under in jmx.
   */
  String METRICS_JMX_CONTEXT = "ZooKeeper,sub=" + METRICS_NAME;

  String EXCEPTION_AUTHFAILED = "AUTHFAILED Exception";
  String EXCEPTION_AUTHFAILED_DESC = "Number of failed ops due to an AUTHFAILED exception,";
  String EXCEPTION_CONNECTIONLOSS = "CONNECTIONLOSS Exception";
  String EXCEPTION_CONNECTIONLOSS_DESC = "Number of failed ops due to a CONNECTIONLOSS exception.";
  String EXCEPTION_DATAINCONSISTENCY = "DATAINCONSISTENCY Exception";
  String EXCEPTION_DATAINCONSISTENCY_DESC = "Number of failed ops due to a DATAINCONSISTENCY exception.";
  String EXCEPTION_INVALIDACL = "INVALIDACL Exception";
  String EXCEPTION_INVALIDACL_DESC = "Number of failed ops due to an INVALIDACL exception";
  String EXCEPTION_NOAUTH = "NOAUTH Exception";
  String EXCEPTION_NOAUTH_DESC = "Number of failed ops due to a NOAUTH exception.";
  String EXCEPTION_OPERATIONTIMEOUT = "OPERATIONTIMEOUT Exception";
  String EXCEPTION_OPERATIONTIMEOUT_DESC = "Number of failed ops due to an OPERATIONTIMEOUT exception.";
  String EXCEPTION_RUNTIMEINCONSISTENCY = "RUNTIMEINCONSISTENCY Exception";
  String EXCEPTION_RUNTIMEINCONSISTENCY_DESC = "Number of failed ops due to a RUNTIMEINCONSISTENCY exception.";
  String EXCEPTION_SESSIONEXPIRED = "SESSIONEXPIRED Exception";
  String EXCEPTION_SESSIONEXPIRED_DESC = "Number of failed ops due to a SESSIONEXPIRED exception.";
  String EXCEPTION_SYSTEMERROR = "SYSTEMERROR Exception";
  String EXCEPTION_SYSTEMERROR_DESC = "Number of failed ops due to a SYSTEMERROR exception.";
  String TOTAL_FAILED_ZK_CALLS = "TotalFailedZKCalls";
  String TOTAL_FAILED_ZK_CALLS_DESC = "Total number of failed ZooKeeper API Calls";

  String READ_OPERATION_LATENCY_NAME = "ReadOperationLatency";
  String READ_OPERATION_LATENCY_DESC = "Latency histogram for read operations.";
  String WRITE_OPERATION_LATENCY_NAME = "WriteOperationLatency";
  String WRITE_OPERATION_LATENCY_DESC = "Latency histogram for write operations.";
  String SYNC_OPERATION_LATENCY_NAME = "SyncOperationLatency";
  String SYNC_OPERATION_LATENCY_DESC = "Latency histogram for sync operations.";

  /**
   * Increment the count of failed ops due to AUTHFAILED Exception.
   */
  void incrementAuthFailedCount();

  /**
   * Increment the count of failed ops due to a CONNECTIONLOSS Exception.
   */
  void incrementConnectionLossCount();

  /**
   * Increment the count of failed ops due to a DATAINCONSISTENCY Exception.
   */
  void incrementDataInconsistencyCount();

  /**
   * Increment the count of failed ops due to INVALIDACL Exception.
   */
  void incrementInvalidACLCount();

  /**
   * Increment the count of failed ops due to NOAUTH Exception.
   */
  void incrementNoAuthCount();

  /**
   * Increment the count of failed ops due to an OPERATIONTIMEOUT Exception.
   */
  void incrementOperationTimeoutCount();

  /**
   * Increment the count of failed ops due to RUNTIMEINCONSISTENCY Exception.
   */
  void incrementRuntimeInconsistencyCount();

  /**
   * Increment the count of failed ops due to a SESSIONEXPIRED Exception.
   */
  void incrementSessionExpiredCount();

  /**
   * Increment the count of failed ops due to a SYSTEMERROR Exception.
   */
  void incrementSystemErrorCount();

  /**
   * Record the latency incurred for read operations.
   */
  void recordReadOperationLatency(long latency);

  /**
   * Record the latency incurred for write operations.
   */
  void recordWriteOperationLatency(long latency);

  /**
   * Record the latency incurred for sync operations.
   */
  void recordSyncOperationLatency(long latency);

  /**
   * Record the total number of failed ZooKeeper API calls.
   */
  void incrementTotalFailedZKCalls();
}
