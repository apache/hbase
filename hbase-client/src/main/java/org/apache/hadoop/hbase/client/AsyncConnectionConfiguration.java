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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.AsyncProcess.DEFAULT_START_LOG_ERRORS_AFTER_COUNT;
import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Timeout configs.
 */
@InterfaceAudience.Private
class AsyncConnectionConfiguration {

  private final long metaOperationTimeoutNs;

  private final long operationTimeoutNs;

  // timeout for each read rpc request
  private final long readRpcTimeoutNs;

  // timeout for each write rpc request
  private final long writeRpcTimeoutNs;

  private final long pauseNs;

  private final int maxRetries;

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  AsyncConnectionConfiguration(Configuration conf) {
    this.metaOperationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.operationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong(HBASE_CLIENT_OPERATION_TIMEOUT, DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.readRpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_READ_TIMEOUT_KEY,
      conf.getLong(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT)));
    this.writeRpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_WRITE_TIMEOUT_KEY,
      conf.getLong(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT)));
    this.pauseNs = TimeUnit.MILLISECONDS
        .toNanos(conf.getLong(HBASE_CLIENT_PAUSE, DEFAULT_HBASE_CLIENT_PAUSE));
    this.maxRetries = conf.getInt(HBASE_CLIENT_RETRIES_NUMBER, DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.startLogErrorsCnt = conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY,
      DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
  }

  long getMetaOperationTimeoutNs() {
    return metaOperationTimeoutNs;
  }

  long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  long getReadRpcTimeoutNs() {
    return readRpcTimeoutNs;
  }

  long getWriteRpcTimeoutNs() {
    return writeRpcTimeoutNs;
  }

  long getPauseNs() {
    return pauseNs;
  }

  int getMaxRetries() {
    return maxRetries;
  }

  int getStartLogErrorsCnt() {
    return startLogErrorsCnt;
  }

}
