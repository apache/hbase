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
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.AsyncProcess.DEFAULT_START_LOG_ERRORS_AFTER_COUNT;
import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_SIZE_KEY;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeout configs.
 */
@InterfaceAudience.Private
class AsyncConnectionConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncConnectionConfiguration.class);

  private final long metaOperationTimeoutNs;

  // timeout for a whole operation such as get, put or delete. Notice that scan will not be effected
  // by this value, see scanTimeoutNs.
  private final long operationTimeoutNs;

  // timeout for each rpc request. Can be overridden by a more specific config, such as
  // readRpcTimeout or writeRpcTimeout.
  private final long rpcTimeoutNs;

  // timeout for each read rpc request
  private final long readRpcTimeoutNs;

  // timeout for each write rpc request
  private final long writeRpcTimeoutNs;

  private final long pauseNs;

  private final long pauseForCQTBENs;

  private final int maxRetries;

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  // As now we have heartbeat support for scan, ideally a scan will never timeout unless the RS is
  // crash. The RS will always return something before the rpc timeout or scan timeout to tell the
  // client that it is still alive. The scan timeout is used as operation timeout for every
  // operations in a scan, such as openScanner or next.
  private final long scanTimeoutNs;

  private final int scannerCaching;

  private final int metaScannerCaching;

  private final long scannerMaxResultSize;

  private final long writeBufferSize;

  private final long writeBufferPeriodicFlushTimeoutNs;

  // this is for supporting region replica get, if the primary does not finished within this
  // timeout, we will send request to secondaries.
  private final long primaryCallTimeoutNs;

  private final long primaryScanTimeoutNs;

  private final long primaryMetaScanTimeoutNs;

  private final int maxKeyValueSize;

  AsyncConnectionConfiguration(Configuration conf) {
    this.metaOperationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.operationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong(HBASE_CLIENT_OPERATION_TIMEOUT, DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    long rpcTimeoutMs = conf.getLong(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT);
    this.rpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(rpcTimeoutMs);
    this.readRpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_READ_TIMEOUT_KEY, rpcTimeoutMs));
    this.writeRpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_WRITE_TIMEOUT_KEY, rpcTimeoutMs));
    long pauseMs = conf.getLong(HBASE_CLIENT_PAUSE, DEFAULT_HBASE_CLIENT_PAUSE);
    long pauseForCQTBEMs = conf.getLong(HBASE_CLIENT_PAUSE_FOR_CQTBE, pauseMs);
    if (pauseForCQTBEMs < pauseMs) {
      LOG.warn(
        "The {} setting: {} ms is less than the {} setting: {} ms, use the greater one instead",
        HBASE_CLIENT_PAUSE_FOR_CQTBE, pauseForCQTBEMs, HBASE_CLIENT_PAUSE, pauseMs);
      pauseForCQTBEMs = pauseMs;
    }
    this.pauseNs = TimeUnit.MILLISECONDS.toNanos(pauseMs);
    this.pauseForCQTBENs = TimeUnit.MILLISECONDS.toNanos(pauseForCQTBEMs);
    this.maxRetries = conf.getInt(HBASE_CLIENT_RETRIES_NUMBER, DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.startLogErrorsCnt =
      conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    this.scanTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
        conf.getInt(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
            DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
    this.scannerCaching =
      conf.getInt(HBASE_CLIENT_SCANNER_CACHING, DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    this.metaScannerCaching =
      conf.getInt(HBASE_META_SCANNER_CACHING, DEFAULT_HBASE_META_SCANNER_CACHING);
    this.scannerMaxResultSize = conf.getLong(HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.writeBufferSize = conf.getLong(WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);
    this.writeBufferPeriodicFlushTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS,
        WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT));
    this.primaryCallTimeoutNs = TimeUnit.MICROSECONDS.toNanos(
      conf.getLong(PRIMARY_CALL_TIMEOUT_MICROSECOND, PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT));
    this.primaryScanTimeoutNs = TimeUnit.MICROSECONDS.toNanos(
      conf.getLong(PRIMARY_SCAN_TIMEOUT_MICROSECOND, PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT));
    this.primaryMetaScanTimeoutNs =
      TimeUnit.MICROSECONDS.toNanos(conf.getLong(HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT,
        HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT));
    this.maxKeyValueSize = conf.getInt(MAX_KEYVALUE_SIZE_KEY, MAX_KEYVALUE_SIZE_DEFAULT);
  }

  long getMetaOperationTimeoutNs() {
    return metaOperationTimeoutNs;
  }

  long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  long getRpcTimeoutNs() {
    return rpcTimeoutNs;
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

  long getPauseForCQTBENs() {
    return pauseForCQTBENs;
  }

  int getMaxRetries() {
    return maxRetries;
  }

  int getStartLogErrorsCnt() {
    return startLogErrorsCnt;
  }

  long getScanTimeoutNs() {
    return scanTimeoutNs;
  }

  int getScannerCaching() {
    return scannerCaching;
  }

  int getMetaScannerCaching() {
    return metaScannerCaching;
  }

  long getScannerMaxResultSize() {
    return scannerMaxResultSize;
  }

  long getWriteBufferSize() {
    return writeBufferSize;
  }

  long getWriteBufferPeriodicFlushTimeoutNs() {
    return writeBufferPeriodicFlushTimeoutNs;
  }

  long getPrimaryCallTimeoutNs() {
    return primaryCallTimeoutNs;
  }

  long getPrimaryScanTimeoutNs() {
    return primaryScanTimeoutNs;
  }

  long getPrimaryMetaScanTimeoutNs() {
    return primaryMetaScanTimeoutNs;
  }

  int getMaxKeyValueSize() {
    return maxKeyValueSize;
  }
}
