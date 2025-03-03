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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_SCANNER_TIMEOUT;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Timeout configs.
 */
@InterfaceAudience.Private
class AsyncConnectionConfiguration {

  /**
   * Configure the number of failures after which the client will start logging. A few failures is
   * fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
   * heuristic for the number of errors we don't log. 5 was chosen because we wait for 1s at this
   * stage.
   */
  public static final String START_LOG_ERRORS_AFTER_COUNT_KEY =
    "hbase.client.start.log.errors.counter";
  public static final int DEFAULT_START_LOG_ERRORS_AFTER_COUNT = 5;

  private final long metaOperationTimeoutNs;

  // timeout for a whole operation such as get, put or delete. Notice that scan will not be effected
  // by this value, see scanTimeoutNs.
  private final long operationTimeoutNs;

  // timeout for each rpc request. Can be overridden by a more specific config, such as
  // readRpcTimeout or writeRpcTimeout.
  private final long rpcTimeoutNs;

  // timeout for each read rpc request
  private final long readRpcTimeoutNs;

  // timeout for each read rpc request against system tables
  private final long metaReadRpcTimeoutNs;

  // timeout for each write rpc request
  private final long writeRpcTimeoutNs;

  private final long pauseNs;

  private final long pauseNsForServerOverloaded;

  private final int maxRetries;

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  // As now we have heartbeat support for scan, ideally a scan will never timeout unless the RS is
  // crash. The RS will always return something before the rpc timeout or scan timeout to tell the
  // client that it is still alive. The scan timeout is used as operation timeout for every
  // operations in a scan, such as openScanner or next.
  private final long scanTimeoutNs;
  private final long metaScanTimeoutNs;

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

  private final int bufferedMutatorMaxMutations;

  AsyncConnectionConfiguration(Configuration conf) {
    ConnectionConfiguration connectionConf = new ConnectionConfiguration(conf);

    // fields we can pull directly from connection configuration
    this.scannerCaching = connectionConf.getScannerCaching();
    this.scannerMaxResultSize = connectionConf.getScannerMaxResultSize();
    this.writeBufferSize = connectionConf.getWriteBufferSize();
    this.writeBufferPeriodicFlushTimeoutNs = connectionConf.getWriteBufferPeriodicFlushTimeoutMs();
    this.maxKeyValueSize = connectionConf.getMaxKeyValueSize();
    this.maxRetries = connectionConf.getRetriesNumber();
    this.bufferedMutatorMaxMutations = connectionConf.getBufferedMutatorMaxMutations();

    // fields from connection configuration that need to be converted to nanos
    this.metaOperationTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(connectionConf.getMetaOperationTimeout());
    this.operationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(connectionConf.getOperationTimeout());
    this.rpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(connectionConf.getRpcTimeout());
    long readRpcTimeoutMillis =
      conf.getLong(HBASE_RPC_READ_TIMEOUT_KEY, connectionConf.getRpcTimeout());
    this.readRpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(readRpcTimeoutMillis);
    this.metaReadRpcTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY, readRpcTimeoutMillis));
    this.writeRpcTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_RPC_WRITE_TIMEOUT_KEY, connectionConf.getWriteRpcTimeout()));
    this.pauseNs = TimeUnit.MILLISECONDS.toNanos(connectionConf.getPauseMillis());
    this.pauseNsForServerOverloaded =
      TimeUnit.MILLISECONDS.toNanos(connectionConf.getPauseMillisForServerOverloaded());
    this.primaryCallTimeoutNs =
      TimeUnit.MICROSECONDS.toNanos(connectionConf.getPrimaryCallTimeoutMicroSecond());
    this.primaryScanTimeoutNs =
      TimeUnit.MICROSECONDS.toNanos(connectionConf.getReplicaCallTimeoutMicroSecondScan());
    this.primaryMetaScanTimeoutNs =
      TimeUnit.MICROSECONDS.toNanos(connectionConf.getMetaReplicaCallTimeoutMicroSecondScan());
    long scannerTimeoutMillis = conf.getLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    this.scanTimeoutNs = TimeUnit.MILLISECONDS.toNanos(scannerTimeoutMillis);
    this.metaScanTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_CLIENT_META_SCANNER_TIMEOUT, scannerTimeoutMillis));

    // fields not in connection configuration
    this.startLogErrorsCnt =
      conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    this.metaScannerCaching =
      conf.getInt(HBASE_META_SCANNER_CACHING, DEFAULT_HBASE_META_SCANNER_CACHING);
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

  long getMetaReadRpcTimeoutNs() {
    return metaReadRpcTimeoutNs;
  }

  long getWriteRpcTimeoutNs() {
    return writeRpcTimeoutNs;
  }

  long getPauseNs() {
    return pauseNs;
  }

  long getPauseNsForServerOverloaded() {
    return pauseNsForServerOverloaded;
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

  long getMetaScanTimeoutNs() {
    return metaScanTimeoutNs;
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

  int getBufferedMutatorMaxMutations() {
    return bufferedMutatorMaxMutations;
  }
}
