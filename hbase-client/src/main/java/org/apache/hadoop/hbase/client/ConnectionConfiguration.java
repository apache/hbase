/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Configuration parameters for the connection.
 * Configuration is a heavy weight registry that does a lot of string operations and regex matching.
 * Method calls into Configuration account for high CPU usage and have huge performance impact.
 * This class caches connection-related configuration values in the  ConnectionConfiguration
 * object so that expensive conf.getXXX() calls are avoided every time HTable, etc is instantiated.
 * see HBASE-12128
 */
@InterfaceAudience.Private
public class ConnectionConfiguration {

  public static final String WRITE_BUFFER_SIZE_KEY = "hbase.client.write.buffer";
  public static final long WRITE_BUFFER_SIZE_DEFAULT = 2097152;
  public static final String WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS =
          "hbase.client.write.buffer.periodicflush.timeout.ms";
  public static final String WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS =
          "hbase.client.write.buffer.periodicflush.timertick.ms";
  public static final long WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT = 0; // 0 == Disabled
  public static final long WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS_DEFAULT = 1000L; // 1 second
  public static final String MAX_KEYVALUE_SIZE_KEY = "hbase.client.keyvalue.maxsize";
  public static final int MAX_KEYVALUE_SIZE_DEFAULT = 10485760;
  public static final String PRIMARY_CALL_TIMEOUT_MICROSECOND =
    "hbase.client.primaryCallTimeout.get";
  public static final int PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT = 10000; // 10ms
  public static final String PRIMARY_SCAN_TIMEOUT_MICROSECOND =
    "hbase.client.replicaCallTimeout.scan";
  public static final int PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT = 1000000; // 1s
  public static final String LOG_SCANNER_ACTIVITY = "hbase.client.log.scanner.activity";

  private final long writeBufferSize;
  private final long writeBufferPeriodicFlushTimeoutMs;
  private final long writeBufferPeriodicFlushTimerTickMs;
  private final int metaOperationTimeout;
  private final int operationTimeout;
  private final int scannerCaching;
  private final long scannerMaxResultSize;
  private final int primaryCallTimeoutMicroSecond;
  private final int replicaCallTimeoutMicroSecondScan;
  private final int metaReplicaCallTimeoutMicroSecondScan;
  private final int retries;
  private final int maxKeyValueSize;
  private final int rpcTimeout;
  private final int readRpcTimeout;
  private final int writeRpcTimeout;
  // toggle for async/sync prefetch
  private final boolean clientScannerAsyncPrefetch;

  /**
   * Constructor
   * @param conf Configuration object
   */
  ConnectionConfiguration(Configuration conf) {
    this.writeBufferSize = conf.getLong(WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);

    this.writeBufferPeriodicFlushTimeoutMs = conf.getLong(
            WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS,
            WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT);

    this.writeBufferPeriodicFlushTimerTickMs = conf.getLong(
            WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS,
            WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS_DEFAULT);

    this.metaOperationTimeout = conf.getInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    this.operationTimeout = conf.getInt(
      HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    this.scannerCaching = conf.getInt(
      HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);

    this.scannerMaxResultSize =
        conf.getLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
            HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

    this.primaryCallTimeoutMicroSecond =
      conf.getInt(PRIMARY_CALL_TIMEOUT_MICROSECOND, PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT);

    this.replicaCallTimeoutMicroSecondScan =
      conf.getInt(PRIMARY_SCAN_TIMEOUT_MICROSECOND, PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT);

    this.metaReplicaCallTimeoutMicroSecondScan =
      conf.getInt(HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT,
        HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT);

    this.retries = conf.getInt(
       HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);

    this.clientScannerAsyncPrefetch = conf.getBoolean(
       Scan.HBASE_CLIENT_SCANNER_ASYNC_PREFETCH, Scan.DEFAULT_HBASE_CLIENT_SCANNER_ASYNC_PREFETCH);

    this.maxKeyValueSize = conf.getInt(MAX_KEYVALUE_SIZE_KEY, MAX_KEYVALUE_SIZE_DEFAULT);

    this.rpcTimeout =
        conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    this.readRpcTimeout = conf.getInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY,
        conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));

    this.writeRpcTimeout = conf.getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
        conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
  }

  /**
   * Constructor
   * This is for internal testing purpose (using the default value).
   * In real usage, we should read the configuration from the Configuration object.
   */
  protected ConnectionConfiguration() {
    this.writeBufferSize = WRITE_BUFFER_SIZE_DEFAULT;
    this.writeBufferPeriodicFlushTimeoutMs = WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT;
    this.writeBufferPeriodicFlushTimerTickMs = WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS_DEFAULT;
    this.metaOperationTimeout = HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    this.operationTimeout = HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    this.scannerCaching = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
    this.scannerMaxResultSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE;
    this.primaryCallTimeoutMicroSecond = 10000;
    this.replicaCallTimeoutMicroSecondScan = 1000000;
    this.metaReplicaCallTimeoutMicroSecondScan =
        HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT;
    this.retries = HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
    this.clientScannerAsyncPrefetch = Scan.DEFAULT_HBASE_CLIENT_SCANNER_ASYNC_PREFETCH;
    this.maxKeyValueSize = MAX_KEYVALUE_SIZE_DEFAULT;
    this.readRpcTimeout = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
    this.writeRpcTimeout = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
    this.rpcTimeout = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
  }

  public int getReadRpcTimeout() {
    return readRpcTimeout;
  }

  public int getWriteRpcTimeout() {
    return writeRpcTimeout;
  }

  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  public long getWriteBufferPeriodicFlushTimeoutMs() {
    return writeBufferPeriodicFlushTimeoutMs;
  }

  public long getWriteBufferPeriodicFlushTimerTickMs() {
    return writeBufferPeriodicFlushTimerTickMs;
  }

  public int getMetaOperationTimeout() {
    return metaOperationTimeout;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

  public int getScannerCaching() {
    return scannerCaching;
  }

  public int getPrimaryCallTimeoutMicroSecond() {
    return primaryCallTimeoutMicroSecond;
  }

  public int getReplicaCallTimeoutMicroSecondScan() {
    return replicaCallTimeoutMicroSecondScan;
  }

  public int getMetaReplicaCallTimeoutMicroSecondScan() {
    return metaReplicaCallTimeoutMicroSecondScan;
  }

  public int getRetriesNumber() {
    return retries;
  }

  public int getMaxKeyValueSize() {
    return maxKeyValueSize;
  }

  public long getScannerMaxResultSize() {
    return scannerMaxResultSize;
  }

  public boolean isClientScannerAsyncPrefetch() {
    return clientScannerAsyncPrefetch;
  }

  public int getRpcTimeout() {
    return rpcTimeout;
  }
}
