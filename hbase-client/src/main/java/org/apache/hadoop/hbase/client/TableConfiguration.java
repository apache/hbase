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
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 *
 * Configuration is a heavy weight registry that does a lot of string operations and regex matching.
 * Method calls into Configuration account for high CPU usage and have huge performance impact.
 * This class caches the value in the TableConfiguration object to improve performance.
 * see HBASE-12128
 *
 */
@InterfaceAudience.Private
public class TableConfiguration {

  private final long writeBufferSize;

  private final int metaOperationTimeout;

  private final int operationTimeout;

  private final int scannerCaching;

  private final int primaryCallTimeoutMicroSecond;

  private final int replicaCallTimeoutMicroSecondScan;

  private final int retries;

  private final int maxKeyValueSize;

  /**
   * Constructor
   * @param conf Configuration object
   */
  TableConfiguration(Configuration conf) {
    this.writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);

    this.metaOperationTimeout = conf.getInt(
      HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    this.operationTimeout = conf.getInt(
      HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    this.scannerCaching = conf.getInt(
      HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);

    this.primaryCallTimeoutMicroSecond =
        conf.getInt("hbase.client.primaryCallTimeout.get", 10000); // 10ms

    this.replicaCallTimeoutMicroSecondScan =
        conf.getInt("hbase.client.replicaCallTimeout.scan", 1000000); // 1000 ms

    this.retries = conf.getInt(
       HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);

    this.maxKeyValueSize = conf.getInt("hbase.client.keyvalue.maxsize", -1);
  }

  /**
   * Constructor
   * This is for internal testing purpose (using the default value).
   * In real usage, we should read the configuration from the Configuration object.
   */
  @VisibleForTesting
  protected TableConfiguration() {
    this.writeBufferSize = 2097152;
    this.metaOperationTimeout = HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    this.operationTimeout = HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    this.scannerCaching = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
    this.primaryCallTimeoutMicroSecond = 10000;
    this.replicaCallTimeoutMicroSecondScan = 1000000;
    this.retries = HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
    this.maxKeyValueSize = -1;
  }

  public long getWriteBufferSize() {
    return writeBufferSize;
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

  public int getRetriesNumber() {
    return retries;
  }

  public int getMaxKeyValueSize() {
    return maxKeyValueSize;
  }
}
