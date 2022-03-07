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

import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for all asynchronous table builders.
 */
@InterfaceAudience.Private
abstract class AsyncTableBuilderBase<C extends ScanResultConsumerBase>
    implements AsyncTableBuilder<C> {

  protected TableName tableName;

  protected long operationTimeoutNs;

  protected long scanTimeoutNs;

  protected long rpcTimeoutNs;

  protected long readRpcTimeoutNs;

  protected long writeRpcTimeoutNs;

  protected long pauseNs;

  protected long pauseNsForServerOverloaded;

  protected int maxAttempts;

  protected int startLogErrorsCnt;

  AsyncTableBuilderBase(TableName tableName, AsyncConnectionConfiguration connConf) {
    this.tableName = tableName;
    this.operationTimeoutNs = tableName.isSystemTable() ? connConf.getMetaOperationTimeoutNs()
      : connConf.getOperationTimeoutNs();
    this.scanTimeoutNs = connConf.getScanTimeoutNs();
    this.rpcTimeoutNs = connConf.getRpcTimeoutNs();
    this.readRpcTimeoutNs = connConf.getReadRpcTimeoutNs();
    this.writeRpcTimeoutNs = connConf.getWriteRpcTimeoutNs();
    this.pauseNs = connConf.getPauseNs();
    this.pauseNsForServerOverloaded = connConf.getPauseNsForServerOverloaded();
    this.maxAttempts = retries2Attempts(connConf.getMaxRetries());
    this.startLogErrorsCnt = connConf.getStartLogErrorsCnt();
  }

  @Override
  public AsyncTableBuilderBase<C> setOperationTimeout(long timeout, TimeUnit unit) {
    this.operationTimeoutNs = unit.toNanos(timeout);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setScanTimeout(long timeout, TimeUnit unit) {
    this.scanTimeoutNs = unit.toNanos(timeout);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setRpcTimeout(long timeout, TimeUnit unit) {
    this.rpcTimeoutNs = unit.toNanos(timeout);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setReadRpcTimeout(long timeout, TimeUnit unit) {
    this.readRpcTimeoutNs = unit.toNanos(timeout);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setWriteRpcTimeout(long timeout, TimeUnit unit) {
    this.writeRpcTimeoutNs = unit.toNanos(timeout);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setRetryPause(long pause, TimeUnit unit) {
    this.pauseNs = unit.toNanos(pause);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setRetryPauseForServerOverloaded(long pause, TimeUnit unit) {
    this.pauseNsForServerOverloaded = unit.toNanos(pause);
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  @Override
  public AsyncTableBuilderBase<C> setStartLogErrorsCnt(int startLogErrorsCnt) {
    this.startLogErrorsCnt = startLogErrorsCnt;
    return this;
  }
}
