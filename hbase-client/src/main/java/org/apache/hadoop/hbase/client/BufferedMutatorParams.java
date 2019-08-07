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

package org.apache.hadoop.hbase.client;

import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Parameters for instantiating a {@link BufferedMutator}.
 */
@InterfaceAudience.Public
public class BufferedMutatorParams implements Cloneable {

  static final int UNSET = -1;

  private final TableName tableName;
  private long writeBufferSize = UNSET;
  private long writeBufferPeriodicFlushTimeoutMs = UNSET;
  private long writeBufferPeriodicFlushTimerTickMs = UNSET;
  private int maxKeyValueSize = UNSET;
  private ExecutorService pool = null;
  private String implementationClassName = null;
  private int rpcTimeout = UNSET;
  private int operationTimeout = UNSET;
  private BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
    @Override
    public void onException(RetriesExhaustedWithDetailsException exception,
        BufferedMutator bufferedMutator)
        throws RetriesExhaustedWithDetailsException {
      throw exception;
    }
  };

  public BufferedMutatorParams(TableName tableName) {
    this.tableName = tableName;
  }

  public TableName getTableName() {
    return tableName;
  }

  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  public BufferedMutatorParams rpcTimeout(final int rpcTimeout) {
    this.rpcTimeout = rpcTimeout;
    return this;
  }

  public int getRpcTimeout() {
    return rpcTimeout;
  }

  public BufferedMutatorParams operationTimeout(final int operationTimeout) {
    this.operationTimeout = operationTimeout;
    return this;
  }

  /**
   * @deprecated Since 2.3.0, will be removed in 4.0.0. Use {@link #operationTimeout(int)}
   */
  @Deprecated
  public BufferedMutatorParams opertationTimeout(final int operationTimeout) {
    this.operationTimeout = operationTimeout;
    return this;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

  /**
   * Override the write buffer size specified by the provided {@link Connection}'s
   * {@link org.apache.hadoop.conf.Configuration} instance, via the configuration key
   * {@code hbase.client.write.buffer}.
   */
  public BufferedMutatorParams writeBufferSize(long writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  public long getWriteBufferPeriodicFlushTimeoutMs() {
    return writeBufferPeriodicFlushTimeoutMs;
  }

  /**
   * Set the max timeout before the buffer is automatically flushed.
   */
  public BufferedMutatorParams setWriteBufferPeriodicFlushTimeoutMs(long timeoutMs) {
    this.writeBufferPeriodicFlushTimeoutMs = timeoutMs;
    return this;
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. We use a common timer in the whole client
   *             implementation so you can not set it any more.
   */
  @Deprecated
  public long getWriteBufferPeriodicFlushTimerTickMs() {
    return writeBufferPeriodicFlushTimerTickMs;
  }

  /**
   * Set the TimerTick how often the buffer timeout if checked.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. We use a common timer in the whole client
   *             implementation so you can not set it any more.
   */
  @Deprecated
  public BufferedMutatorParams setWriteBufferPeriodicFlushTimerTickMs(long timerTickMs) {
    this.writeBufferPeriodicFlushTimerTickMs = timerTickMs;
    return this;
  }

  public int getMaxKeyValueSize() {
    return maxKeyValueSize;
  }

  /**
   * Override the maximum key-value size specified by the provided {@link Connection}'s
   * {@link org.apache.hadoop.conf.Configuration} instance, via the configuration key
   * {@code hbase.client.keyvalue.maxsize}.
   */
  public BufferedMutatorParams maxKeyValueSize(int maxKeyValueSize) {
    this.maxKeyValueSize = maxKeyValueSize;
    return this;
  }

  public ExecutorService getPool() {
    return pool;
  }

  /**
   * Override the default executor pool defined by the {@code hbase.htable.threads.*}
   * configuration values.
   */
  public BufferedMutatorParams pool(ExecutorService pool) {
    this.pool = pool;
    return this;
  }

  /**
   * @return Name of the class we will use when we construct a {@link BufferedMutator} instance or
   *         null if default implementation.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. You can not set it any more as the
   *             implementation has to use too many internal stuffs in HBase.
   */
  @Deprecated
  public String getImplementationClassName() {
    return this.implementationClassName;
  }

  /**
   * Specify a BufferedMutator implementation other than the default.
   * @param implementationClassName Name of the BufferedMutator implementation class
   * @deprecated Since 3.0.0, will be removed in 4.0.0. You can not set it any more as the
   *             implementation has to use too many internal stuffs in HBase.
   */
  @Deprecated
  public BufferedMutatorParams implementationClassName(String implementationClassName) {
    this.implementationClassName = implementationClassName;
    return this;
  }

  public BufferedMutator.ExceptionListener getListener() {
    return listener;
  }

  /**
   * Override the default error handler. Default handler simply rethrows the exception.
   */
  public BufferedMutatorParams listener(BufferedMutator.ExceptionListener listener) {
    this.listener = listener;
    return this;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="CN_IDIOM_NO_SUPER_CALL",
    justification="The clone below is complete")
  @Override
  public BufferedMutatorParams clone() {
    BufferedMutatorParams clone = new BufferedMutatorParams(this.tableName);
    clone.writeBufferSize                     = this.writeBufferSize;
    clone.writeBufferPeriodicFlushTimeoutMs   = this.writeBufferPeriodicFlushTimeoutMs;
    clone.writeBufferPeriodicFlushTimerTickMs = this.writeBufferPeriodicFlushTimerTickMs;
    clone.maxKeyValueSize                     = this.maxKeyValueSize;
    clone.pool                                = this.pool;
    clone.listener                            = this.listener;
    clone.implementationClassName             = this.implementationClassName;
    return clone;
  }
}
