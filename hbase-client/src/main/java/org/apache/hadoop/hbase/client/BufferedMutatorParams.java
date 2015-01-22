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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Parameters for instantiating a {@link BufferedMutator}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BufferedMutatorParams {

  static final int UNSET = -1;

  private final TableName tableName;
  private long writeBufferSize = UNSET;
  private int maxKeyValueSize = UNSET;
  private ExecutorService pool = null;
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

  /**
   * Override the write buffer size specified by the provided {@link Connection}'s
   * {@link org.apache.hadoop.conf.Configuration} instance, via the configuration key
   * {@code hbase.client.write.buffer}.
   */
  public BufferedMutatorParams writeBufferSize(long writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
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
}
