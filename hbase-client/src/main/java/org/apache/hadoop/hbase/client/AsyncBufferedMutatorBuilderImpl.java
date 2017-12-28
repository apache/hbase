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

import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The implementation of {@link AsyncBufferedMutatorBuilder}.
 */
@InterfaceAudience.Private
class AsyncBufferedMutatorBuilderImpl implements AsyncBufferedMutatorBuilder {

  private final AsyncTableBuilder<?> tableBuilder;

  private long writeBufferSize;

  public AsyncBufferedMutatorBuilderImpl(AsyncConnectionConfiguration connConf,
      AsyncTableBuilder<?> tableBuilder) {
    this.tableBuilder = tableBuilder;
    this.writeBufferSize = connConf.getWriteBufferSize();
  }

  @Override
  public AsyncBufferedMutatorBuilder setOperationTimeout(long timeout, TimeUnit unit) {
    tableBuilder.setOperationTimeout(timeout, unit);
    return this;
  }

  @Override
  public AsyncBufferedMutatorBuilder setRpcTimeout(long timeout, TimeUnit unit) {
    tableBuilder.setRpcTimeout(timeout, unit);
    return this;
  }

  @Override
  public AsyncBufferedMutatorBuilder setRetryPause(long pause, TimeUnit unit) {
    tableBuilder.setRetryPause(pause, unit);
    return this;
  }

  @Override
  public AsyncBufferedMutatorBuilder setMaxAttempts(int maxAttempts) {
    tableBuilder.setMaxAttempts(maxAttempts);
    return this;
  }

  @Override
  public AsyncBufferedMutatorBuilder setStartLogErrorsCnt(int startLogErrorsCnt) {
    tableBuilder.setStartLogErrorsCnt(startLogErrorsCnt);
    return this;
  }

  @Override
  public AsyncBufferedMutatorBuilder setWriteBufferSize(long writeBufferSize) {
    Preconditions.checkArgument(writeBufferSize > 0, "writeBufferSize %d must be >= 0",
      writeBufferSize);
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  @Override
  public AsyncBufferedMutator build() {
    return new AsyncBufferedMutatorImpl(tableBuilder.build(), writeBufferSize);
  }

}
