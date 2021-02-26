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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Used to communicate with a single HBase table in batches. Obtain an instance from a
 * {@link AsyncConnection} and call {@link #close()} afterwards.
 * <p>
 * The implementation is required to be thread safe.
 */
@InterfaceAudience.Public
public interface AsyncBufferedMutator extends Closeable {

  /**
   * Gets the fully qualified table name instance of the table that this
   * {@code AsyncBufferedMutator} writes to.
   */
  TableName getName();

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Sends a {@link Mutation} to the table. The mutations will be buffered and sent over the wire as
   * part of a batch. Currently only supports {@link Put} and {@link Delete} mutations.
   * @param mutation The data to send.
   */
  default CompletableFuture<Void> mutate(Mutation mutation) {
    return Iterables.getOnlyElement(mutate(Collections.singletonList(mutation)));
  }

  /**
   * Send some {@link Mutation}s to the table. The mutations will be buffered and sent over the wire
   * as part of a batch. There is no guarantee of sending entire content of {@code mutations} in a
   * single batch, the implementations are free to break it up according to the write buffer
   * capacity.
   * @param mutations The data to send.
   */
  List<CompletableFuture<Void>> mutate(List<? extends Mutation> mutations);

  /**
   * Executes all the buffered, asynchronous operations.
   */
  void flush();

  /**
   * Performs a {@link #flush()} and releases any resources held.
   */
  @Override
  void close();

  /**
   * Returns the maximum size in bytes of the write buffer.
   * <p>
   * The default value comes from the configuration parameter {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  long getWriteBufferSize();

  /**
   * Returns the periodical flush interval, 0 means disabled.
   */
  default long getPeriodicalFlushTimeout(TimeUnit unit) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
