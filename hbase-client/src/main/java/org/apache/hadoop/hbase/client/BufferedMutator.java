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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * <p>Used to communicate with a single HBase table similar to {@link Table} but meant for
 * batched, asynchronous puts. Obtain an instance from a {@link Connection} and call
 * {@link #close()} afterwards. Customizations can be applied to the {@code BufferedMutator} via
 * the {@link BufferedMutatorParams}.
 * </p>
 *
 * <p>Exception handling with asynchronously via the {@link BufferedMutator.ExceptionListener}.
 * The default implementation is to throw the exception upon receipt. This behavior can be
 * overridden with a custom implementation, provided as a parameter with
 * {@link BufferedMutatorParams#listener(BufferedMutator.ExceptionListener)}.</p>
 *
 * <p>Map/Reduce jobs are good use cases for using {@code BufferedMutator}. Map/reduce jobs
 * benefit from batching, but have no natural flush point. {@code BufferedMutator} receives the
 * puts from the M/R job and will batch puts based on some heuristic, such as the accumulated size
 * of the puts, and submit batches of puts asynchronously so that the M/R logic can continue
 * without interruption.
 * </p>
 *
 * <p>{@code BufferedMutator} can also be used on more exotic circumstances. Map/Reduce batch jobs
 * will have a single {@code BufferedMutator} per thread. A single {@code BufferedMutator} can
 * also be effectively used in high volume online systems to batch puts, with the caveat that
 * extreme circumstances, such as JVM or machine failure, may cause some data loss.</p>
 *
 * <p>NOTE: This class replaces the functionality that used to be available via
 * HTable#setAutoFlush(boolean) set to {@code false}.
 * </p>
 *
 * <p>See also the {@code BufferedMutatorExample} in the hbase-examples module.</p>
 * @see ConnectionFactory
 * @see Connection
 * @since 1.0.0
 */
@InterfaceAudience.Public
public interface BufferedMutator extends Closeable {
  /**
   * Key to use setting non-default BufferedMutator implementation in Configuration.
   */
  String CLASSNAME_KEY = "hbase.client.bufferedmutator.classname";

  /**
   * Having the timer tick run more often that once every 100ms is needless and will
   * probably cause too many timer events firing having a negative impact on performance.
   */
  long MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS = 100;

  /**
   * Gets the fully qualified table name instance of the table that this BufferedMutator writes to.
   */
  TableName getName();

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will
   * affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Sends a {@link Mutation} to the table. The mutations will be buffered and sent over the
   * wire as part of a batch. Currently only supports {@link Put} and {@link Delete} mutations.
   *
   * @param mutation The data to send.
   * @throws IOException if a remote or network exception occurs.
   */
  void mutate(Mutation mutation) throws IOException;

  /**
   * Send some {@link Mutation}s to the table. The mutations will be buffered and sent over the
   * wire as part of a batch. There is no guarantee of sending entire content of {@code mutations}
   * in a single batch; it will be broken up according to the write buffer capacity.
   *
   * @param mutations The data to send.
   * @throws IOException if a remote or network exception occurs.
   */
  void mutate(List<? extends Mutation> mutations) throws IOException;

  /**
   * Performs a {@link #flush()} and releases any resources held.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  @Override
  void close() throws IOException;

  /**
   * Executes all the buffered, asynchronous {@link Mutation} operations and waits until they
   * are done.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  void flush() throws IOException;

  /**
   * Sets the maximum time before the buffer is automatically flushed checking once per second.
   * @param timeoutMs    The maximum number of milliseconds how long records may be buffered
   *                     before they are flushed. Set to 0 to disable.
   */
  default void setWriteBufferPeriodicFlush(long timeoutMs) {
    setWriteBufferPeriodicFlush(timeoutMs, 1000L);
  }

  /**
   * Sets the maximum time before the buffer is automatically flushed.
   * @param timeoutMs    The maximum number of milliseconds how long records may be buffered
   *                     before they are flushed. Set to 0 to disable.
   * @param timerTickMs  The number of milliseconds between each check if the
   *                     timeout has been exceeded. Must be 100ms (as defined in
   *                     {@link #MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS})
   *                     or larger to avoid performance problems.
   */
  default void setWriteBufferPeriodicFlush(long timeoutMs, long timerTickMs) {
    throw new UnsupportedOperationException(
            "The BufferedMutator::setWriteBufferPeriodicFlush has not been implemented");
  }

  /**
   * Disable periodic flushing of the write buffer.
   */
  default void disableWriteBufferPeriodicFlush() {
    setWriteBufferPeriodicFlush(0, MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
  }

  /**
   * Returns the current periodic flush timeout value in milliseconds.
   * @return The maximum number of milliseconds how long records may be buffered before they
   *   are flushed. The value 0 means this is disabled.
   */
  default long getWriteBufferPeriodicFlushTimeoutMs() {
    throw new UnsupportedOperationException(
            "The BufferedMutator::getWriteBufferPeriodicFlushTimeoutMs has not been implemented");
  }

  /**
   * Returns the current periodic flush timertick interval in milliseconds.
   * @return The number of milliseconds between each check if the timeout has been exceeded.
   *   This value only has a real meaning if the timeout has been set to > 0
   */
  default long getWriteBufferPeriodicFlushTimerTickMs() {
    throw new UnsupportedOperationException(
            "The BufferedMutator::getWriteBufferPeriodicFlushTimerTickMs has not been implemented");
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  long getWriteBufferSize();

  /**
   * Set rpc timeout for this mutator instance
   */
  void setRpcTimeout(int timeout);

  /**
   * Set operation timeout for this mutator instance
   */
  void setOperationTimeout(int timeout);

  /**
   * Listens for asynchronous exceptions on a {@link BufferedMutator}.
   */
  @InterfaceAudience.Public
  interface ExceptionListener {
    public void onException(RetriesExhaustedWithDetailsException exception,
        BufferedMutator mutator) throws RetriesExhaustedWithDetailsException;
  }
}
