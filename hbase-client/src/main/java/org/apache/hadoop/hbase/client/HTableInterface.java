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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Used to communicate with a single HBase table.
 * Obtain an instance from an {@link HConnection}.
 *
 * @since 0.21.0
 * @deprecated use {@link org.apache.hadoop.hbase.client.Table} instead
 */
@Deprecated
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface HTableInterface extends Table {

  /**
   * Gets the name of this table.
   *
   * @return the table name.
   * @deprecated Use {@link #getName()} instead
   */
  @Deprecated
  byte[] getTableName();

  /**
   * @deprecated As of release 0.96
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-9508">HBASE-9508</a>).
   *             This will be removed in HBase 2.0.0.
   *             Use {@link #incrementColumnValue(byte[], byte[], byte[], long, Durability)}.
   */
  @Deprecated
  long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException;

  /**
   * @deprecated Use {@link #existsAll(java.util.List)}  instead.
   */
  @Deprecated
  Boolean[] exists(List<Get> gets) throws IOException;


  /**
   * See {@link #setAutoFlush(boolean, boolean)}
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   * @deprecated in 0.96. When called with setAutoFlush(false), this function also
   *  set clearBufferOnFail to true, which is unexpected but kept for historical reasons.
   *  Replace it with setAutoFlush(false, false) if this is exactly what you want, though
   *  this is the method you want for most cases.
   */
  @Deprecated
  void setAutoFlush(boolean autoFlush);

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed
   * and are immediately executed. Failed operations are not retried. This is
   * slower but safer.
   * <p>
   * Turning off {@code #autoFlush} means that multiple {@link Put}s will be
   * accepted before any RPC is actually sent to do the write operations. If the
   * application dies before pending writes get flushed to HBase, data will be
   * lost.
   * <p>
   * When you turn {@code #autoFlush} off, you should also consider the
   * {@code #clearBufferOnFail} option. By default, asynchronous {@link Put}
   * requests will be retried on failure until successful. However, this can
   * pollute the writeBuffer and slow down batching performance. Additionally,
   * you may want to issue a number of Put requests and call
   * {@link #flushCommits()} as a barrier. In both use cases, consider setting
   * clearBufferOnFail to true to erase the buffer after {@link #flushCommits()}
   * has been called, regardless of success.
   * <p>
   * In other words, if you call {@code #setAutoFlush(false)}; HBase will retry N time for each
   *  flushCommit, including the last one when closing the table. This is NOT recommended,
   *  most of the time you want to call {@code #setAutoFlush(false, true)}.
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   * @param clearBufferOnFail
   *          Whether to keep Put failures in the writeBuffer. If autoFlush is true, then
   *          the value of this parameter is ignored and clearBufferOnFail is set to true.
   *          Setting clearBufferOnFail to false is deprecated since 0.96.
   * @deprecated in 0.99 since setting clearBufferOnFail is deprecated. Use
   *  {@link #setAutoFlush(boolean)}} instead.
   * @see BufferedMutator#flush()
   */
  @Deprecated
  void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail);

  /**
   * Set the autoFlush behavior, without changing the value of {@code clearBufferOnFail}.
   * @deprecated in 0.99 since setting clearBufferOnFail is deprecated. Use
   * {@link #setAutoFlush(boolean)} instead, or better still, move on to {@link BufferedMutator}
   */
  @Deprecated
  void setAutoFlushTo(boolean autoFlush);
  
  /**
   * Tells whether or not 'auto-flush' is turned on.
   *
   * @return {@code true} if 'auto-flush' is enabled (default), meaning
   * {@link Put} operations don't get buffered/delayed and are immediately
   * executed.
   * @deprecated as of 1.0.0. Replaced by {@link BufferedMutator}
   */
  @Deprecated
  boolean isAutoFlush();

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch
   * of {@link Put}s (when <code>put(List&lt;Put&gt;)</code> is used) when
   * {@link #isAutoFlush} is {@code true}.
   * @throws IOException if a remote or network exception occurs.
   * @deprecated as of 1.0.0. Replaced by {@link BufferedMutator#flush()}
   */
  @Deprecated
  void flushCommits() throws IOException;

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   * @deprecated as of 1.0.0. Replaced by {@link BufferedMutator#getWriteBufferSize()}
   */
  @Deprecated
  long getWriteBufferSize();

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   * @deprecated as of 1.0.0. Replaced by {@link BufferedMutator} and
   * {@link BufferedMutatorParams#writeBufferSize(long)}
   */
  @Deprecated
  void setWriteBufferSize(long writeBufferSize) throws IOException;


  /**
   * Return the row that matches <i>row</i> exactly,
   * or the one that immediately precedes it.
   *
   * @param row A row key.
   * @param family Column family to include in the {@link Result}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   *
   * @deprecated As of version 0.92 this method is deprecated without
   * replacement. Since version 0.96+, you can use reversed scan.
   * getRowOrBefore is used internally to find entries in hbase:meta and makes
   * various assumptions about the table (which are true for hbase:meta but not
   * in general) to be efficient.
   */
  @Deprecated
  Result getRowOrBefore(byte[] row, byte[] family) throws IOException;
}
