/*
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

package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A collection of interfaces and utilities used for interacting with custom RPC
 * interfaces exposed by Coprocessors.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class Batch {
  /**
   * Defines a unit of work to be executed.
   *
   * <p>
   * When used with
   * {@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[],
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}
   * the implementations {@link Batch.Call#call(Object)} method will be invoked
   * with a proxy to each region's coprocessor {@link com.google.protobuf.Service} implementation.
   * </p>
   * @see org.apache.hadoop.hbase.client.coprocessor
   * @see org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])
   * @see org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[],
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call)
   * @param <T> the instance type to be passed to
   * {@link Batch.Call#call(Object)}
   * @param <R> the return type from {@link Batch.Call#call(Object)}
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public interface Call<T,R> {
    R call(T instance) throws IOException;
  }

  /**
   * Defines a generic callback to be triggered for each {@link Batch.Call#call(Object)}
   * result.
   *
   * <p>
   * When used with
   * {@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[],
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}
   * the implementation's {@link Batch.Callback#update(byte[], byte[], Object)}
   * method will be called with the {@link Batch.Call#call(Object)} return value
   * from each region in the selected range.
   * </p>
   * @param <R> the return type from the associated {@link Batch.Call#call(Object)}
   * @see org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[],
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call)
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public interface Callback<R> {
    void update(byte[] region, byte[] row, R result);
  }
}
