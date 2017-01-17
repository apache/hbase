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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A low level asynchronous table.
 * <p>
 * The implementation is required to be thread safe.
 * <p>
 * The returned {@code CompletableFuture} will be finished directly in the rpc framework's callback
 * thread, so typically you should not do any time consuming work inside these methods, otherwise
 * you will be likely to block at least one connection to RS(even more if the rpc framework uses
 * NIO).
 * <p>
 * So, only experts that want to build high performance service should use this interface directly,
 * especially for the {@link #scan(Scan, RawScanResultConsumer)} below.
 * <p>
 * TODO: For now the only difference between this interface and {@link AsyncTable} is the scan
 * method. The {@link RawScanResultConsumer} exposes the implementation details of a scan(heartbeat)
 * so it is not suitable for a normal user. If it is still the only difference after we implement
 * most features of AsyncTable, we can think about merge these two interfaces.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface RawAsyncTable extends AsyncTableBase {

  /**
   * The basic scan API uses the observer pattern. All results that match the given scan object will
   * be passed to the given {@code consumer} by calling
   * {@link RawScanResultConsumer#onNext(Result[])}. {@link RawScanResultConsumer#onComplete()}
   * means the scan is finished, and {@link RawScanResultConsumer#onError(Throwable)} means we hit
   * an unrecoverable error and the scan is terminated. {@link RawScanResultConsumer#onHeartbeat()}
   * means the RS is still working but we can not get a valid result to call
   * {@link RawScanResultConsumer#onNext(Result[])}. This is usually because the matched results are
   * too sparse, for example, a filter which almost filters out everything is specified.
   * <p>
   * Notice that, the methods of the given {@code consumer} will be called directly in the rpc
   * framework's callback thread, so typically you should not do any time consuming work inside
   * these methods, otherwise you will be likely to block at least one connection to RS(even more if
   * the rpc framework uses NIO).
   * @param scan A configured {@link Scan} object.
   * @param consumer the consumer used to receive results.
   */
  void scan(Scan scan, RawScanResultConsumer consumer);
}
