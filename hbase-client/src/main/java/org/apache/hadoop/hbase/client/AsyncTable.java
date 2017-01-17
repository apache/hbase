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
 * The asynchronous table for normal users.
 * <p>
 * The implementation is required to be thread safe.
 * <p>
 * The implementation should make sure that user can do everything they want to the returned
 * {@code CompletableFuture} without breaking anything. Usually the implementation will require user
 * to provide a {@code ExecutorService}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncTable extends AsyncTableBase {

  /**
   * Gets a scanner on the current table for the given family.
   * @param family The column family to scan.
   * @return A scanner.
   */
  default ResultScanner getScanner(byte[] family) {
    return getScanner(new Scan().addFamily(family));
  }

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   */
  default ResultScanner getScanner(byte[] family, byte[] qualifier) {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  /**
   * Returns a scanner on the current table as specified by the {@link Scan} object.
   * @param scan A configured {@link Scan} object.
   * @return A scanner.
   */
  ResultScanner getScanner(Scan scan);

  /**
   * The scan API uses the observer pattern. All results that match the given scan object will be
   * passed to the given {@code consumer} by calling {@link ScanResultConsumer#onNext(Result)}.
   * {@link ScanResultConsumer#onComplete()} means the scan is finished, and
   * {@link ScanResultConsumer#onError(Throwable)} means we hit an unrecoverable error and the scan
   * is terminated.
   * @param scan A configured {@link Scan} object.
   * @param consumer the consumer used to receive results.
   */
  void scan(Scan scan, ScanResultConsumer consumer);
}
