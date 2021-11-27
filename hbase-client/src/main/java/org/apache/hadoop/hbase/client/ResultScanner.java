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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for client-side scanning.
 * Go to {@link Table} to obtain instances.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ResultScanner extends Closeable, Iterable<Result> {

  /**
   * Grab the next row's worth of values. The scanner will return a Result.
   * @return Result object if there is another row, null if the scanner is
   * exhausted.
   * @throws IOException e
   */
  Result next() throws IOException;

  /**
   * @param nbRows number of rows to return
   * @return Between zero and nbRows results
   * @throws IOException e
   */
  Result[] next(int nbRows) throws IOException;

  /**
   * @param nbRows nbRows number of rows to return
   * @param maxResultInitLength maxResultInitLength number of resultSets init length
   * @return Between zero and nbRows results
   * @throws IOException e
   */
  Result[] next(int nbRows, int maxResultInitLength) throws IOException;

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  @Override
  void close();

  /**
   * Allow the client to renew the scanner's lease on the server.
   * @return true if the lease was successfully renewed, false otherwise.
   */
  boolean renewLease();

  /**
   * @return the scan metrics, or {@code null} if we do not enable metrics.
   */
  ScanMetrics getScanMetrics();
}
