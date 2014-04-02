/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Map;

/**
 * The interface of a client for calling a endpoint.
 */
public interface IEndpointClient {

  /**
   * The interface of a caller for <code>coprocessorEndpoint</code>
   *
   * @param <T>
   *          The type of the endpoint interface. (NOT the implementation)
   */
  public interface Caller<T extends IEndpoint> {

    /**
     * Calls an endpoint.
     *
     * @param client
     *          an RPC client.
     * @return the result to be put as a value in coprocessorEndpoint's results
     */
    byte[] call(T client) throws IOException;
  }

  /**
   * Calls an endpoint in the server side and returns results.
   *
   * The <tt>caller</tt> is called for every region provied with an RPC client
   * with the same time as the endpoint interface.
   *
   * @param clazz
   *          the class of the endpoint interface.
   * @param startRow
   *          the start row. null or empty array means no limit on start.
   * @param stopRow
   *          the stop row. null or empty array means no limit on stop.
   * @param caller
   *          the caller for each region
   * @return a map from region name to results.
   */
  <T extends IEndpoint> Map<byte[], byte[]> coprocessorEndpoint(Class<T> clazz,
      byte[] startRow, byte[] stopRow, Caller<T> caller) throws IOException;
}
