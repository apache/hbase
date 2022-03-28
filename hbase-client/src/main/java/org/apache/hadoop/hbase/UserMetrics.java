/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
  * Encapsulates per-user load metrics.
  */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface UserMetrics {

  interface ClientMetrics {

    String getHostName();

    long getReadRequestsCount();

    long getWriteRequestsCount();

    long getFilteredReadRequestsCount();
  }

  /**
   * @return the user name
   */
  byte[] getUserName();

  /**
   * @return the number of read requests made by user
   */
  long getReadRequestCount();

  /**
   * @return the number of write requests made by user
   */
  long getWriteRequestCount();

  /**
   * @return the number of write requests and read requests and coprocessor
   *         service requests made by the user
   */
  default long getRequestCount() {
    return getReadRequestCount() + getWriteRequestCount();
  }

  /**
   * @return the user name as a string
   */
  default String getNameAsString() {
    return Bytes.toStringBinary(getUserName());
  }

  /**
   * @return metrics per client(hostname)
   */
  Map<String, ClientMetrics> getClientMetrics();

  /**
   * @return count of filtered read requests for a user
   */
  long getFilteredReadRequests();
}
