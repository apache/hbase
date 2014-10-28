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
package org.apache.hadoop.hbase.client.backoff;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Configurable policy for the amount of time a client should wait for a new request to the
 * server when given the server load statistics.
 * <p>
 * Must have a single-argument constructor that takes a {@link org.apache.hadoop.conf.Configuration}
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ClientBackoffPolicy {

  public static final String BACKOFF_POLICY_CLASS =
      "hbase.client.statistics.backoff-policy";

  /**
   * @return the number of ms to wait on the client based on the
   */
  public long getBackoffTime(ServerName serverName, byte[] region, ServerStatistics stats);
}