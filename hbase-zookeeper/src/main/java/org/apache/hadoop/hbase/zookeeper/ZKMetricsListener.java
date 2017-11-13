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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface ZKMetricsListener {

  /**
   * An AUTHFAILED Exception was seen.
   */
  void registerAuthFailedException();

  /**
   * A CONNECTIONLOSS Exception was seen.
   */
  void registerConnectionLossException();

  /**
   * A DATAINCONSISTENCY Exception was seen.
   */
  void registerDataInconsistencyException();

  /**
   * An INVALIDACL Exception was seen.
   */
  void registerInvalidACLException();

  /**
   * A NOAUTH Exception was seen.
   */
  void registerNoAuthException();

  /**
   * A OPERATIONTIMEOUT Exception was seen.
   */
  void registerOperationTimeoutException();

  /**
   * A RUNTIMEINCONSISTENCY Exception was seen.
   */
  void registerRuntimeInconsistencyException();

  /**
   * A SESSIONEXPIRED Exception was seen.
   */
  void registerSessionExpiredException();

  /**
   * A SYSTEMERROR Exception was seen.
   */
  void registerSystemErrorException();

  /**
   * A ZooKeeper API Call failed.
   */
  void registerFailedZKCall();

  /**
   * Register the latency incurred for read operations.
   */
  void registerReadOperationLatency(long latency);

  /**
   * Register the latency incurred for write operations.
   */
  void registerWriteOperationLatency(long latency);

  /**
   * Register the latency incurred for sync operations.
   */
  void registerSyncOperationLatency(long latency);
}
