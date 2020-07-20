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

package org.apache.hadoop.hbase.regionserver.slowlog;

import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * An envelope to carry payload in the slow log ring buffer that serves as online buffer
 * to provide latest TooSlowLog
 */
@InterfaceAudience.Private
final class RingBufferEnvelope {

  private RpcLogDetails rpcLogDetails;

  /**
   * Load the Envelope with {@link RpcCall}
   *
   * @param rpcLogDetails all details of rpc call that would be useful for ring buffer
   *   consumers
   */
  public void load(RpcLogDetails rpcLogDetails) {
    this.rpcLogDetails = rpcLogDetails;
  }

  /**
   * Retrieve current rpcCall details {@link RpcLogDetails} available on Envelope and
   * free up the Envelope
   *
   * @return Retrieve rpc log details
   */
  public RpcLogDetails getPayload() {
    final RpcLogDetails rpcLogDetails = this.rpcLogDetails;
    this.rpcLogDetails = null;
    return rpcLogDetails;
  }

}
