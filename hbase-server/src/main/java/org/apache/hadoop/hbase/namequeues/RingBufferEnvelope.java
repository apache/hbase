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

package org.apache.hadoop.hbase.namequeues;

import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * An envelope to carry payload in the ring buffer that serves as online buffer
 * to provide latest events
 */
@InterfaceAudience.Private
final class RingBufferEnvelope {

  private NamedQueuePayload namedQueuePayload;

  /**
   * Load the Envelope with {@link RpcCall}
   *
   * @param namedQueuePayload all details of rpc call that would be useful for ring buffer
   *   consumers
   */
  public void load(NamedQueuePayload namedQueuePayload) {
    this.namedQueuePayload = namedQueuePayload;
  }

  /**
   * Retrieve current namedQueue payload {@link NamedQueuePayload} available on Envelope and
   * free up the Envelope
   *
   * @return Retrieve rpc log details
   */
  public NamedQueuePayload getPayload() {
    final NamedQueuePayload namedQueuePayload = this.namedQueuePayload;
    this.namedQueuePayload = null;
    return namedQueuePayload;
  }

}
