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

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * NonceGenerator implementation that uses client ID hash + random int as nonce group,
 * and random numbers as nonces.
 */
@InterfaceAudience.Private
public class PerClientRandomNonceGenerator implements NonceGenerator {
  private final Random rdm = new Random();
  private final long clientId;

  public PerClientRandomNonceGenerator() {
    byte[] clientIdBase = ClientIdGenerator.generateClientId();
    this.clientId = (((long)Arrays.hashCode(clientIdBase)) << 32) + rdm.nextInt();
  }

  public long getNonceGroup() {
    return this.clientId;
  }

  public long newNonce() {
    long result = HConstants.NO_NONCE;
    do {
      result = rdm.nextLong();
    } while (result == HConstants.NO_NONCE);
    return result;
  }
}
