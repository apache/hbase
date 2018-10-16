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
package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

 /**
   * This implementation is not smart and just treats nonce group and nonce as random bits.
   */
  // TODO: we could use pure byte arrays, but then we wouldn't be able to use hash map.
@InterfaceAudience.Private
public class NonceKey {
  private long group;
  private long nonce;

  public NonceKey(long group, long nonce) {
    this.group = group;
    this.nonce = nonce;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof NonceKey)) {
      return false;
    }
    NonceKey nk = ((NonceKey)obj);
    return this.nonce == nk.nonce && this.group == nk.group;
  }

  @Override
  public int hashCode() {
    return (int)((group >> 32) ^ group ^ (nonce >> 32) ^ nonce);
  }

  @Override
  public String toString() {
    return "[" + group + ":" + nonce + "]";
  }

  public long getNonceGroup() {
    return group;
  }

  public long getNonce() {
    return nonce;
  }
}
