/*
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
package org.apache.hadoop.hbase.io.crypto;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import java.security.Key;

@InterfaceAudience.Public
public class PBEKeyData {
  private byte[] pbe_prefix;
  private Key theKey;
  private PBEKeyStatus keyStatus;
  private String keyMetadata;

  public PBEKeyData(byte[] pbe_prefix, Key theKey, PBEKeyStatus keyStatus, String keyMetadata) {
    this.pbe_prefix = pbe_prefix;
    this.theKey = theKey;
    this.keyStatus = keyStatus;
    this.keyMetadata = keyMetadata;
  }

  public byte[] getPbe_prefix() {
    return pbe_prefix;
  }

  public Key getTheKey() {
    return theKey;
  }

  public PBEKeyStatus getKeyStatus() {
    return keyStatus;
  }

  public String getKeyMetadata() {
    return keyMetadata;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    PBEKeyData that = (PBEKeyData) o;

    return new EqualsBuilder().append(pbe_prefix, that.pbe_prefix).append(theKey, that.theKey)
      .append(keyStatus, that.keyStatus).append(keyMetadata, that.keyMetadata).isEquals();
  }

  @Override public int hashCode() {
    return new HashCodeBuilder(17, 37).append(pbe_prefix).append(
      theKey).append(keyStatus).append(keyMetadata).toHashCode();
  }
}
