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
package org.apache.hadoop.hbase.keymeta;

import java.util.Objects;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Abstraction representing a key identity (custodian + namespace + partial identity (optional)).
 * The underlying representation can be anything, a single byte array, or multiple individual byte
 * arrays. The interface is designed such that byte arrays can be passed around with least amount of
 * copying.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ManagedKeyIdentity extends Cloneable {

  Bytes KEY_NULL_IDENTITY_BYTES = new Bytes(new byte[0]);

  /** Returns a Bytes view of the custodian segment (no copy). */
  public Bytes getCustodianView();

  /** Returns a Bytes view of the namespace segment (no copy). */
  public Bytes getNamespaceView();

  /** Returns a Bytes view of the partial identity segment (no copy). */
  public Bytes getPartialIdentityView();

  /** Returns a Bytes view of the full identity avoiding a copy when possible. */
  public Bytes getFullIdentityView();

  /**
   * Returns a Bytes view of the identity prefix suitable for use as a prefix filter to scan for all
   * keys with the same custodian and namespace.
   */
  public Bytes getIdentityPrefixView();

  /**
   * Returns a {@link ManagedKeyIdentity} representing only the custodian and namespace segments
   * (partial identity stripped). Implementations should avoid byte copies when possible.
   * <p>
   * If this instance has no partial identity segment, this method should return {@code this}.
   */
  public ManagedKeyIdentity getKeyIdentityPrefix();

  /** Returns the length of the custodian segment. */
  public int getCustodianLength();

  /** Returns the length of the namespace segment. */
  public int getNamespaceLength();

  /** Returns the length of the partial identity segment. */
  public int getPartialIdentityLength();

  /** Returns a copy of the custodian bytes (owned). */
  public byte[] copyCustodian();

  /** Returns a copy of the namespace bytes (owned). */
  public byte[] copyNamespace();

  /** Returns a copy of the partial identity bytes (owned). */
  public byte[] copyPartialIdentity();

  /** Returns the custodian encoded as a Base64 string. */
  public String getCustodianEncoded();

  /** Returns the namespace as a string. */
  public String getNamespaceString();

  /** Returns the partial identity encoded as a Base64 string. */
  public String getPartialIdentityEncoded();

  /** Clones the FullKeyIdentity. */
  public ManagedKeyIdentity clone();

  /** Compares the custodian bytes with the other custodian bytes. */
  public int compareCustodian(byte[] otherCustodian);

  /** Compares the custodian bytes with the other custodian bytes. */
  public int compareCustodian(byte[] otherCustodian, int otherOffset, int otherLength);

  /** Compares the namespace bytes with the other namespace bytes. */
  public int compareNamespace(byte[] otherNamespace);

  /** Compares the namespace bytes with the other namespace bytes. */
  public int compareNamespace(byte[] otherNamespace, int otherOffset, int otherLength);

  /** Compares the partial identity bytes with the other partial identity bytes. */
  public int comparePartialIdentity(byte[] otherPartialIdentity);

  /** Compares the partial identity bytes with the other partial identity bytes. */
  public int comparePartialIdentity(byte[] otherPartialIdentity, int otherOffset, int otherLength);

  /**
   * Content-based equality so that all implementations are interchangeable as map keys. Two
   * identities are equal iff their custodian, namespace, and partial identity bytes are equal.
   * Implementations should override {@link Object#equals} and delegate to this method.
   */
  static boolean contentEquals(ManagedKeyIdentity self, Object obj) {
    if (self == obj) {
      return true;
    }
    if (!(obj instanceof ManagedKeyIdentity)) {
      return false;
    }
    ManagedKeyIdentity that = (ManagedKeyIdentity) obj;
    return self.getCustodianView().equals(that.getCustodianView())
      && self.getNamespaceView().equals(that.getNamespaceView())
      && self.getPartialIdentityView().equals(that.getPartialIdentityView());
  }

  /**
   * Content-based hash so that all implementations are interchangeable as map keys. Uses
   * HashCodeBuilder(17, 37) over the three Bytes views; each view's hashCode is content-based.
   * Implementations should override {@link Object#hashCode} and delegate to this method.
   */
  static int contentHashCode(ManagedKeyIdentity self) {
    return Objects.hash(self.getCustodianView(), self.getNamespaceView(),
      self.getPartialIdentityView());
  }
}
