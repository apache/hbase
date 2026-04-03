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

import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Immutable value type representing a key identity (custodian + namespace + optional partial
 * identity) backed by a single byte array. The backing byte array will not be duplicated for the
 * sake of optimizing the memory usage, so only share those byte arrays that are truly immutable and
 * owned by the caller. For the format of the byte array refer to
 * {@link ManagedKeyIdentityUtils#constructRowKeyForIdentity}. In addition to a 0-length partial
 * identity encoding, this implementation also works with a partial identity that is completely
 * missing, so it works with marker rows created by
 * {@link ManagedKeyIdentityUtils#constructRowKeyForCustNamespace}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class KeyIdentitySingleArrayBacked implements ManagedKeyIdentity {

  /** Backing key full identity bytes, or null when view-based (fromViews). */
  private final byte[] keyIdentity;
  private final int offset;
  private final int length;

  /**
   * Creates a KeyIdentity backed by the given key full identity bytes. Validates format and parses
   * segment offsets/lengths; no array copies.
   * @param keyFullIdentity the key full identity byte array (format: custLen, cust, nsLen, ns,
   *                        partialLen, partial)
   * @throws IllegalArgumentException if keyFullIdentity is null or malformed
   */
  public KeyIdentitySingleArrayBacked(byte[] keyFullIdentity) {
    this(keyFullIdentity, 0, keyFullIdentity != null ? keyFullIdentity.length : 0);
  }

  /**
   * Creates a KeyIdentity backed by a slice of the given array.
   * @param keyFullIdentity the key full identity byte array
   * @param offset          start offset
   * @param length          length of the key full identity segment
   */
  public KeyIdentitySingleArrayBacked(byte[] keyFullIdentity, int offset, int length) {
    Preconditions.checkArgument(keyFullIdentity != null, "keyFullIdentity cannot be null");
    int minLen = 1 + 1 + 1 + 1; // custLen byte + min 1 cust + nsLen byte + min 1 ns
    Preconditions.checkArgument(length >= minLen, "keyFullIdentity appears to be too short: %s",
      length);
    this.keyIdentity = keyFullIdentity;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Returns the backing key full identity byte array, or null when view-based. When non-null and
   * offset is 0 and length is array length, this is the row key for Get.
   */
  public byte[] getBackingArray() {
    return keyIdentity;
  }

  /** Returns the offset into the backing array, or 0 when view-based. */
  public int getOffset() {
    return offset;
  }

  /** Returns the length of the key full identity segment, or 0 when view-based. */
  public int getLength() {
    return length;
  }

  /** Returns a Bytes view of the custodian segment (no copy). */
  @Override
  public Bytes getCustodianView() {
    int off = getCustodianOffset();
    int custLen = keyIdentity[off++] & 0xFF;
    return new Bytes(keyIdentity, off, custLen);
  }

  /** Returns a Bytes view of the namespace segment (no copy). */
  @Override
  public Bytes getNamespaceView() {
    int off = getNamespaceOffset();
    int nsLen = keyIdentity[off++] & 0xFF;
    return new Bytes(keyIdentity, off, nsLen);
  }

  /** Returns a Bytes view of the partial identity segment (no copy). */
  @Override
  public Bytes getPartialIdentityView() {
    int off = getPartialIdentityOffset();
    if (off == -1) {
      return ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES;
    }
    int partialLen = keyIdentity[off++] & 0xFF;
    return new Bytes(keyIdentity, off, partialLen);
  }

  @Override
  public Bytes getFullIdentityView() {
    return new Bytes(keyIdentity, offset, length);
  }

  @Override
  public Bytes getIdentityPrefixView() {
    int prefixEndExclusive = getNamespaceOffset() + 1 + getNamespaceLength();
    return new Bytes(keyIdentity, offset, prefixEndExclusive - offset);
  }

  @Override
  public ManagedKeyIdentity getKeyIdentityPrefix() {
    int partialOffset = getPartialIdentityOffset();
    if (partialOffset == -1) {
      return this;
    }
    return new KeyIdentitySingleArrayBacked(keyIdentity, offset, partialOffset - offset);
  }

  /** Returns a copy of the custodian bytes (owned). */
  @Override
  public byte[] copyCustodian() {
    return getCustodianView().copyBytes();
  }

  /** Returns a copy of the namespace bytes (owned). */
  @Override
  public byte[] copyNamespace() {
    return getNamespaceView().copyBytes();
  }

  /** Returns a copy of the partial identity bytes (owned). */
  @Override
  public byte[] copyPartialIdentity() {
    return getPartialIdentityView().copyBytes();
  }

  @Override
  public boolean equals(Object obj) {
    return ManagedKeyIdentity.contentEquals(this, obj);
  }

  @Override
  public int hashCode() {
    return ManagedKeyIdentity.contentHashCode(this);
  }

  private int getCustodianOffset() {
    int off = offset;
    int custLen = keyIdentity[off] & 0xFF;
    Preconditions.checkArgument(custLen >= 1, "Custodian length must be at least 1, got %s",
      custLen);
    Preconditions.checkArgument(off + custLen <= offset + length,
      "keyIdentity too short for custodian length expected %s, got %s", off + custLen,
      offset + length);
    return off;
  }

  private int getNamespaceOffset() {
    int off = getCustodianOffset();
    int custLen = keyIdentity[off++] & 0xFF;
    off += custLen;
    int nsLen = keyIdentity[off] & 0xFF;
    Preconditions.checkArgument(nsLen >= 1, "Namespace length must be at least 1, got %s", nsLen);
    Preconditions.checkArgument(off + nsLen <= offset + length,
      "keyIdentity too short for namespace length expected %s, got %s", off + nsLen,
      offset + length);
    return off;
  }

  private int getPartialIdentityOffset() {
    int off = getNamespaceOffset();
    int nsLen = keyIdentity[off++] & 0xFF;
    off += nsLen;
    if (off >= length) {
      return -1;
    }
    int partialLen = keyIdentity[off] & 0xFF;
    Preconditions.checkArgument(partialLen >= 0,
      "Partial identity length must be at least 0, got %s", partialLen);
    Preconditions.checkArgument(off + 1 + partialLen == offset + length,
      "keyIdentity too short for partialIdentity length expected %s, got %s", off + partialLen,
      offset + length);
    return off;
  }

  @Override
  public int getCustodianLength() {
    return keyIdentity[getCustodianOffset()] & 0xFF;
  }

  @Override
  public int getNamespaceLength() {
    return keyIdentity[getNamespaceOffset()] & 0xFF;
  }

  @Override
  public int getPartialIdentityLength() {
    int off = getPartialIdentityOffset();
    if (off == -1) {
      return 0;
    }
    return keyIdentity[off] & 0xFF;
  }

  @Override
  public KeyIdentitySingleArrayBacked clone() {
    return new KeyIdentitySingleArrayBacked(Bytes.copy(keyIdentity, offset, length), 0, length);
  }

  @Override
  public String getCustodianEncoded() {
    return ManagedKeyProvider.encodeToStr(keyIdentity, getCustodianOffset() + 1,
      getCustodianLength());
  }

  @Override
  public String getNamespaceString() {
    return Bytes.toString(keyIdentity, getNamespaceOffset() + 1, getNamespaceLength());
  }

  @Override
  public String getPartialIdentityEncoded() {
    int off = getPartialIdentityOffset();
    if (off == -1) {
      return null;
    }
    return ManagedKeyProvider.encodeToStr(keyIdentity, off + 1, getPartialIdentityLength());
  }

  @Override
  public int compareCustodian(byte[] otherCustodian) {
    return compareCustodian(otherCustodian, 0, otherCustodian.length);
  }

  @Override
  public int compareCustodian(byte[] otherCustodian, int otherOffset, int otherLength) {
    return Bytes.compareTo(keyIdentity, getCustodianOffset() + 1, getCustodianLength(),
      otherCustodian, otherOffset, otherLength);
  }

  @Override
  public int compareNamespace(byte[] otherNamespace) {
    return compareNamespace(otherNamespace, 0, otherNamespace.length);
  }

  @Override
  public int compareNamespace(byte[] otherNamespace, int otherOffset, int otherLength) {
    return Bytes.compareTo(keyIdentity, getNamespaceOffset() + 1, getNamespaceLength(),
      otherNamespace, otherOffset, otherLength);
  }

  @Override
  public int comparePartialIdentity(byte[] otherPartialIdentity) {
    return comparePartialIdentity(otherPartialIdentity, 0, otherPartialIdentity.length);
  }

  @Override
  public int comparePartialIdentity(byte[] otherPartialIdentity, int otherOffset, int otherLength) {
    int off = getPartialIdentityOffset();
    if (off == -1) { // partial identity segment omitted (length 0).
      if (otherLength == 0) {
        return 0;
      }
      return -1;
    }
    return Bytes.compareTo(keyIdentity, off + 1, getPartialIdentityLength(), otherPartialIdentity,
      otherOffset, otherLength);
  }
}
