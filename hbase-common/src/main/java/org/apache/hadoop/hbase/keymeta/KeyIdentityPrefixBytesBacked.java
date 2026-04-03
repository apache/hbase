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

import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Immutable value type for custodian + namespace only (same binary prefix as
 * {@link ManagedKeyIdentityUtils#constructRowKeyForCustodianNamespace(byte[], byte[])}). Use as a
 * {@link ManagedKeyIdentity} where partial identity is intentionally absent, equivalent to
 * {@link KeyIdentityBytesBacked} with {@link ManagedKeyIdentity#KEY_NULL_IDENTITY_BYTES} for the
 * partial segment but without storing a third backing array so it is more space efficient. The
 * backing {@link Bytes} objects will not be duplicated for the sake of optimizing the memory usage,
 * so only share those {@link Bytes} objects that are truly immutable and owned by the caller. The
 * same applies to the conveienice constructor that takes byte arrays, as they are simply wrapped in
 * {@link Bytes} objects.
 * <p>
 * {@link #getPartialIdentityView()} returns {@link ManagedKeyIdentity#KEY_NULL_IDENTITY_BYTES} so
 * {@link ManagedKeyIdentity#contentEquals} and {@link ManagedKeyIdentity#contentHashCode} remain
 * consistent with a bytes-backed identity that uses that sentinel. Operations that copy, encode, or
 * lexicographically compare the partial segment throw {@link UnsupportedOperationException} because
 * there is no distinct partial payload (only the shared empty sentinel).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class KeyIdentityPrefixBytesBacked implements ManagedKeyIdentity {
  private static final String NO_PARTIAL =
    "This identity has no partial identity segment; use " + ManagedKeyData.class.getSimpleName()
      + ".KEY_NULL_IDENTITY_BYTES via getPartialIdentityView() if an empty view is required";

  private final Bytes custodian;
  private final Bytes namespace;

  public KeyIdentityPrefixBytesBacked(byte[] custodian, byte[] namespace) {
    this(custodian == null ? null : new Bytes(custodian),
      namespace == null ? null : new Bytes(namespace));
  }

  public KeyIdentityPrefixBytesBacked(Bytes custodian, Bytes namespace) {
    Preconditions.checkNotNull(custodian, "custodian cannot be null");
    Preconditions.checkNotNull(namespace, "namespace cannot be null");
    Preconditions.checkArgument(custodian.getLength() >= 1,
      "Custodian length must be at least 1, got %s", custodian.getLength());
    Preconditions.checkArgument(namespace.getLength() >= 1,
      "Namespace length must be at least 1, got %s", namespace.getLength());
    this.custodian = custodian;
    this.namespace = namespace;
  }

  @Override
  public Bytes getCustodianView() {
    return custodian;
  }

  @Override
  public Bytes getNamespaceView() {
    return namespace;
  }

  @Override
  public Bytes getPartialIdentityView() {
    return ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES;
  }

  @Override
  public Bytes getFullIdentityView() {
    throw new UnsupportedOperationException(NO_PARTIAL);
  }

  @Override
  public Bytes getIdentityPrefixView() {
    return new Bytes(
      ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(custodian.get(), namespace.get()));
  }

  @Override
  public ManagedKeyIdentity getKeyIdentityPrefix() {
    return this;
  }

  @Override
  public byte[] copyCustodian() {
    return custodian.copyBytes();
  }

  @Override
  public byte[] copyNamespace() {
    return namespace.copyBytes();
  }

  @Override
  public byte[] copyPartialIdentity() {
    throw new UnsupportedOperationException(NO_PARTIAL);
  }

  @Override
  public boolean equals(Object obj) {
    return ManagedKeyIdentity.contentEquals(this, obj);
  }

  @Override
  public int hashCode() {
    return ManagedKeyIdentity.contentHashCode(this);
  }

  @Override
  public KeyIdentityPrefixBytesBacked clone() {
    return new KeyIdentityPrefixBytesBacked(custodian.clone(), namespace.clone());
  }

  @Override
  public int getCustodianLength() {
    return custodian.getLength();
  }

  @Override
  public int getNamespaceLength() {
    return namespace.getLength();
  }

  @Override
  public int getPartialIdentityLength() {
    return 0;
  }

  @Override
  public String getCustodianEncoded() {
    return ManagedKeyProvider.encodeToStr(custodian.get());
  }

  @Override
  public String getNamespaceString() {
    return Bytes.toString(namespace.get());
  }

  @Override
  public String getPartialIdentityEncoded() {
    throw new UnsupportedOperationException(NO_PARTIAL);
  }

  @Override
  public int compareCustodian(byte[] otherCustodian) {
    return compareCustodian(otherCustodian, 0, otherCustodian.length);
  }

  @Override
  public int compareCustodian(byte[] otherCustodian, int otherOffset, int otherLength) {
    return custodian.compareTo(otherCustodian, otherOffset, otherLength);
  }

  @Override
  public int compareNamespace(byte[] otherNamespace) {
    return compareNamespace(otherNamespace, 0, otherNamespace.length);
  }

  @Override
  public int compareNamespace(byte[] otherNamespace, int otherOffset, int otherLength) {
    return namespace.compareTo(otherNamespace, otherOffset, otherLength);
  }

  @Override
  public int comparePartialIdentity(byte[] otherPartialIdentity) {
    throw new UnsupportedOperationException(NO_PARTIAL);
  }

  @Override
  public int comparePartialIdentity(byte[] otherPartialIdentity, int otherOffset, int otherLength) {
    throw new UnsupportedOperationException(NO_PARTIAL);
  }
}
