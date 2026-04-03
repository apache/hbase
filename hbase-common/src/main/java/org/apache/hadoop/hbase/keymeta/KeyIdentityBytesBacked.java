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
 * Immutable value type representing a key identity (custodian + namespace + partial identity)
 * backed by three {@link Bytes} objects. The backing {@link Bytes} objects will not be duplicated
 * for the sake of optimizing the memory usage, so only share those {@link Bytes} objects that are
 * truly immutable and owned by the caller. The same applies to the conveienice constructor that
 * takes byte arrays, as they are simply wrapped in {@link Bytes} objects. While the partial
 * identity is optional, it can't be {@code null} instead a zero-length array is accepted, however
 * in such cases, {@link KeyIdentityPrefixBytesBacked} is the more efficient alternative.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class KeyIdentityBytesBacked implements ManagedKeyIdentity {
  private final Bytes custodian;
  private final Bytes namespace;
  private final Bytes partialIdentity;

  public KeyIdentityBytesBacked(byte[] custodian, byte[] namespace, byte[] partialIdentity) {
    this(custodian == null ? null : new Bytes(custodian),
      namespace == null ? null : new Bytes(namespace),
      partialIdentity == null ? null : new Bytes(partialIdentity));
  }

  public KeyIdentityBytesBacked(Bytes custodian, Bytes namespace, Bytes partialIdentity) {
    Preconditions.checkNotNull(custodian, "custodian cannot be null");
    Preconditions.checkNotNull(namespace, "namespace cannot be null");
    Preconditions.checkNotNull(partialIdentity, "partialIdentity cannot be null");
    Preconditions.checkArgument(custodian.getLength() >= 1,
      "Custodian length must be at least 1, got %s", custodian.getLength());
    Preconditions.checkArgument(namespace.getLength() >= 1,
      "Namespace length must be at least 1, got %s", namespace.getLength());
    Preconditions.checkArgument(partialIdentity.getLength() >= 0,
      "Partial identity length must be >= 0, got %s", partialIdentity.getLength());
    this.custodian = custodian;
    this.namespace = namespace;
    this.partialIdentity = partialIdentity;
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
    return partialIdentity;
  }

  @Override
  public Bytes getFullIdentityView() {
    return new Bytes(ManagedKeyIdentityUtils.constructRowKeyForIdentity(custodian.get(),
      namespace.get(), partialIdentity.get()));
  }

  @Override
  public Bytes getIdentityPrefixView() {
    return new Bytes(
      ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(custodian.get(), namespace.get()));
  }

  @Override
  public ManagedKeyIdentity getKeyIdentityPrefix() {
    if (partialIdentity.getLength() == 0) {
      return this;
    }
    return new KeyIdentityPrefixBytesBacked(custodian, namespace);
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
    return partialIdentity.copyBytes();
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
  public KeyIdentityBytesBacked clone() {
    return new KeyIdentityBytesBacked(custodian.clone(), namespace.clone(),
      partialIdentity.clone());
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
    return partialIdentity.getLength();
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
    return ManagedKeyProvider.encodeToStr(partialIdentity.get());
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
    return comparePartialIdentity(otherPartialIdentity, 0, otherPartialIdentity.length);
  }

  @Override
  public int comparePartialIdentity(byte[] otherPartialIdentity, int otherOffset, int otherLength) {
    return partialIdentity.compareTo(otherPartialIdentity, otherOffset, otherLength);
  }
}
