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

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentity;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This class represents an encryption key data which includes the key itself, its state, metadata
 * and a prefix. The metadata encodes enough information on the key such that it can be used to
 * retrieve the exact same key again in the future. If the key state is
 * {@link ManagedKeyState#FAILED} expect the key to be {@code null}. The key data is represented by
 * the following fields:
 * <ul>
 * <li>key_cust: The prefix for which this key belongs to</li>
 * <li>theKey: The key capturing the bytes and encoding</li>
 * <li>keyState: The state of the key (see {@link ManagedKeyState})</li>
 * <li>keyMetadata: Metadata that identifies the key</li>
 * </ul>
 * The class provides methods to retrieve, as well as to compute a checksum for the key data. The
 * checksum is used to ensure the integrity of the key data. The class also provides a method to
 * generate a digest of the key metadata (partial identity), which can be used for validation and
 * identification.
 */
@InterfaceAudience.Public
public class ManagedKeyData {
  /**
   * Special value to be used for custodian or namespace to indicate that it is global, meaning it
   * is not associated with a specific custodian or namespace.
   */
  public static final String KEY_SPACE_GLOBAL = "*";

  /**
   * Byte representation of the {@link #KEY_SPACE_GLOBAL}.
   */
  public static final Bytes KEY_SPACE_GLOBAL_BYTES =
    new Bytes(KEY_SPACE_GLOBAL.getBytes(StandardCharsets.UTF_8));

  /**
   * Encoded form of global custodian.
   */
  public static final String GLOBAL_CUST_ENCODED =
    ManagedKeyProvider.encodeToStr(KEY_SPACE_GLOBAL_BYTES.get());

  /** Maximum length: key custodian in bytes, key namespace in bytes. */
  public static final short MAX_UNSIGNED_BYTE = 255;

  private final ManagedKeyIdentity keyIdentity;
  private final Key theKey;
  private final ManagedKeyState keyState;
  private final String keyMetadata;
  private final long refreshTimestamp;
  private volatile long keyChecksum = 0;

  /**
   * Constructs a new instance with the given parameters. Convenience constructor to build a
   * ManagedKeyIdentity from the (custodian, namespace and metadata).
   * @param key_cust      The key custodian.
   * @param key_namespace The key namespace as a byte array.
   * @param theKey        The actual key, can be {@code null}.
   * @param keyState      The state of the key.
   * @param keyMetadata   The metadata associated with the key.
   * @throws NullPointerException if any of key_cust, keyState or keyMetadata is null.
   */
  public ManagedKeyData(byte[] key_cust, byte[] key_namespace, Key theKey, ManagedKeyState keyState,
    String keyMetadata) {
    this(ManagedKeyIdentityUtils.buildIdentityFromMetadata(key_cust, key_namespace, keyMetadata),
      theKey, keyState, keyMetadata, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Constructs a new instance with the given parameters. Convenience constructor to build a
   * ManagedKeyIdentity from the (custodian, namespace and metadata).
   * @param key_cust      The key custodian.
   * @param key_namespace The key namespace as a byte array.
   * @param theKey        The actual key, can be {@code null}.
   * @param keyState      The state of the key.
   * @param keyMetadata   The metadata associated with the key.
   * @throws NullPointerException if any of key_cust, keyState or keyMetadata is null.
   */
  public ManagedKeyData(Bytes custodian, Bytes namespace, Key theKey, ManagedKeyState keyState,
    String keyMetadata) {
    this(ManagedKeyIdentityUtils.buildIdentityFromMetadata(custodian, namespace, keyMetadata),
      theKey, keyState, keyMetadata, EnvironmentEdgeManager.currentTime());
  }

  // ---------------------------------------------------------------------------
  // New FullKeyIdentity-based constructors — primary constructors that store fields.
  // ---------------------------------------------------------------------------

  /**
   * Constructs a new instance from a {@link ManagedKeyIdentity} with a key and metadata.
   * @param fullIdentity The full key identity (custodian + namespace + partial identity).
   * @param theKey       The actual key, can be {@code null}.
   * @param keyState     The state of the key.
   * @param keyMetadata  The metadata associated with the key.
   * @throws NullPointerException if any of fullIdentity, keyState or keyMetadata is null.
   */
  public ManagedKeyData(ManagedKeyIdentity fullIdentity, Key theKey, ManagedKeyState keyState,
    String keyMetadata) {
    this(fullIdentity, theKey, keyState, keyMetadata, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Constructs a new instance from a {@link ManagedKeyIdentity} with a key and metadata.
   * @param fullIdentity     The full key identity (custodian + namespace + partial identity).
   * @param theKey           The actual key, can be {@code null}.
   * @param keyState         The state of the key.
   * @param keyMetadata      The metadata associated with the key.
   * @param refreshTimestamp The refresh timestamp for the key.
   * @throws NullPointerException if any of fullIdentity, keyState or keyMetadata is null.
   */
  public ManagedKeyData(ManagedKeyIdentity fullIdentity, Key theKey, ManagedKeyState keyState,
    String keyMetadata, long refreshTimestamp) {
    Preconditions.checkNotNull(fullIdentity, "fullIdentity should not be null");
    Preconditions.checkNotNull(keyState, "keyState should not be null");
    Preconditions.checkNotNull(keyMetadata, "metadata should not be null");
    this.keyIdentity = fullIdentity;
    this.theKey = theKey;
    this.keyState = keyState;
    this.keyMetadata = keyMetadata;
    this.refreshTimestamp = refreshTimestamp;
  }

  /**
   * Constructs a new instance from a {@link ManagedKeyIdentity} without a key or metadata (used for
   * client-side or key-management-state instances).
   * @param fullIdentity The full key identity (custodian + namespace + partial identity).
   * @param keyState     The state of the key.
   * @throws NullPointerException if any of fullIdentity or keyState is null.
   */
  public ManagedKeyData(ManagedKeyIdentity fullIdentity, ManagedKeyState keyState) {
    this(fullIdentity, keyState, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Constructs a new instance from a {@link ManagedKeyIdentity} without a key or metadata (used for
   * client-side or key-management-state instances).
   * @param fullIdentity     The full key identity (custodian + namespace + partial identity).
   * @param keyState         The state of the key.
   * @param refreshTimestamp The refresh timestamp for the key.
   * @throws NullPointerException if any of fullIdentity or keyState is null.
   */
  public ManagedKeyData(ManagedKeyIdentity fullIdentity, ManagedKeyState keyState,
    long refreshTimestamp) {
    Preconditions.checkNotNull(fullIdentity, "fullIdentity should not be null");
    Preconditions.checkNotNull(keyState, "keyState should not be null");
    this.keyIdentity = fullIdentity;
    this.keyState = keyState;
    this.refreshTimestamp = refreshTimestamp;
    this.theKey = null;
    this.keyMetadata = null;
  }

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public ManagedKeyData createClientFacingInstance() {
    return new ManagedKeyData(keyIdentity, keyState, refreshTimestamp);
  }

  /** Returns The full key identity. */
  public ManagedKeyIdentity getKeyIdentity() {
    return keyIdentity;
  }

  /**
   * Returns the custodian associated with the key.
   * @return The key custodian as a byte array.
   */
  public byte[] getKeyCustodian() {
    return keyIdentity.copyCustodian();
  }

  /**
   * Return the key Custodian in Base64 encoded form.
   * @return the encoded key custodian
   */
  public String getKeyCustodianEncoded() {
    return keyIdentity.getCustodianEncoded();
  }

  /**
   * Returns the namespace associated with the key as a String. Intended for logging, RPC responses,
   * and external-facing display only. Internal code should use {@link #getKeyNamespaceBytes()} to
   * avoid unnecessary String allocation.
   * @return The namespace as a {@code String}.
   */
  public String getKeyNamespace() {
    return keyIdentity.getNamespaceString();
  }

  /**
   * Returns the namespace associated with the key as a byte array. Use this in internal keymeta
   * code (row-key building, cache keys, table accessor) to avoid String conversion.
   * @return The namespace as a byte array.
   */
  public byte[] getKeyNamespaceBytes() {
    return keyIdentity.copyNamespace();
  }

  /**
   * Returns the actual key.
   * @return The key as a {@code Key} object.
   */
  public Key getTheKey() {
    return theKey;
  }

  /**
   * Returns the state of the key.
   * @return The key state as a {@code ManagedKeyState} enum value.
   */
  public ManagedKeyState getKeyState() {
    return keyState;
  }

  /**
   * Returns the metadata associated with the key.
   * @return The key metadata as a {@code String}.
   */
  public String getKeyMetadata() {
    return keyMetadata;
  }

  /**
   * Returns the refresh timestamp of the key.
   * @return The refresh timestamp as a long value.
   */
  public long getRefreshTimestamp() {
    return refreshTimestamp;
  }

  @Override
  public String toString() {
    return "ManagedKeyData{" + "keyCustodian=" + Arrays.toString(getKeyCustodian())
      + ", keyNamespace='" + getKeyNamespace() + '\'' + ", keyState=" + keyState + ", keyMetadata='"
      + keyMetadata + '\'' + ", refreshTimestamp=" + refreshTimestamp + ", keyChecksum="
      + getKeyChecksum() + '}';
  }

  /**
   * Computes the checksum of the key. If the checksum has already been computed, this method
   * returns the previously computed value. The checksum is computed using the CRC32C algorithm.
   * @return The checksum of the key as a long value, {@code 0} if no key is available.
   */
  public long getKeyChecksum() {
    if (theKey == null) {
      return 0;
    }
    if (keyChecksum == 0) {
      keyChecksum = constructKeyChecksum(theKey.getEncoded());
    }
    return keyChecksum;
  }

  public static long constructKeyChecksum(byte[] data) {
    DataChecksum dataChecksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 16);
    dataChecksum.update(data, 0, data.length);
    return dataChecksum.getValue();
  }

  /**
   * Returns the partial identity (digest of key metadata). If the digest has already been computed,
   * this method returns the previously computed value.
   * @return The partial identity as a byte array, or {@code null} if not available.
   */
  public byte[] getPartialIdentity() {
    Bytes pi = keyIdentity.getPartialIdentityView();
    return pi.getLength() == 0 ? null : pi.copyBytesIfNecessary();
  }

  /**
   * Return the partial identity in Base64 encoded form.
   * @return the encoded partial identity or {@code null} if no metadata is available.
   */
  public String getPartialIdentityEncoded() {
    byte[] id = getPartialIdentity();
    if (id != null) {
      return ManagedKeyProvider.encodeToStr(id);
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ManagedKeyData that = (ManagedKeyData) o;

    return Objects.equals(keyIdentity, that.keyIdentity) && Objects.equals(theKey, that.theKey)
      && Objects.equals(keyState, that.keyState) && Objects.equals(keyMetadata, that.keyMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyIdentity, theKey, keyState, keyMetadata);
  }
}
