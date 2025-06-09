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
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/**
 * This class represents an encryption key data which includes the key itself, its state, metadata
 * and a prefix. The metadata encodes enough information on the key such that it can be used to
 * retrieve the exact same key again in the future. If the key state is {@link ManagedKeyState#FAILED}
 * expect the key to be {@code null}.
 *
 * The key data is represented by the following fields:
 * <ul>
 * <li>key_cust: The prefix for which this key belongs to</li>
 * <li>theKey: The key capturing the bytes and encoding</li>
 * <li>keyState: The state of the key (see {@link ManagedKeyState})</li>
 * <li>keyMetadata: Metadata that identifies the key</li>
 * </ul>
 *
 * The class provides methods to retrieve, as well as to compute a checksum
 * for the key data. The checksum is used to ensure the integrity of the key data.
 *
 * The class also provides a method to generate an MD5 hash of the key metadata, which can be used
 * for validation and identification.
 */
@InterfaceAudience.Public
public class ManagedKeyData {
  /**
   * Special value to be used for custodian or namespace to indicate that it is global, meaning it
   * is not associated with a specific custodian or namespace.
   */
  public static final String KEY_SPACE_GLOBAL = "*";

  /**
   * Encoded form of global custodian.
   */
  public static final String KEY_GLOBAL_CUSTODIAN =
    ManagedKeyProvider.encodeToStr(KEY_SPACE_GLOBAL.getBytes());

  private final byte[] keyCust;
  private final String keyNamespace;
  private final Key theKey;
  private final ManagedKeyState keyState;
  private final String keyMetadata;
  private final long refreshTimestamp;
  private final long readOpCount;
  private final long writeOpCount;
  private volatile long keyChecksum = 0;
  private byte[] keyMetadataHash;

  /**
   * Constructs a new instance with the given parameters.
   *
   * @param key_cust     The key custodian.
   * @param theKey       The actual key, can be {@code null}.
   * @param keyState    The state of the key.
   * @param keyMetadata  The metadata associated with the key.
   * @throws NullPointerException if any of key_cust, keyState or keyMetadata is null.
   */
  public ManagedKeyData(byte[] key_cust, String key_namespace, Key theKey, ManagedKeyState keyState,
                        String keyMetadata) {
    this(key_cust, key_namespace, theKey, keyState, keyMetadata,
      EnvironmentEdgeManager.currentTime(), 0, 0);
  }

  /**
   * Constructs a new instance with the given parameters.
   *
   * @param key_cust         The key custodian.
   * @param theKey           The actual key, can be {@code null}.
   * @param keyState        The state of the key.
   * @param keyMetadata      The metadata associated with the key.
   * @param refreshTimestamp The timestamp when this key was last refreshed.
   * @param readOpCount      The current number of read operations for this key.
   * @param writeOpCount     The current number of write operations for this key.
   * @throws NullPointerException if any of key_cust, keyState or keyMetadata is null.
   */
  public ManagedKeyData(byte[] key_cust, String key_namespace, Key theKey, ManagedKeyState keyState,
                        String keyMetadata, long refreshTimestamp, long readOpCount,
                        long writeOpCount) {
    Preconditions.checkNotNull(key_cust, "key_cust should not be null");
    Preconditions.checkNotNull(key_namespace, "key_namespace should not be null");
    Preconditions.checkNotNull(keyState,  "keyState should not be null");
    // Only check for null metadata if state is not FAILED
    if (keyState != ManagedKeyState.FAILED) {
      Preconditions.checkNotNull(keyMetadata, "keyMetadata should not be null");
    }
    Preconditions.checkArgument(readOpCount >= 0, "readOpCount: " + readOpCount +
      " should be >= 0");
    Preconditions.checkArgument(writeOpCount >= 0, "writeOpCount: " + writeOpCount +
      " should be >= 0");

    this.keyCust = key_cust;
    this.keyNamespace = key_namespace;
    this.theKey = theKey;
    this.keyState = keyState;
    this.keyMetadata = keyMetadata;
    this.refreshTimestamp = refreshTimestamp;
    this.readOpCount = readOpCount;
    this.writeOpCount = writeOpCount;
  }

  @VisibleForTesting
  public ManagedKeyData cloneWithoutKey() {
    return new ManagedKeyData(keyCust, keyNamespace, null, keyState, keyMetadata,
      refreshTimestamp, readOpCount, writeOpCount);
  }

  /**
   * Returns the custodian associated with the key.
   *
   * @return The key custodian as a byte array.
   */
  public byte[] getKeyCustodian() {
    return keyCust;
  }

  /**
   * Return the key Custodian in Base64 encoded form.
   * @return the encoded key custodian
   */
  public String getKeyCustodianEncoded() {
    return Base64.getEncoder().encodeToString(keyCust);
  }


  /**
   * Returns the namespace associated with the key.
   *
   * @return The namespace as a {@code String}.
   */
  public String getKeyNamespace() {
    return keyNamespace;
  }

  /**
   * Returns the actual key.
   *
   * @return The key as a {@code Key} object.
   */
  public Key getTheKey() {
    return theKey;
  }

  /**
   * Returns the state of the key.
   *
   * @return The key state as a {@code ManagedKeyState} enum value.
   */
  public ManagedKeyState getKeyState() {
    return keyState;
  }

  /**
   * Returns the metadata associated with the key.
   *
   * @return The key metadata as a {@code String}.
   */
  public String getKeyMetadata() {
    return keyMetadata;
  }

  @Override
  public String toString() {
    return "ManagedKeyData{" + "keyCustodian=" + Arrays.toString(keyCust) + ", keyNamespace='"
      + keyNamespace + '\'' + ", keyState=" + keyState + ", keyMetadata='" + keyMetadata + '\''
      + ", refreshTimestamp=" + refreshTimestamp + ", keyChecksum=" + getKeyChecksum() + '}';
  }

  public long getRefreshTimestamp() {
    return refreshTimestamp;
  }

  /**
   * @return the number of times this key has been used for read operations as of the time this
   * key data was initialized.
   */
  public long getReadOpCount() {
    return readOpCount;
  }

  /**
   * @return the number of times this key has been used for write operations as of the time this
   * key data was initialized.
   */
  public long getWriteOpCount() {
    return writeOpCount;
  }

  /**
   * Computes the checksum of the key. If the checksum has already been computed, this method
   * returns the previously computed value. The checksum is computed using the CRC32C algorithm.
   *
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
   * Computes the hash of the key metadata. If the hash has already been computed, this method
   * returns the previously computed value. The hash is computed using the MD5 algorithm.
   *
   * @return The hash of the key metadata as a byte array.
   */
  public byte[] getKeyMetadataHash() {
    if (keyMetadataHash == null && keyMetadata != null) {
      keyMetadataHash = constructMetadataHash(keyMetadata);
    }
    return keyMetadataHash;
  }

  /**
   * Return the hash of key metadata in Base64 encoded form.
   * @return the encoded hash or {@code null} if no meatadata is available.
   */
  public String getKeyMetadataHashEncoded() {
    byte[] hash = getKeyMetadataHash();
    if (hash != null) {
      return Base64.getEncoder().encodeToString(hash);
    }
    return null;
  }

  public static byte[] constructMetadataHash(String metadata) {
    MessageDigest md5;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    return md5.digest(metadata.getBytes());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    ManagedKeyData that = (ManagedKeyData) o;

    return new EqualsBuilder()
      .append(keyCust, that.keyCust)
      .append(keyNamespace, that.keyNamespace)
      .append(theKey, that.theKey)
      .append(keyState, that.keyState)
      .append(keyMetadata, that.keyMetadata)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(keyCust)
      .append(keyNamespace)
      .append(theKey)
      .append(keyState)
      .append(keyMetadata)
      .toHashCode();
  }
}
