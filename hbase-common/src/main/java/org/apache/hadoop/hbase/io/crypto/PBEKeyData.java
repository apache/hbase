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
 * This class represents an encryption key data which includes the key itself, its status, metadata
 * and a prefix. The metadata encodes enough information on the key such that it can be used to
 * retrieve the exact same key again in the future. If the key status is {@link PBEKeyStatus#FAILED}
 * expect the key to be {@code null}.
 *
 * The key data is represented by the following fields:
 * <ul>
 * <li>pbe_prefix: The prefix for which this key belongs to</li>
 * <li>theKey: The key capturing the bytes and encoding</li>
 * <li>keyStatus: The status of the key (see {@link PBEKeyStatus})</li>
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
public class PBEKeyData {
  public static final String KEY_NAMESPACE_GLOBAL = "*";

  private final byte[] pbePrefix;
  private final String keyNamespace;
  private final Key theKey;
  private final PBEKeyStatus keyStatus;
  private final String keyMetadata;
  private final long refreshTimestamp;
  private final long readOpCount;
  private final long writeOpCount;
  private volatile long keyChecksum = 0;
  private byte[] keyMetadataHash;

  /**
   * Constructs a new instance with the given parameters.
   *
   * @param pbe_prefix   The PBE prefix associated with the key.
   * @param theKey       The actual key, can be {@code null}.
   * @param keyStatus    The status of the key.
   * @param keyMetadata  The metadata associated with the key.
   * @throws NullPointerException if any of pbe_prefix, keyStatus or keyMetadata is null.
   */
  public PBEKeyData(byte[] pbe_prefix, String key_namespace, Key theKey, PBEKeyStatus keyStatus,
      String keyMetadata) {
    this(pbe_prefix, key_namespace, theKey, keyStatus, keyMetadata,
      EnvironmentEdgeManager.currentTime(), 0, 0);
  }

  /**
   * Constructs a new instance with the given parameters.
   *
   * @param pbe_prefix       The PBE prefix associated with the key.
   * @param theKey           The actual key, can be {@code null}.
   * @param keyStatus        The status of the key.
   * @param keyMetadata      The metadata associated with the key.
   * @param refreshTimestamp The timestamp when this key was last refreshed.
   * @param readOpCount      The current number of read operations for this key.
   * @param writeOpCount     The current number of write operations for this key.
   * @throws NullPointerException if any of pbe_prefix, keyStatus or keyMetadata is null.
   */
  public PBEKeyData(byte[] pbe_prefix, String key_namespace, Key theKey, PBEKeyStatus keyStatus,
      String keyMetadata, long refreshTimestamp, long readOpCount, long writeOpCount) {
    Preconditions.checkNotNull(pbe_prefix, "pbe_prefix should not be null");
    Preconditions.checkNotNull(key_namespace, "key_namespace should not be null");
    Preconditions.checkNotNull(keyStatus,  "keyStatus should not be null");
    Preconditions.checkNotNull(keyMetadata, "keyMetadata should not be null");
    Preconditions.checkArgument(readOpCount >= 0, "readOpCount: " + readOpCount +
      " should be >= 0");
    Preconditions.checkArgument(writeOpCount >= 0, "writeOpCount: " + writeOpCount +
      " should be >= 0");

    this.pbePrefix = pbe_prefix;
    this.keyNamespace = key_namespace;
    this.theKey = theKey;
    this.keyStatus = keyStatus;
    this.keyMetadata = keyMetadata;
    this.refreshTimestamp = refreshTimestamp;
    this.readOpCount = readOpCount;
    this.writeOpCount = writeOpCount;
  }

  /**
   * Returns the PBE prefix associated with the key.
   *
   * @return The PBE prefix as a byte array.
   */
  public byte[] getPBEPrefix() {
    return pbePrefix;
  }

  /**
   * Return the PBE prefix in Base64 encoded form.
   * @return the encoded PBE prefix.
   */
  public String getPBEPrefixEncoded() {
    return Base64.getEncoder().encodeToString(pbePrefix);
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
   * Returns the status of the key.
   *
   * @return The key status as a {@code PBEKeyStatus} enum value.
   */
  public PBEKeyStatus getKeyStatus() {
    return keyStatus;
  }

  /**
   * Returns the metadata associated with the key.
   *
   * @return The key metadata as a {@code String}.
   */
  public String getKeyMetadata() {
    return keyMetadata;
  }

  @Override public String toString() {
    return "PBEKeyData{" + "pbePrefix=" + Arrays.toString(pbePrefix) + ", keyNamespace='"
      + keyNamespace + '\'' + ", keyStatus=" + keyStatus + ", keyMetadata='" + keyMetadata + '\''
      + ", refreshTimestamp=" + refreshTimestamp + '}';
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
   * @return The checksum of the key as a long value.
   */
  public long getKeyChecksum() {
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
    if (keyMetadataHash == null) {
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

    PBEKeyData that = (PBEKeyData) o;

    return new EqualsBuilder()
      .append(pbePrefix, that.pbePrefix)
      .append(keyNamespace, that.keyNamespace)
      .append(theKey, that.theKey)
      .append(keyStatus, that.keyStatus)
      .append(keyMetadata, that.keyMetadata)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(pbePrefix)
      .append(keyNamespace)
      .append(theKey)
      .append(keyStatus)
      .append(keyMetadata)
      .toHashCode();
  }
}
