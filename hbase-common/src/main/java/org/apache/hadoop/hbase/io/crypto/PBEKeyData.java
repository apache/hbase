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

  private byte[] pbe_prefix;
  private String key_namespace;
  private Key theKey;
  private PBEKeyStatus keyStatus;
  private String keyMetadata;
  private long refreshTimestamp;
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
      EnvironmentEdgeManager.currentTime());
  }

  /**
   * Constructs a new instance with the given parameters.
   *
   * @param pbe_prefix   The PBE prefix associated with the key.
   * @param theKey       The actual key, can be {@code null}.
   * @param keyStatus    The status of the key.
   * @param keyMetadata  The metadata associated with the key.
   * @param refreshTimestamp The timestamp when this key was last refreshed.
   * @throws NullPointerException if any of pbe_prefix, keyStatus or keyMetadata is null.
   */
  public PBEKeyData(byte[] pbe_prefix, String key_namespace, Key theKey, PBEKeyStatus keyStatus,
      String keyMetadata, long refreshTimestamp) {
    Preconditions.checkNotNull(pbe_prefix, "pbe_prefix should not be null");
    Preconditions.checkNotNull(key_namespace, "key_namespace should not be null");
    Preconditions.checkNotNull(keyStatus,  "keyStatus should not be null");
    Preconditions.checkNotNull(keyMetadata, "keyMetadata should not be null");

    this.pbe_prefix = pbe_prefix;
    this.key_namespace = key_namespace;
    this.theKey = theKey;
    this.keyStatus = keyStatus;
    this.keyMetadata = keyMetadata;
    this.refreshTimestamp = refreshTimestamp;
  }

  /**
   * Returns the PBE prefix associated with the key.
   *
   * @return The PBE prefix as a byte array.
   */
  public byte[] getPbe_prefix() {
    return pbe_prefix;
  }

  /**
   * Returns the namespace associated with the key.
   *
   * @return The namespace as a {@code String}.
   */
  public String getKeyNamespace() {
    return key_namespace;
  }

  /**
   * Returns the namespace associated with the key.
   *
   * @return The namespace as a {@code String}.
   */
  public String getKey_namespace() {
    return key_namespace;
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

  public long getRefreshTimestamp() {
    return refreshTimestamp;
  }

  /**
   * Computes the checksum of the key. If the checksum has already been computed, this method
   * returns the previously computed value. The checksum is computed using the CRC32C algorithm.
   *
   * @return The checksum of the key as a long value.
   */
  public long getKeyChecksum() {
    if (keyChecksum == 0) {
      DataChecksum dataChecksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 16);
      byte[] data = theKey.getEncoded();
      dataChecksum.update(data, 0, data.length);
      keyChecksum = dataChecksum.getValue();
    }
    return keyChecksum;
  }

  /**
   * Computes the hash of the key metadata. If the hash has already been computed, this method
   * returns the previously computed value. The hash is computed using the MD5 algorithm.
   *
   * @return The hash of the key metadata as a byte array.
   */
  public byte[] getKeyMetadataHash() {
    if (keyMetadataHash == null) {
      keyMetadataHash = makeMetadataHash(keyMetadata);
    }
    return keyMetadataHash;
  }

  public static byte[] makeMetadataHash(String metadata) {
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
      .append(pbe_prefix, that.pbe_prefix)
      .append(key_namespace, that.key_namespace)
      .append(theKey, that.theKey)
      .append(keyStatus, that.keyStatus)
      .append(keyMetadata, that.keyMetadata)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(pbe_prefix)
      .append(key_namespace)
      .append(theKey)
      .append(keyStatus)
      .append(keyMetadata)
      .toHashCode();
  }
}
