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

import java.io.IOException;
import java.security.KeyException;
import java.util.List;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * KeymetaAdmin is an interface for administrative functions related to managed keys. It handles the
 * following methods:
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface KeymetaAdmin {
  /**
   * Enables key management for the specified custodian and namespace.
   * @param keyCust      The key custodian identifier.
   * @param keyNamespace The namespace for the key management.
   * @return The list of {@link ManagedKeyData} objects each identifying the key and its current
   *         status.
   * @throws IOException if an error occurs while enabling key management.
   */
  ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Get the status of all the keys for the specified custodian.
   * @param keyCust      The key custodian identifier.
   * @param keyNamespace The namespace for the key management.
   * @return The list of {@link ManagedKeyData} objects each identifying the key and its current
   *         status.
   * @throws IOException if an error occurs while enabling key management.
   */
  List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Triggers rotation of the System Key (STK) by checking for a new key and propagating it to all
   * region servers.
   * @return true if a new STK was found and rotated, false if no change was detected
   * @throws IOException if an error occurs while rotating the STK
   */
  boolean rotateSTK() throws IOException;

  /**
   * Eject a specific managed key entry from the managed key data cache on all live region servers.
   * @param keyCustodian the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata
   * @throws IOException if an error occurs while ejecting the key
   */
  void ejectManagedKeyDataCacheEntry(byte[] keyCustodian, String keyNamespace, String keyMetadata)
    throws IOException;

  /**
   * Clear all entries in the managed key data cache on all live region servers.
   * @throws IOException if an error occurs while clearing the cache
   */
  void clearManagedKeyDataCache() throws IOException;

  /**
   * Disables key management for the specified custodian and namespace. This marks any ACTIVE keys
   * as INACTIVE and adds a DISABLED state marker such that no new ACTIVE key is retrieved, so the
   * new data written will not be encrypted.
   * @param keyCust      The key custodian identifier.
   * @param keyNamespace The namespace for the key management.
   * @return The {@link ManagedKeyData} object identifying the previously active key and its current
   *         state.
   * @throws IOException  if an error occurs while disabling key management.
   * @throws KeyException if an error occurs while disabling key management.
   */
  ManagedKeyData disableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Disables the specific managed key identified by the specified custodian, namespace, and
   * metadata hash.
   * @param keyCust         The key custodian identifier.
   * @param keyNamespace    The namespace for the key management.
   * @param keyMetadataHash The key metadata hash.
   * @return A {@link ManagedKeyData} object identifying the key and its current status.
   * @throws IOException  if an error occurs while disabling the managed key.
   * @throws KeyException if an error occurs while disabling the managed key.
   */
  ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace, byte[] keyMetadataHash)
    throws IOException, KeyException;

  /**
   * Attempt a key rotation for the active key of the specified custodian and namespace.
   * @param keyCust      The key custodian identifier.
   * @param keyNamespace The namespace for the key management.
   * @return A {@link ManagedKeyData} object identifying the key and its current status.
   * @throws IOException  if an error occurs while rotating the managed key.
   * @throws KeyException if an error occurs while rotating the managed key.
   */
  ManagedKeyData rotateManagedKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Refresh all the keymeta entries for the specified custodian and namespace.
   * @param keyCust      The key custodian identifier.
   * @param keyNamespace The namespace for the key management.
   * @throws IOException  if an error occurs while refreshing managed keys.
   * @throws KeyException if an error occurs while refreshing managed keys.
   */
  void refreshManagedKeys(byte[] keyCust, String keyNamespace) throws IOException, KeyException;
}
