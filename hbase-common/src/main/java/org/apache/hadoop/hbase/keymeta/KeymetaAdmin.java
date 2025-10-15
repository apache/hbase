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

/**
 * KeymetaAdmin is an interface for administrative functions related to managed keys. It handles the
 * following methods:
 */
@InterfaceAudience.Public
public interface KeymetaAdmin {
  /**
   * Enables key management for the specified custodian and namespace.
   * @param keyCust      The key custodian in base64 encoded format.
   * @param keyNamespace The namespace for the key management.
   * @return The list of {@link ManagedKeyData} objects each identifying the key and its current
   *         status.
   * @throws IOException if an error occurs while enabling key management.
   */
  List<ManagedKeyData> enableKeyManagement(String keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Get the status of all the keys for the specified custodian.
   * @param keyCust      The key custodian in base64 encoded format.
   * @param keyNamespace The namespace for the key management.
   * @return The list of {@link ManagedKeyData} objects each identifying the key and its current
   *         status.
   * @throws IOException if an error occurs while enabling key management.
   */
  List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException;

  /**
   * Triggers rotation of the System Key (STK) by checking for a new key and propagating it
   * to all region servers.
   * @return true if a new STK was found and rotated, false if no change was detected
   * @throws IOException if an error occurs while rotating the STK
   */
  boolean rotateSTK() throws IOException;
}
