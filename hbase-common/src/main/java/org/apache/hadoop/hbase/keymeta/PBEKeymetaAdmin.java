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

import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.KeyException;
import java.util.List;

/**
 * PBEKeymetaAdmin is an interface for administrative functions related to PBE keys.
 * It handles the following methods:
 *
 * <ul>
 * <li>enablePBE(): Enables PBE for a given pbe_prefix and namespace.</li>
 * </ul>
 */
@InterfaceAudience.Public
public interface PBEKeymetaAdmin {
  /**
   * Enables PBE for the specified key prefix and namespace.
   *
   * @param pbePrefix    The prefix for the PBE key in base64 encoded format.
   * @param keyNamespace The namespace for the PBE key.
   *
   * @return The current status of the PBE key.
   * @throws IOException if an error occurs while enabling PBE.
   */
  PBEKeyStatus enablePBE(String pbePrefix, String keyNamespace) throws IOException;

  /**
   * Get the status of all the keys for the specified pbe_prefix.
   *
   * @param pbePrefix    The prefix for the PBE key in base64 encoded format.
   * @param keyNamespace The namespace for the PBE key.
   * @return The list of status objects each identifying the key and its current status.
   * @throws IOException if an error occurs while enabling PBE.
   */
  List<PBEKeyData> getPBEKeyStatuses(String pbePrefix, String keyNamespace)
    throws IOException, KeyException;
}
