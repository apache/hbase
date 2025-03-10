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
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.KeyException;
import java.util.List;

/**
 * This class provides a unified access on top of both {@code PBEKeyDataCache} (L1) and
 * {@code PBEKeymetaTableAccessor} (L2) to access PBE keys. When the getter is called, it first
 * checks if L1 cache has the key, if not, it tries to get the key from L2.
 */
@InterfaceAudience.Private
public class PBEKeyAccessor {
  private final PBEKeyDataCache keyDataCache;
  private final PBEKeymetaTableAccessor keymetaAccessor;

  public PBEKeyAccessor(PBEKeymetaTableAccessor keymetaAccessor) {
    this.keymetaAccessor = keymetaAccessor;
    keyDataCache = new PBEKeyDataCache();
  }

  /**
   * Get key data by key metadata.
   *
   * @param pbePrefix The prefix of the key
   * @param keyNamespace The namespace of the key
   * @param keyMetadata The metadata of the key
   * @return The key data
   * @throws IOException if an error occurs while retrieving the key
   */
  public PBEKeyData getKey(byte[] pbePrefix, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    PBEKeyData keyData = keyDataCache.getEntry(keyMetadata);
    if (keyData == null) {
      keyData = keymetaAccessor.getKey(pbePrefix, keyNamespace, keyMetadata);
      keyDataCache.addEntry(keyData);
    }
    return keyData;
  }

  /**
   * Get an active key for the given prefix suitable for use in encryption.
   *
   * @param pbePrefix The prefix of the key
   * @param keyNamespace The namespace of the key
   * @return The key data
   * @throws IOException if an error occurs while retrieving the key
   */
  public PBEKeyData getAnActiveKey(byte[] pbePrefix, String keyNamespace)
    throws IOException, KeyException {
    PBEKeyData keyData = keyDataCache.getRandomEntryForPrefix(pbePrefix, keyNamespace);
    if (keyData == null) {
      List<PBEKeyData> activeKeys = keymetaAccessor.getActiveKeys(pbePrefix, keyNamespace);
      for (PBEKeyData kd: activeKeys) {
        keyDataCache.addEntry(kd);
      }
      keyData = keyDataCache.getRandomEntryForPrefix(pbePrefix, keyNamespace);
    }
    return keyData;
  }
}
