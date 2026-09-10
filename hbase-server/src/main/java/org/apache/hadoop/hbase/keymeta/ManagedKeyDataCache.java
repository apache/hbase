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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * STUB IMPLEMENTATION - Feature not yet complete. This class will be fully implemented in
 * HBASE-29368 feature PR.
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache {
  public ManagedKeyDataCache(Configuration conf, KeymetaAdmin admin) {
    // Stub constructor - does nothing
  }

  public ManagedKeyData getActiveEntry(byte[] keyCustodian, String keyNamespace) {
    return null;
  }

  public ManagedKeyData getEntry(byte[] keyCustodian, String keyNamespace, String keyMetadata,
    byte[] wrappedKey) throws IOException, KeyException {
    return null;
  }

  public void clearCache() {
    // Stub - does nothing
  }

  public void ejectEntry(byte[] keyCustodian, String keyNamespace, String keyMetadata) {
    // Stub - does nothing
  }

  public boolean ejectKey(byte[] keyCustodian, String keyNamespace, byte[] keyMetadataHash) {
    return false;
  }
}
