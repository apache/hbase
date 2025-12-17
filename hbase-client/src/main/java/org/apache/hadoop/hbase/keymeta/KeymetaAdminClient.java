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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * STUB IMPLEMENTATION - Feature not yet complete. This class will be fully implemented in
 * HBASE-29368 feature PR.
 */
@InterfaceAudience.Public
public class KeymetaAdminClient implements KeymetaAdmin {

  public KeymetaAdminClient(Connection conn) throws IOException {
    // Stub constructor
  }

  @Override
  public ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public boolean rotateSTK() throws IOException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public void ejectManagedKeyDataCacheEntry(byte[] keyCustodian, String keyNamespace,
    String keyMetadata) throws IOException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public void clearManagedKeyDataCache() throws IOException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public ManagedKeyData disableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public ManagedKeyData rotateManagedKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }

  @Override
  public void refreshManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    throw new UnsupportedOperationException("KeymetaAdmin feature not yet implemented");
  }
}
