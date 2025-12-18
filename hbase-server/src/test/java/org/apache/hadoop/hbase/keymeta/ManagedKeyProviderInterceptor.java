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
import java.security.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.mockito.Mockito;

public class ManagedKeyProviderInterceptor extends MockManagedKeyProvider {
  public final MockManagedKeyProvider delegate;
  public final MockManagedKeyProvider spy;

  public ManagedKeyProviderInterceptor() {
    this.delegate = new MockManagedKeyProvider();
    this.spy = Mockito.spy(delegate);
  }

  @Override
  public void initConfig(Configuration conf, String providerParameters) {
    spy.initConfig(conf, providerParameters);
  }

  @Override
  public ManagedKeyData getManagedKey(byte[] custodian, String namespace) throws IOException {
    return spy.getManagedKey(custodian, namespace);
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] systemId) throws IOException {
    return spy.getSystemKey(systemId);
  }

  @Override
  public ManagedKeyData unwrapKey(String keyMetadata, byte[] wrappedKey) throws IOException {
    return spy.unwrapKey(keyMetadata, wrappedKey);
  }

  @Override
  public void init(String params) {
    spy.init(params);
  }

  @Override
  public Key getKey(String alias) {
    return spy.getKey(alias);
  }

  @Override
  public Key[] getKeys(String[] aliases) {
    return spy.getKeys(aliases);
  }

  @Override
  public void setMockedKeyState(String alias, ManagedKeyState state) {
    delegate.setMockedKeyState(alias, state);
  }

  @Override
  public void setMultikeyGenMode(boolean multikeyGenMode) {
    delegate.setMultikeyGenMode(multikeyGenMode);
  }

  @Override
  public ManagedKeyData getLastGeneratedKeyData(String alias, String keyNamespace) {
    return delegate.getLastGeneratedKeyData(alias, keyNamespace);
  }

  @Override
  public void setMockedKey(String alias, java.security.Key key, String keyNamespace) {
    delegate.setMockedKey(alias, key, keyNamespace);
  }
}
