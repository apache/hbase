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
  public void initConfig(Configuration conf) {
    spy.initConfig(conf);
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