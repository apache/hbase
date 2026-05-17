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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestEncryptionTest {

  @Test
  public void testTestKeyProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    EncryptionTest.testKeyProvider(conf);
  }

  @Test
  public void testBadKeyProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, FailingKeyProvider.class.getName());
    assertThrows(IOException.class, () -> {
      EncryptionTest.testKeyProvider(conf);
    });
  }

  @Test
  public void testDefaultCipherProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, DefaultCipherProvider.class.getName());
    EncryptionTest.testCipherProvider(conf);
  }

  @Test
  public void testBadCipherProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, FailingCipherProvider.class.getName());
    assertThrows(IOException.class, () -> {
      EncryptionTest.testCipherProvider(conf);
    });
  }

  @Test
  public void testAESCipher() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded");
    }
  }

  @Test
  public void testUnknownCipher() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    assertThrows(IOException.class, () -> {
      EncryptionTest.testEncryption(conf, "foobar", null);
    });
  }

  @Test
  public void testTestEnabledWithDefaultConfig() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded, when "
        + Encryption.CRYPTO_ENABLED_CONF_KEY + " is not set");
    }
  }

  @Test
  public void testTestEnabledWhenCryptoIsExplicitlyEnabled() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, true);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded, when "
        + Encryption.CRYPTO_ENABLED_CONF_KEY + " is set to true");
    }
  }

  @Test
  public void testTestEnabledWhenCryptoIsExplicitlyDisabled() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, false);
    assertThrows(IOException.class, () -> {
      EncryptionTest.testEncryption(conf, algorithm, null);
    });
  }

  // Utility methods for configuration setup
  private Configuration createManagedKeyProviderConfig() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
      MockManagedKeyProvider.class.getName());
    return conf;
  }

  @Test
  public void testManagedKeyProvider() throws Exception {
    Configuration conf = createManagedKeyProviderConfig();
    EncryptionTest.testKeyProvider(conf);
    assertTrue(EncryptionTest.keyProviderResults
      .containsKey(conf.get(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY)),
      "Managed provider should be cached");
  }

  @Test
  public void testBadManagedKeyProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
      FailingManagedKeyProvider.class.getName());
    assertThrows(IOException.class, () -> EncryptionTest.testKeyProvider(conf));
  }

  @Test
  public void testEncryptionWithManagedKeyProvider() throws Exception {
    Configuration conf = createManagedKeyProviderConfig();
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    EncryptionTest.testEncryption(conf, algorithm, null);
    assertTrue(EncryptionTest.keyProviderResults
      .containsKey(conf.get(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY)),
      "Managed provider should be cached");
  }

  @Test
  public void testUnknownCipherWithManagedKeyProvider() throws Exception {
    Configuration conf = createManagedKeyProviderConfig();
    assertThrows(IOException.class, () -> EncryptionTest.testEncryption(conf, "foobar", null));
  }

  @Test
  public void testManagedKeyProviderWhenCryptoIsExplicitlyDisabled() throws Exception {
    Configuration conf = createManagedKeyProviderConfig();
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, false);
    assertThrows(IOException.class, () -> EncryptionTest.testEncryption(conf, algorithm, null));
  }

  @Test
  public void testManagedKeyProviderWithKeyManagementDisabled() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);
    // This should cause issues since we're trying to use managed provider without enabling key
    // management
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, ManagedKeyStoreKeyProvider.class.getName());

    assertThrows(IOException.class, () -> EncryptionTest.testKeyProvider(conf));
  }

  public static class FailingKeyProvider implements KeyProvider {

    @Override
    public void init(String params) {
      throw new RuntimeException("BAD!");
    }

    @Override
    public Key getKey(String alias) {
      return null;
    }

    @Override
    public Key[] getKeys(String[] aliases) {
      return null;
    }

  }

  public static class FailingCipherProvider implements CipherProvider {

    public FailingCipherProvider() {
      super();
      throw new RuntimeException("BAD!");
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String[] getSupportedCiphers() {
      return null;
    }

    @Override
    public Cipher getCipher(String name) {
      return null;
    }

  }

  // Helper class for testing failing managed key provider
  public static class FailingManagedKeyProvider extends MockManagedKeyProvider {
    @Override
    public void initConfig(Configuration conf, String params) {
      throw new RuntimeException("BAD MANAGED PROVIDER!");
    }
  }
}
