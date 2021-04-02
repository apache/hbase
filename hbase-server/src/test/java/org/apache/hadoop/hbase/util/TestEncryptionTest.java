/**
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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestEncryptionTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEncryptionTest.class);

  @Test
  public void testTestKeyProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    EncryptionTest.testKeyProvider(conf);
  }

  @Test(expected = IOException.class)
  public void testBadKeyProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, FailingKeyProvider.class.getName());
    EncryptionTest.testKeyProvider(conf);
    fail("Instantiation of bad test key provider should have failed check");
  }

  @Test
  public void testDefaultCipherProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, DefaultCipherProvider.class.getName());
    EncryptionTest.testCipherProvider(conf);
  }

  @Test(expected = IOException.class)
  public void testBadCipherProvider() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, FailingCipherProvider.class.getName());
    EncryptionTest.testCipherProvider(conf);
    fail("Instantiation of bad test cipher provider should have failed check");
  }

  @Test
  public void testAESCipher() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded");
    }
  }

  @Test(expected = IOException.class)
  public void testUnknownCipher() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    EncryptionTest.testEncryption(conf, "foobar", null);
    fail("Test for bogus cipher should have failed");
  }

  @Test
  public void testTestEnabledWithDefaultConfig() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded, when " +
        Encryption.CRYPTO_ENABLED_CONF_KEY + " is not set");
    }
  }

  @Test
  public void testTestEnabledWhenCryptoIsExplicitlyEnabled() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, true);
    try {
      EncryptionTest.testEncryption(conf, algorithm, null);
    } catch (Exception e) {
      fail("Test for cipher " + algorithm + " should have succeeded, when " +
        Encryption.CRYPTO_ENABLED_CONF_KEY + " is set to true");
    }
  }

  @Test(expected = IOException.class)
  public void testTestEnabledWhenCryptoIsExplicitlyDisabled() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, false);
    EncryptionTest.testEncryption(conf, algorithm, null);
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
}
