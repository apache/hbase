/*
 *
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

import java.security.Key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestEncryptionTest {

  @Test
  public void testTestKeyProvider() {
    Configuration conf = HBaseConfiguration.create();
    try {
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
      EncryptionTest.testKeyProvider(conf);
    } catch (Exception e) {
      fail("Instantiation of test key provider should have passed");
    }
    try {
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, FailingKeyProvider.class.getName());
      EncryptionTest.testKeyProvider(conf);
      fail("Instantiation of bad test key provider should have failed check");
    } catch (Exception e) { }
  }

  @Test
  public void testTestCipherProvider() {
    Configuration conf = HBaseConfiguration.create();
    try {
      conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, DefaultCipherProvider.class.getName());
      EncryptionTest.testCipherProvider(conf);
    } catch (Exception e) {
      fail("Instantiation of test cipher provider should have passed");
    }
    try {
      conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, FailingCipherProvider.class.getName());
      EncryptionTest.testCipherProvider(conf);
      fail("Instantiation of bad test cipher provider should have failed check");
    } catch (Exception e) { }
  }

  @Test
  public void testTestCipher() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    try {
      EncryptionTest.testEncryption(conf, "AES", null);
    } catch (Exception e) {
      fail("Test for cipher AES should have succeeded");
    }
    try {
      EncryptionTest.testEncryption(conf, "foobar", null);
      fail("Test for bogus cipher should have failed");
    } catch (Exception e) { }
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
