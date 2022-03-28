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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Key;
import java.security.KeyException;

import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestEncryptionUtil {

  private static final String INVALID_HASH_ALG = "this-hash-algorithm-not-exists hopefully... :)";
  private static final String DEFAULT_HASH_ALGORITHM = "use-default";

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEncryptionUtil.class);

  // There does not seem to be a ready way to test either getKeyFromBytesOrMasterKey
  // or createEncryptionContext, and the existing code under MobUtils appeared to be
  // untested.  Not ideal!

  @Test
  public void testKeyWrappingUsingHashAlgDefault() throws Exception {
    testKeyWrapping(DEFAULT_HASH_ALGORITHM);
  }

  @Test
  public void testKeyWrappingUsingHashAlgMD5() throws Exception {
    testKeyWrapping("MD5");
  }

  @Test
  public void testKeyWrappingUsingHashAlgSHA256() throws Exception {
    testKeyWrapping("SHA-256");
  }

  @Test
  public void testKeyWrappingUsingHashAlgSHA384() throws Exception {
    testKeyWrapping("SHA-384");
  }

  @Test(expected = RuntimeException.class)
  public void testKeyWrappingWithInvalidHashAlg() throws Exception {
    testKeyWrapping(INVALID_HASH_ALG);
  }

  @Test
  public void testWALKeyWrappingUsingHashAlgDefault() throws Exception {
    testWALKeyWrapping(DEFAULT_HASH_ALGORITHM);
  }

  @Test
  public void testWALKeyWrappingUsingHashAlgMD5() throws Exception {
    testWALKeyWrapping("MD5");
  }

  @Test
  public void testWALKeyWrappingUsingHashAlgSHA256() throws Exception {
    testWALKeyWrapping("SHA-256");
  }

  @Test
  public void testWALKeyWrappingUsingHashAlgSHA384() throws Exception {
    testWALKeyWrapping("SHA-384");
  }

  @Test(expected = RuntimeException.class)
  public void testWALKeyWrappingWithInvalidHashAlg() throws Exception {
    testWALKeyWrapping(INVALID_HASH_ALG);
  }

  @Test(expected = KeyException.class)
  public void testWALKeyWrappingWithIncorrectKey() throws Exception {
    // set up the key provider for testing to resolve a key for our test subject
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());

    // generate a test key
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    Bytes.secureRandom(keyBytes);
    String algorithm = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key key = new SecretKeySpec(keyBytes, algorithm);

    // wrap the test key
    byte[] wrappedKeyBytes = EncryptionUtil.wrapKey(conf, "hbase", key);
    assertNotNull(wrappedKeyBytes);

    // unwrap with an incorrect key
    EncryptionUtil.unwrapWALKey(conf, "other", wrappedKeyBytes);
  }

  @Test(expected = KeyException.class)
  public void testHashAlgorithmMismatchWhenFailExpected() throws Exception {
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    conf.setBoolean(Encryption.CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY, true);
    testKeyWrappingWithMismatchingAlgorithms(conf);
  }

  @Test
  public void testHashAlgorithmMismatchWhenFailNotExpected() throws Exception {
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    conf.setBoolean(Encryption.CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY, false);
    testKeyWrappingWithMismatchingAlgorithms(conf);
  }

  @Test
  public void testHashAlgorithmMismatchShouldNotFailWithDefaultConfig() throws Exception {
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    testKeyWrappingWithMismatchingAlgorithms(conf);
  }

  private void testKeyWrapping(String hashAlgorithm) throws Exception {
    // set up the key provider for testing to resolve a key for our test subject
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    if(!hashAlgorithm.equals(DEFAULT_HASH_ALGORITHM)) {
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, hashAlgorithm);
    }

    // generate a test key
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    Bytes.secureRandom(keyBytes);
    String algorithm =
      conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key key = new SecretKeySpec(keyBytes, algorithm);

    // wrap the test key
    byte[] wrappedKeyBytes = EncryptionUtil.wrapKey(conf, "hbase", key);
    assertNotNull(wrappedKeyBytes);

    // unwrap
    Key unwrappedKey = EncryptionUtil.unwrapKey(conf, "hbase", wrappedKeyBytes);
    assertNotNull(unwrappedKey);
    // only secretkeyspec supported for now
    assertTrue(unwrappedKey instanceof SecretKeySpec);
    // did we get back what we wrapped?
    assertTrue("Unwrapped key bytes do not match original",
               Bytes.equals(keyBytes, unwrappedKey.getEncoded()));

    // unwrap with an incorrect key
    try {
      EncryptionUtil.unwrapKey(conf, "other", wrappedKeyBytes);
      fail("Unwrap with incorrect key did not throw KeyException");
    } catch (KeyException e) {
      // expected
    }
  }

  private void testWALKeyWrapping(String hashAlgorithm) throws Exception {
    // set up the key provider for testing to resolve a key for our test subject
    Configuration conf = new Configuration(); // we don't need HBaseConfiguration for this
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    if(!hashAlgorithm.equals(DEFAULT_HASH_ALGORITHM)) {
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, hashAlgorithm);
    }

    // generate a test key
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    Bytes.secureRandom(keyBytes);
    String algorithm = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key key = new SecretKeySpec(keyBytes, algorithm);

    // wrap the test key
    byte[] wrappedKeyBytes = EncryptionUtil.wrapKey(conf, "hbase", key);
    assertNotNull(wrappedKeyBytes);

    // unwrap
    Key unwrappedKey = EncryptionUtil.unwrapWALKey(conf, "hbase", wrappedKeyBytes);
    assertNotNull(unwrappedKey);
    // only secretkeyspec supported for now
    assertTrue(unwrappedKey instanceof SecretKeySpec);
    // did we get back what we wrapped?
    assertTrue("Unwrapped key bytes do not match original",
               Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
  }

  private void testKeyWrappingWithMismatchingAlgorithms(Configuration conf) throws Exception {
    // we use MD5 to hash the encryption key during wrapping
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "MD5");

    // generate a test key
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    Bytes.secureRandom(keyBytes);
    String algorithm =
      conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key key = new SecretKeySpec(keyBytes, algorithm);

    // wrap the test key
    byte[] wrappedKeyBytes = EncryptionUtil.wrapKey(conf, "hbase", key);
    assertNotNull(wrappedKeyBytes);

    // we set the default hash algorithm to SHA-384 during unwrapping
    conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "SHA-384");

    // unwrap
    // we expect to fail, if CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY == true
    // otherwise we will use the algorithm written during wrapping
    Key unwrappedKey = EncryptionUtil.unwrapKey(conf, "hbase", wrappedKeyBytes);
    assertNotNull(unwrappedKey);

    // did we get back what we wrapped?
    assertTrue("Unwrapped key bytes do not match original",
      Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
  }

}
