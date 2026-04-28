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
package org.apache.hadoop.hbase.io.crypto.aes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.SecureRandomSpi;
import java.security.Security;
import java.util.Arrays;
import java.util.Collection;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({ MiscTests.class, SmallTests.class })
@RunWith(Parameterized.class)
public class TestAES {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAES.class);

  private final String cipherName;
  private final int expectedKeyLength;
  private final int expectedIvLength;

  public TestAES(String cipherName, int expectedKeyLength, int expectedIvLength) {
    this.cipherName = cipherName;
    this.expectedKeyLength = expectedKeyLength;
    this.expectedIvLength = expectedIvLength;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> ciphers() {
    return Arrays.asList(new Object[][] { { "AES", Cipher.KEY_LENGTH, Cipher.IV_LENGTH },
      { "AES_256_GCM", AES256GCM.KEY_LENGTH, AES256GCM.NONCE_LENGTH }, });
  }

  // Validation for AES in CTR mode with a 128 bit key
  // From NIST Special Publication 800-38A
  @Test
  public void testAESAlgorithm() throws Exception {
    Assume.assumeTrue("NIST SP 800-38A vectors apply only to AES-CTR", "AES".equals(cipherName));

    Configuration conf = HBaseConfiguration.create();
    Cipher aes = Encryption.getCipher(conf, cipherName);
    assertEquals(expectedKeyLength, aes.getKeyLength());
    assertEquals(expectedIvLength, aes.getIvLength());
    Encryptor e = aes.getEncryptor();
    e.setKey(new SecretKeySpec(Bytes.fromHex("2b7e151628aed2a6abf7158809cf4f3c"), "AES"));
    e.setIv(Bytes.fromHex("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStream cout = e.createEncryptionStream(out);
    cout.write(Bytes.fromHex("6bc1bee22e409f96e93d7e117393172a"));
    cout.write(Bytes.fromHex("ae2d8a571e03ac9c9eb76fac45af8e51"));
    cout.write(Bytes.fromHex("30c81c46a35ce411e5fbc1191a0a52ef"));
    cout.write(Bytes.fromHex("f69f2445df4f9b17ad2b417be66c3710"));
    cout.close();

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    byte[] b = new byte[16];
    IOUtils.readFully(in, b);
    assertTrue("Failed #1", Bytes.equals(b, Bytes.fromHex("874d6191b620e3261bef6864990db6ce")));
    IOUtils.readFully(in, b);
    assertTrue("Failed #2", Bytes.equals(b, Bytes.fromHex("9806f66b7970fdff8617187bb9fffdff")));
    IOUtils.readFully(in, b);
    assertTrue("Failed #3", Bytes.equals(b, Bytes.fromHex("5ae4df3edbd5d35e5b4f09020db03eab")));
    IOUtils.readFully(in, b);
    assertTrue("Failed #4", Bytes.equals(b, Bytes.fromHex("1e031dda2fbe03d1792170a0f3009cee")));
  }

  @Test
  public void testEncryptDecryptRoundTrip() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, cipherName);
    assertEquals(expectedKeyLength, cipher.getKeyLength());
    assertEquals(expectedIvLength, cipher.getIvLength());

    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[expectedKeyLength];
    rng.nextBytes(keyBytes);
    SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
    byte[] iv = new byte[expectedIvLength];
    rng.nextBytes(iv);

    byte[] plaintext = Bytes.toBytes("Hello, HBase encryption round-trip test!");

    Encryptor e = cipher.getEncryptor();
    e.setKey(key);
    e.setIv(iv);
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = e.createEncryptionStream(encOut);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    Decryptor d = cipher.getDecryptor();
    d.setKey(key);
    d.setIv(iv);
    ByteArrayInputStream encIn = new ByteArrayInputStream(ciphertext);
    InputStream din = d.createDecryptionStream(encIn);
    byte[] decrypted = IOUtils.toByteArray(din);
    din.close();

    assertArrayEquals("Round-trip encrypt/decrypt failed for " + cipherName, plaintext, decrypted);
  }

  @Test
  public void testAlternateRNG() throws Exception {
    Security.addProvider(new TestProvider());

    Configuration conf = new Configuration();
    conf.set(Cipher.RNG_ALGORITHM_KEY, "TestRNG");
    conf.set(Cipher.RNG_PROVIDER_KEY, "TEST");
    DefaultCipherProvider.getInstance().setConf(conf);

    AESCipher cipher = (AESCipher) Encryption.getCipher(conf, cipherName);
    assertEquals(cipherName + " did not find alternate RNG", "TestRNG",
      cipher.getRNG().getAlgorithm());
  }

  static class TestProvider extends Provider {
    private static final long serialVersionUID = 1L;

    public TestProvider() {
      super("TEST", 1.0, "Test provider");
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          put("SecureRandom.TestRNG", TestAES.class.getName() + "$TestRNG");
          return null;
        }
      });
    }
  }

  // Must be public for instantiation by the SecureRandom SPI
  public static class TestRNG extends SecureRandomSpi {
    private static final long serialVersionUID = 1L;
    private SecureRandom rng;

    public TestRNG() throws Exception {
      rng = java.security.SecureRandom.getInstance("SHA1PRNG");
    }

    @Override
    protected void engineSetSeed(byte[] seed) {
      rng.setSeed(seed);
    }

    @Override
    protected void engineNextBytes(byte[] bytes) {
      rng.nextBytes(bytes);
    }

    @Override
    protected byte[] engineGenerateSeed(int numBytes) {
      return rng.generateSeed(numBytes);
    }
  }

}
