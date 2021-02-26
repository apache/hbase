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
package org.apache.hadoop.hbase.io.crypto.aes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.SecureRandomSpi;
import java.security.Security;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestCommonsAES {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCommonsAES.class);

  // Validation for AES in CTR mode with a 128 bit key
  // From NIST Special Publication 800-38A
  @Test
  public void testAESAlgorithm() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher aes = Encryption.getCipher(conf, "AES");
    assertEquals(CommonsCryptoAES.KEY_LENGTH, aes.getKeyLength());
    assertEquals(CommonsCryptoAES.IV_LENGTH, aes.getIvLength());
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
  public void testAlternateRNG() throws Exception {
    Security.addProvider(new TestProvider());

    Configuration conf = new Configuration();
    conf.set(AES.RNG_ALGORITHM_KEY, "TestRNG");
    conf.set(AES.RNG_PROVIDER_KEY, "TEST");
    DefaultCipherProvider.getInstance().setConf(conf);

    AES aes = new AES(DefaultCipherProvider.getInstance());
    assertEquals("AES did not find alternate RNG", "TestRNG", aes.getRNG().getAlgorithm());
  }

  static class TestProvider extends Provider {
    private static final long serialVersionUID = 1L;
    public TestProvider() {
      super("TEST", 1.0, "Test provider");
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          put("SecureRandom.TestRNG", TestCommonsAES.class.getName() + "$TestRNG");
          return null;
        }
      });
    }
  }

  // Must be public for instantiation by the SecureRandom SPI
  public static class TestRNG extends SecureRandomSpi {
    private static final long serialVersionUID = 1L;
    private SecureRandom rng;

    public TestRNG() {
      try {
        rng = SecureRandom.getInstance("SHA1PRNG");
      } catch (NoSuchAlgorithmException e) {
        fail("Unable to create SecureRandom instance");
      }
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
