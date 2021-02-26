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
package org.apache.hadoop.hbase.io.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.Key;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestEncryption {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEncryption.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestEncryption.class);

  @Test
  public void testSmallBlocks() throws Exception {
    byte[] key = new byte[16];
    Bytes.random(key);
    byte[] iv = new byte[16];
    Bytes.random(iv);
    for (int size: new int[] { 4, 8, 16, 32, 64, 128, 256, 512 }) {
      checkTransformSymmetry(key, iv, getRandomBlock(size));
    }
  }

  @Test
  public void testLargeBlocks() throws Exception {
    byte[] key = new byte[16];
    Bytes.random(key);
    byte[] iv = new byte[16];
    Bytes.random(iv);
    for (int size: new int[] { 256 * 1024, 512 * 1024, 1024 * 1024 }) {
      checkTransformSymmetry(key, iv, getRandomBlock(size));
    }
  }

  @Test
  public void testOddSizedBlocks() throws Exception {
    byte[] key = new byte[16];
    Bytes.random(key);
    byte[] iv = new byte[16];
    Bytes.random(iv);
    for (int size: new int[] { 3, 7, 11, 23, 47, 79, 119, 175 }) {
      checkTransformSymmetry(key, iv, getRandomBlock(size));
    }
  }

  @Test
  public void testTypicalHFileBlocks() throws Exception {
    byte[] key = new byte[16];
    Bytes.random(key);
    byte[] iv = new byte[16];
    Bytes.random(iv);
    for (int size: new int[] { 4 * 1024, 8 * 1024, 64 * 1024, 128 * 1024 }) {
      checkTransformSymmetry(key, iv, getRandomBlock(size));
    }
  }

  private void checkTransformSymmetry(byte[] keyBytes, byte[] iv, byte[] plaintext)
      throws Exception {
    LOG.info("checkTransformSymmetry: AES, plaintext length = " + plaintext.length);

    Configuration conf = HBaseConfiguration.create();
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher aes = Encryption.getCipher(conf, algorithm);
    Key key = new SecretKeySpec(keyBytes, algorithm);

    Encryptor e = aes.getEncryptor();
    e.setKey(key);
    e.setIv(iv);
    e.reset();
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    Encryption.encrypt(encOut, plaintext, 0, plaintext.length, e);
    byte[] encrypted = encOut.toByteArray();

    Decryptor d = aes.getDecryptor();
    d.setKey(key);
    d.setIv(iv);
    d.reset();
    ByteArrayInputStream encIn = new ByteArrayInputStream(encrypted);
    ByteArrayOutputStream decOut = new ByteArrayOutputStream();
    Encryption.decrypt(decOut, encIn, plaintext.length, d);

    byte[] result = decOut.toByteArray();
    assertEquals("Decrypted result has different length than plaintext",
      result.length, plaintext.length);
    assertTrue("Transformation was not symmetric",
      Bytes.equals(result, plaintext));
  }

  private byte[] getRandomBlock(int size) {
    byte[] b = new byte[size];
    Bytes.random(b);
    return b;
  }
}
