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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Key;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestAES256GCM {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAES256GCM.class);

  @Test
  public void testCipherProperties() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull("AES_256_GCM cipher should be available", cipher);
    assertEquals("AES_256_GCM", cipher.getName());
    assertEquals(32, cipher.getKeyLength());
    assertEquals(12, cipher.getIvLength());
    assertEquals("AES", cipher.getKeyAlgorithm());
  }

  @Test
  public void testEncryptDecryptRoundTrip() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Key key = cipher.getRandomKey();
    assertNotNull(key);
    assertEquals(32, key.getEncoded().length);
    assertEquals("AES", key.getAlgorithm());

    byte[] plaintext = Bytes.toBytes("Hello, AES-256-GCM authenticated encryption!");
    byte[] iv = new byte[12];
    Bytes.secureRandom(iv);

    // Encrypt
    Encryptor encryptor = cipher.getEncryptor();
    encryptor.setKey(key);
    encryptor.setIv(iv);
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = encryptor.createEncryptionStream(encOut);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    // Ciphertext should be larger than plaintext due to the 16-byte auth tag
    assertTrue("Ciphertext should be larger than plaintext", ciphertext.length >= plaintext.length);

    // Decrypt
    Decryptor decryptor = cipher.getDecryptor();
    decryptor.setKey(key);
    decryptor.setIv(iv);
    InputStream din = decryptor.createDecryptionStream(new ByteArrayInputStream(ciphertext));
    byte[] decrypted = IOUtils.toByteArray(din);
    din.close();

    assertArrayEquals("Decrypted data should match original plaintext", plaintext, decrypted);
  }

  @Test
  public void testEncryptDecryptLargeData() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Key key = cipher.getRandomKey();
    byte[] plaintext = new byte[64 * 1024];
    Bytes.secureRandom(plaintext);
    byte[] iv = new byte[12];
    Bytes.secureRandom(iv);

    // Encrypt
    Encryptor encryptor = cipher.getEncryptor();
    encryptor.setKey(key);
    encryptor.setIv(iv);
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = encryptor.createEncryptionStream(encOut);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    // Decrypt
    Decryptor decryptor = cipher.getDecryptor();
    decryptor.setKey(key);
    decryptor.setIv(iv);
    InputStream din = decryptor.createDecryptionStream(new ByteArrayInputStream(ciphertext));
    byte[] decrypted = IOUtils.toByteArray(din);
    din.close();

    assertArrayEquals("Large data round-trip failed", plaintext, decrypted);
  }

  @Test
  public void testTamperedCiphertextDetection() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Key key = cipher.getRandomKey();
    byte[] plaintext = Bytes.toBytes("Tamper detection test data");
    byte[] iv = new byte[12];
    Bytes.secureRandom(iv);

    // Encrypt
    Encryptor encryptor = cipher.getEncryptor();
    encryptor.setKey(key);
    encryptor.setIv(iv);
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = encryptor.createEncryptionStream(encOut);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    // Tamper with the ciphertext
    ciphertext[ciphertext.length / 2] ^= 0xFF;

    // Attempt to decrypt tampered data should fail
    Decryptor decryptor = cipher.getDecryptor();
    decryptor.setKey(key);
    decryptor.setIv(iv);
    try {
      InputStream din = decryptor.createDecryptionStream(new ByteArrayInputStream(ciphertext));
      IOUtils.toByteArray(din);
      din.close();
      fail("Decrypting tampered ciphertext should have thrown an exception");
    } catch (Exception e) {
      // GCM auth tag verification failure is expected
    }
  }

  @Test
  public void testIvIncrementIsOne() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Encryptor encryptor = cipher.getEncryptor();
    assertEquals(1, encryptor.getIvIncrement(0));
    assertEquals(1, encryptor.getIvIncrement(1024));
    assertEquals(1, encryptor.getIvIncrement(65536));
  }

  @Test
  public void testEncryptDecryptWithContextAPI() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Key key = cipher.getRandomKey();
    byte[] iv = new byte[12];
    Bytes.secureRandom(iv);

    byte[] plaintext = Bytes.toBytes("Context API test");

    // Encrypt using Context-based API
    Encryption.Context ctx = Encryption.newContext(conf);
    ctx.setCipher(cipher);
    ctx.setKey(key);

    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = cipher.createEncryptionStream(encOut, ctx, iv);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    // Decrypt using Context-based API
    InputStream din = cipher.createDecryptionStream(new ByteArrayInputStream(ciphertext), ctx, iv);
    byte[] decrypted = IOUtils.toByteArray(din);
    din.close();

    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  public void testWrongKeyDecryptionFails() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Cipher cipher = Encryption.getCipher(conf, "AES_256_GCM");
    assertNotNull(cipher);

    Key key1 = cipher.getRandomKey();
    Key key2 = cipher.getRandomKey();
    byte[] iv = new byte[12];
    Bytes.secureRandom(iv);

    byte[] plaintext = Bytes.toBytes("Wrong key test");

    // Encrypt with key1
    Encryptor encryptor = cipher.getEncryptor();
    encryptor.setKey(key1);
    encryptor.setIv(iv);
    ByteArrayOutputStream encOut = new ByteArrayOutputStream();
    OutputStream cout = encryptor.createEncryptionStream(encOut);
    cout.write(plaintext);
    cout.close();
    byte[] ciphertext = encOut.toByteArray();

    // Decrypt with key2 should fail
    Decryptor decryptor = cipher.getDecryptor();
    decryptor.setKey(key2);
    decryptor.setIv(iv);
    try {
      InputStream din = decryptor.createDecryptionStream(new ByteArrayInputStream(ciphertext));
      IOUtils.toByteArray(din);
      din.close();
      fail("Decrypting with wrong key should have thrown an exception");
    } catch (Exception e) {
      // Expected - GCM authentication failure
    }
  }
}
