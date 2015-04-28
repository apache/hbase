/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.io.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URLEncoder;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Properties;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestKeyStoreKeyProvider {

  private static final Log LOG = LogFactory.getLog(TestKeyStoreKeyProvider.class);
  static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();
  static final String ALIAS = "test";
  static final String PASSWORD = "password";

  static byte[] KEY;
  static File storeFile;
  static File passwordFile;

  @BeforeClass
  public static void setUp() throws Exception {
    KEY = MessageDigest.getInstance("SHA-256").digest(ALIAS.getBytes());
    // Create a JKECS store containing a test secret key
    KeyStore store = KeyStore.getInstance("JCEKS");
    store.load(null, PASSWORD.toCharArray());
    store.setEntry(ALIAS,
      new KeyStore.SecretKeyEntry(new SecretKeySpec(KEY, "AES")),
      new KeyStore.PasswordProtection(PASSWORD.toCharArray()));
    // Create the test directory
    String dataDir = TEST_UTIL.getDataTestDir().toString();
    new File(dataDir).mkdirs();
    // Write the keystore file
    storeFile = new File(dataDir, "keystore.jks");
    FileOutputStream os = new FileOutputStream(storeFile);
    try {
      store.store(os, PASSWORD.toCharArray());
    } finally {
      os.close();
    }
    // Write the password file
    Properties p = new Properties();
    p.setProperty(ALIAS, PASSWORD);
    passwordFile = new File(dataDir, "keystore.pw");
    os = new FileOutputStream(passwordFile);
    try {
      p.store(os, "");
    } finally {
      os.close();
    }
  }

  @Test(timeout=30000)
  public void testKeyStoreKeyProviderWithPassword() throws Exception {
    KeyProvider provider = new KeyStoreKeyProvider();
    provider.init("jceks://" + storeFile.toURI().getPath() + "?password=" + PASSWORD);
    Key key = provider.getKey(ALIAS);
    assertNotNull(key);
    byte[] keyBytes = key.getEncoded();
    assertEquals(keyBytes.length, KEY.length);
    for (int i = 0; i < KEY.length; i++) {
      assertEquals(keyBytes[i], KEY[i]);
    }
  }

  @Test(timeout=30000)
  public void testKeyStoreKeyProviderWithPasswordFile() throws Exception {
    KeyProvider provider = new KeyStoreKeyProvider();
    provider.init("jceks://" + storeFile.toURI().getPath() + "?passwordFile=" +
      URLEncoder.encode(passwordFile.getAbsolutePath(), "UTF-8"));
    Key key = provider.getKey(ALIAS);
    assertNotNull(key);
    byte[] keyBytes = key.getEncoded();
    assertEquals(keyBytes.length, KEY.length);
    for (int i = 0; i < KEY.length; i++) {
      assertEquals(keyBytes[i], KEY[i]);
    }
  }
}
