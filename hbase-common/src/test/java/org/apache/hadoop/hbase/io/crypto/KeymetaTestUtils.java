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
package org.apache.hadoop.hbase.io.crypto;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

public class KeymetaTestUtils {

  private KeymetaTestUtils() {
    // Utility class
  }

  public static final String ALIAS = "test";
  public static final String PASSWORD = "password";

  public static void addEntry(Configuration conf, int keyLen, KeyStore store, String alias,
    String custodian, boolean withPasswordOnAlias, Map<Bytes, Bytes> cust2key,
    Map<Bytes, String> cust2alias, Properties passwordFileProps) throws Exception {
    Preconditions.checkArgument(keyLen == 256 || keyLen == 128, "Key length must be 256 or 128");
    byte[] key =
      MessageDigest.getInstance(keyLen == 256 ? "SHA-256" : "MD5").digest(Bytes.toBytes(alias));
    cust2alias.put(new Bytes(custodian.getBytes()), alias);
    cust2key.put(new Bytes(custodian.getBytes()), new Bytes(key));
    store.setEntry(alias, new KeyStore.SecretKeyEntry(new SecretKeySpec(key, "AES")),
      new KeyStore.PasswordProtection(withPasswordOnAlias ? PASSWORD.toCharArray() : new char[0]));
    String encCust = Base64.getEncoder().encodeToString(custodian.getBytes());
    String confKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encCust + "." + "alias";
    conf.set(confKey, alias);
    if (passwordFileProps != null) {
      passwordFileProps.setProperty(alias, PASSWORD);
    }
  }

  public static String setupTestKeyStore(HBaseCommonTestingUtil testUtil,
    boolean withPasswordOnAlias, boolean withPasswordFile,
    Function<KeyStore, Properties> customEntriesAdder) throws Exception {
    KeyStore store = KeyStore.getInstance("JCEKS");
    store.load(null, PASSWORD.toCharArray());
    Properties passwordProps = null;
    if (customEntriesAdder != null) {
      passwordProps = customEntriesAdder.apply(store);
    }
    // Create the test directory
    String dataDir = testUtil.getDataTestDir().toString();
    new File(dataDir).mkdirs();
    // Write the keystore file
    File storeFile = new File(dataDir, "keystore.jks");
    FileOutputStream os = new FileOutputStream(storeFile);
    try {
      store.store(os, PASSWORD.toCharArray());
    } finally {
      os.close();
    }
    File passwordFile = null;
    if (withPasswordFile) {
      passwordFile = new File(dataDir, "keystore.pw");
      os = new FileOutputStream(passwordFile);
      try {
        passwordProps.store(os, "");
      } finally {
        os.close();
      }
    }
    String providerParams;
    if (withPasswordFile) {
      providerParams = "jceks://" + storeFile.toURI().getPath() + "?passwordFile="
        + URLEncoder.encode(passwordFile.getAbsolutePath(), "UTF-8");
    } else {
      providerParams = "jceks://" + storeFile.toURI().getPath() + "?password=" + PASSWORD;
    }
    return providerParams;
  }

  public static FileStatus createMockFile(String fileName) {
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn(fileName);
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getPath()).thenReturn(mockPath);
    return mockFileStatus;
  }

  public static Path createMockPath(String tableName, String family) {
    Path mockPath = mock(Path.class);
    Path mockRegionDir = mock(Path.class);
    Path mockTableDir = mock(Path.class);
    Path mockNamespaceDir = mock(Path.class);
    Path mockFamilyDir = mock(Path.class);
    Path mockDataDir = mock(Path.class);
    when(mockPath.getParent()).thenReturn(mockFamilyDir);
    when(mockFamilyDir.getParent()).thenReturn(mockRegionDir);
    when(mockRegionDir.getParent()).thenReturn(mockTableDir);
    when(mockTableDir.getParent()).thenReturn(mockNamespaceDir);
    when(mockNamespaceDir.getParent()).thenReturn(mockDataDir);
    when(mockTableDir.getName()).thenReturn(tableName);
    when(mockFamilyDir.getName()).thenReturn(family);
    return mockPath;
  }
}
