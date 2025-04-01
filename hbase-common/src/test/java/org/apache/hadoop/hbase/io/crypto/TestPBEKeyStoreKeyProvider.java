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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import javax.crypto.spec.SecretKeySpec;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import static org.apache.hadoop.hbase.io.crypto.PBEKeyStoreKeyProvider.KEY_METADATA_ALIAS;
import static org.apache.hadoop.hbase.io.crypto.PBEKeyStoreKeyProvider.KEY_METADATA_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category({ MiscTests.class, SmallTests.class })
@RunWith(Parameterized.class)
public class TestPBEKeyStoreKeyProvider extends TestKeyStoreKeyProvider {

  private static final String MASTER_KEY_ALIAS = "master-alias";

  private Configuration conf = HBaseConfiguration.create();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPBEKeyStoreKeyProvider.class);
  private int nPrefixes = 2;

  private PBEKeyProvider pbeProvider;

  private Map<Bytes, Bytes> prefix2key = new HashMap<>();
  private Map<Bytes, String> prefix2alias = new HashMap<>();
  private String clusterId;
  private byte[] masterKey;

  @Before
  public void setUp() throws Exception {
    super.setUp();;
    pbeProvider = (PBEKeyProvider) provider;
    pbeProvider.initConfig(conf);
  }

  protected KeyProvider createProvider() {
    return new PBEKeyStoreKeyProvider();
  }

  protected void addCustomEntries(KeyStore store, Properties passwdProps) throws Exception {
    super.addCustomEntries(store, passwdProps);
    for (int i = 0; i < nPrefixes; ++i) {
      String prefix = "prefix+ " + i;
      String alias = prefix + "-alias";
      byte[] key = MessageDigest.getInstance("SHA-256").digest(Bytes.toBytes(alias));
      prefix2alias.put(new Bytes(prefix.getBytes()), alias);
      prefix2key.put(new Bytes(prefix.getBytes()), new Bytes(key));
      store.setEntry(alias, new KeyStore.SecretKeyEntry(new SecretKeySpec(key, "AES")),
        new KeyStore.PasswordProtection(withPasswordOnAlias ? PASSWORD.toCharArray() : new char[0]));

      String encPrefix = Base64.getEncoder().encodeToString(prefix.getBytes());
      String confKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encPrefix + "." + "alias";
      conf.set(confKey, alias);

      passwdProps.setProperty(alias, PASSWORD);

      clusterId = UUID.randomUUID().toString();
      masterKey = MessageDigest.getInstance("SHA-256").digest(
        Bytes.toBytes(MASTER_KEY_ALIAS));
      store.setEntry(MASTER_KEY_ALIAS, new KeyStore.SecretKeyEntry(
        new SecretKeySpec(masterKey, "AES")),
        new KeyStore.PasswordProtection(withPasswordOnAlias ? PASSWORD.toCharArray() :
          new char[0]));

      conf.set(HConstants.CRYPTO_PBE_MASTERKEY_NAME_CONF_KEY, MASTER_KEY_ALIAS);

      passwdProps.setProperty(MASTER_KEY_ALIAS, PASSWORD);
    }
  }

  private void addEntry(String alias, String prefix) {
    String encPrefix = Base64.getEncoder().encodeToString(prefix.getBytes());
    String confKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encPrefix + "." + "alias";
    conf.set(confKey, alias);
  }

  @Test
  public void testGetPBEKey() throws Exception {
    for (Bytes prefix : prefix2key.keySet()) {
      PBEKeyData keyData = pbeProvider.getPBEKey(prefix.get(), PBEKeyData.KEY_NAMESPACE_GLOBAL);
      assertPBEKeyData(keyData, PBEKeyStatus.ACTIVE, prefix2key.get(prefix).get(), prefix.get(),
        prefix2alias.get(prefix));
    }
  }

  @Test
  public void testGetInactiveKey() throws Exception {
    Bytes firstPrefix = prefix2key.keySet().iterator().next();
    String encPrefix = Base64.getEncoder().encodeToString(firstPrefix.get());
    conf.set(HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encPrefix + ".active", "false");
    PBEKeyData keyData = pbeProvider.getPBEKey(firstPrefix.get(), PBEKeyData.KEY_NAMESPACE_GLOBAL);
    assertNotNull(keyData);
    assertPBEKeyData(keyData, PBEKeyStatus.INACTIVE, prefix2key.get(firstPrefix).get(),
      firstPrefix.get(), prefix2alias.get(firstPrefix));
  }

  @Test
  public void testGetInvalidKey() throws Exception {
    byte[] invalidPrefixBytes = "invalid".getBytes();
    PBEKeyData keyData = pbeProvider.getPBEKey(invalidPrefixBytes,
      PBEKeyData.KEY_NAMESPACE_GLOBAL);
    assertNotNull(keyData);
    assertPBEKeyData(keyData, PBEKeyStatus.FAILED, null, invalidPrefixBytes, null);
  }

  @Test
  public void testGetDisabledKey() throws Exception {
    byte[] invalidPrefix = new byte[] { 1, 2, 3 };
    String invalidPrefixEnc = PBEKeyStoreKeyProvider.encodeToPrefixStr(invalidPrefix);
    conf.set(HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + invalidPrefixEnc + ".active", "false");
    PBEKeyData keyData = pbeProvider.getPBEKey(invalidPrefix, PBEKeyData.KEY_NAMESPACE_GLOBAL);
    assertNotNull(keyData);
    assertPBEKeyData(keyData, PBEKeyStatus.DISABLED, null,
      invalidPrefix, null);
  }

  @Test
  public void testGetClusterKey() throws Exception {
    PBEKeyData clusterKeyData = pbeProvider.getClusterKey(clusterId.getBytes());
    assertPBEKeyData(clusterKeyData, PBEKeyStatus.ACTIVE, masterKey, clusterId.getBytes(),
      MASTER_KEY_ALIAS);
  }

  @Test
  public void testUnwrapInvalidKey() throws Exception {
    String invalidAlias = "invalidAlias";
    byte[] invalidPrefix = new byte[] { 1, 2, 3 };
    String invalidPrefixEnc = PBEKeyStoreKeyProvider.encodeToPrefixStr(invalidPrefix);
    String invalidMetadata = PBEKeyStoreKeyProvider.generateKeyMetadata(invalidAlias,
      invalidPrefixEnc);
    PBEKeyData keyData = pbeProvider.unwrapKey(invalidMetadata);
    assertNotNull(keyData);
    assertPBEKeyData(keyData, PBEKeyStatus.FAILED, null, invalidPrefix,
      invalidAlias);
  }

  @Test
  public void testUnwrapDisabledKey() throws Exception {
    String invalidAlias = "invalidAlias";
    byte[] invalidPrefix = new byte[] { 1, 2, 3 };
    String invalidPrefixEnc = PBEKeyStoreKeyProvider.encodeToPrefixStr(invalidPrefix);
    conf.set(HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + invalidPrefixEnc + ".active", "false");
    String invalidMetadata = PBEKeyStoreKeyProvider.generateKeyMetadata(invalidAlias,
      invalidPrefixEnc);
    PBEKeyData keyData = pbeProvider.unwrapKey(invalidMetadata);
    assertNotNull(keyData);
    assertPBEKeyData(keyData, PBEKeyStatus.DISABLED, null, invalidPrefix, invalidAlias);
  }

  private void assertPBEKeyData(PBEKeyData keyData, PBEKeyStatus expKeyStatus, byte[] key,
      byte[] prefixBytes, String alias) throws Exception {
    assertNotNull(keyData);
    assertEquals(expKeyStatus, keyData.getKeyStatus());
    if (key == null) {
      assertNull(keyData.getTheKey());
    }
    else {
      byte[] keyBytes = keyData.getTheKey().getEncoded();
      assertEquals(key.length, keyBytes.length);
      assertEquals(new Bytes(key), keyBytes);
    }
    Map keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyData.getKeyMetadata(),
      HashMap.class);
    assertNotNull(keyMetadata);
    assertEquals(new Bytes(prefixBytes), keyData.getPBEPrefix());
    assertEquals(alias, keyMetadata.get(KEY_METADATA_ALIAS));
    assertEquals(Base64.getEncoder().encodeToString(prefixBytes),
      keyMetadata.get(KEY_METADATA_PREFIX));
    assertEquals(keyData, pbeProvider.unwrapKey(keyData.getKeyMetadata()));
  }
}
