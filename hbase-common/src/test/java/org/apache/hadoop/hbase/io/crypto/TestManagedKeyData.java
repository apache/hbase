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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.security.Key;
import java.util.Base64;
import java.util.List;
import javax.crypto.KeyGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.keymeta.KeyIdentityPrefixBytesBacked;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentity;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyData {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyData.class);

  private byte[] keyCust;
  private String keyNamespace;
  private byte[] keyNamespaceBytes;
  private Key theKey;
  private ManagedKeyState keyState;
  private String keyMetadata;
  private ManagedKeyData managedKeyData;

  @Before
  public void setUp() throws Exception {
    resetDigestAlgosCache();
    keyCust = "testCustodian".getBytes();
    keyNamespace = "testNamespace";
    keyNamespaceBytes = Bytes.toBytes(keyNamespace);
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    theKey = keyGen.generateKey();
    keyState = ManagedKeyState.ACTIVE;
    keyMetadata = "testMetadata";
    managedKeyData = new ManagedKeyData(keyCust, keyNamespaceBytes, theKey, keyState, keyMetadata);
  }

  @Test
  public void testConstructor() {
    assertNotNull(managedKeyData);
    assertEquals(keyNamespace, managedKeyData.getKeyNamespace());
    assertArrayEquals(keyCust, managedKeyData.getKeyCustodian());
    assertEquals(theKey, managedKeyData.getTheKey());
    assertEquals(keyState, managedKeyData.getKeyState());
    assertEquals(keyMetadata, managedKeyData.getKeyMetadata());
  }

  @Test
  public void testConstructorNullChecks() {
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(null, keyNamespaceBytes, theKey, keyState, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, null, theKey, keyState, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, keyNamespaceBytes, theKey, null, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, keyNamespaceBytes, theKey, ManagedKeyState.ACTIVE, null));
  }

  @Test
  public void testConstructorWithFailedEncryptionStateAndNullMetadata() {
    ManagedKeyData keyData = new ManagedKeyData(
      new KeyIdentityPrefixBytesBacked(keyCust, keyNamespaceBytes), ManagedKeyState.FAILED);
    assertNotNull(keyData);
    assertEquals(ManagedKeyState.FAILED, keyData.getKeyState());
    assertNull(keyData.getKeyMetadata());
    assertNull(keyData.getPartialIdentity());
    assertNull(keyData.getTheKey());
  }

  @Test
  public void testConstructorWithRefreshTimestamp() {
    long refreshTimestamp = System.currentTimeMillis();
    ManagedKeyIdentity fullIdentity = ManagedKeyIdentityUtils
      .fullKeyIdentityFromMetadata(new Bytes(keyCust), new Bytes(keyNamespaceBytes), keyMetadata);
    ManagedKeyData keyDataWithTimestamp =
      new ManagedKeyData(fullIdentity, theKey, keyState, keyMetadata, refreshTimestamp);
    assertEquals(refreshTimestamp, keyDataWithTimestamp.getRefreshTimestamp());
  }

  @Test
  public void testCloneWithoutKey() {
    ManagedKeyData cloned = managedKeyData.createClientFacingInstance();
    assertNull(cloned.getTheKey());
    assertNull(cloned.getKeyMetadata());
    assertTrue(Bytes.equals(managedKeyData.getKeyCustodian(), cloned.getKeyCustodian()));
    assertEquals(managedKeyData.getKeyNamespace(), cloned.getKeyNamespace());
    assertEquals(managedKeyData.getKeyState(), cloned.getKeyState());
    assertTrue(Bytes.equals(managedKeyData.getPartialIdentity(), cloned.getPartialIdentity()));
  }

  @Test
  public void testGetKeyCustodianEncoded() {
    String encoded = managedKeyData.getKeyCustodianEncoded();
    assertNotNull(encoded);
    assertArrayEquals(keyCust, Base64.getDecoder().decode(encoded));
  }

  @Test
  public void testGetKeyChecksum() {
    long checksum = managedKeyData.getKeyChecksum();
    assertNotEquals(0, checksum);

    // Test with null key
    ManagedKeyData nullKeyData =
      new ManagedKeyData(keyCust, keyNamespaceBytes, null, keyState, keyMetadata);
    assertEquals(0, nullKeyData.getKeyChecksum());
  }

  @Test
  public void testConstructKeyChecksum() {
    byte[] data = "testData".getBytes();
    long checksum = ManagedKeyData.constructKeyChecksum(data);
    assertNotEquals(0, checksum);
  }

  @Test
  public void testGetPartialIdentity() {
    byte[] partialIdentity = managedKeyData.getPartialIdentity();
    assertNotNull(partialIdentity);
    assertEquals(9, partialIdentity.length); // algo selector (1) + XXH3 (8 bytes), default digest
  }

  @Test
  public void testGetPartialIdentityEncoded() {
    String encoded = managedKeyData.getPartialIdentityEncoded();
    assertNotNull(encoded);
    assertEquals(12, encoded.length()); // Base64 of algo selector + XXH3 (9 bytes) = 12 chars
  }

  @Test
  public void testConstructMetadataHash() {
    byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash(keyMetadata);
    assertNotNull(partialIdentity);
    assertEquals(9, partialIdentity.length); // default xxh3: algo selector (1) + 8 bytes
  }

  @Test
  public void testToString() {
    String toString = managedKeyData.toString();
    assertTrue(toString.contains("keyCustodian"));
    assertTrue(toString.contains("keyNamespace"));
    assertTrue(toString.contains("keyState"));
    assertTrue(toString.contains("keyMetadata"));
    assertTrue(toString.contains("refreshTimestamp"));
    assertTrue(toString.contains("keyChecksum"));
  }

  @Test
  public void testEquals() {
    ManagedKeyData same =
      new ManagedKeyData(keyCust, keyNamespaceBytes, theKey, keyState, keyMetadata);
    assertEquals(managedKeyData, same);

    ManagedKeyData different = new ManagedKeyData("differentCust".getBytes(), keyNamespaceBytes,
      theKey, keyState, keyMetadata);
    assertNotEquals(managedKeyData, different);
  }

  @Test
  public void testEqualsWithNull() {
    assertNotEquals(managedKeyData, null);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    assertNotEquals(managedKeyData, new Object());
  }

  @Test
  public void testHashCode() {
    ManagedKeyData same =
      new ManagedKeyData(keyCust, keyNamespaceBytes, theKey, keyState, keyMetadata);
    assertEquals(managedKeyData.hashCode(), same.hashCode());

    ManagedKeyData different = new ManagedKeyData("differentCust".getBytes(), keyNamespaceBytes,
      theKey, keyState, keyMetadata);
    assertNotEquals(managedKeyData.hashCode(), different.hashCode());
  }

  @Test
  public void testConstants() {
    assertEquals("*", ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(ManagedKeyProvider.encodeToStr(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.copyBytes()),
      ManagedKeyData.GLOBAL_CUST_ENCODED);
  }

  /**
   * Resets the cached digest algorithms so initDigestAlgos() can be tested with different configs.
   */
  private static void resetDigestAlgosCache() throws Exception {
    Field field = ManagedKeyIdentityUtils.class.getDeclaredField("DIGEST_ALGOS");
    field.setAccessible(true);
    field.set(null, null);
  }

  @Test
  public void testInitDigestAlgosWithNullConf() throws Exception {
    resetDigestAlgosCache();
    ManagedKeyIdentityUtils.initDigestAlgos(null);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(1, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
  }

  @Test
  public void testInitDigestAlgosWithDefaultConfig() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(1, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
  }

  @Test
  public void testInitDigestAlgosWithSingleAlgoConfig() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY, "md5");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(1, algos.size());
    assertEquals(DigestAlgorithms.MD5, algos.get(0));
  }

  @Test
  public void testInitDigestAlgosWithTwoAlgosSortedByBitPosition() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY, "md5,xxh3");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(2, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
    assertEquals(DigestAlgorithms.MD5, algos.get(1));
  }

  @Test
  public void testInitDigestAlgosWithThreeAlgosUsesFirstTwo() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY,
      "xxh3,xxhash64,md5");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(2, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
    assertEquals(DigestAlgorithms.XXHASH64, algos.get(1));
  }

  @Test
  public void testInitDigestAlgosDedupesAndIgnoresUnknown() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY,
      "xxh3,unknown,xxh3");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(1, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
  }

  @Test
  public void testInitDigestAlgosEmptyConfigFallsBackToXxh3() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY, "");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    List<DigestAlgorithms> algos = ManagedKeyIdentityUtils.getDigestAlgos();
    assertNotNull(algos);
    assertEquals(1, algos.size());
    assertEquals(DigestAlgorithms.XXH3, algos.get(0));
  }

  @Test
  public void testInitDigestAlgosIdempotentAfterFirstCall() throws Exception {
    resetDigestAlgosCache();
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY, "md5");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    assertEquals(DigestAlgorithms.MD5, ManagedKeyIdentityUtils.getDigestAlgos().get(0));
    // Second call with different config should not change cached value
    conf.set(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY, "xxh3");
    ManagedKeyIdentityUtils.initDigestAlgos(conf);
    assertEquals(DigestAlgorithms.MD5, ManagedKeyIdentityUtils.getDigestAlgos().get(0));
  }
}
