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
package org.apache.hadoop.hbase.keymeta;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for SystemKeyCache class. NOTE: The createCache() method is tested in
 * TestKeyManagementService.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestSystemKeyCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSystemKeyCache.class);

  @Mock
  private SystemKeyAccessor mockAccessor;

  private static final Bytes TEST_CUSTODIAN = new Bytes("test-custodian".getBytes());
  private static final String TEST_NAMESPACE = "test-namespace";
  private static final Bytes TEST_NAMESPACE_BYTES = new Bytes(TEST_NAMESPACE.getBytes());
  private static final String TEST_METADATA_1 = "metadata-1";
  private static final String TEST_METADATA_2 = "metadata-2";
  private static final String TEST_METADATA_3 = "metadata-3";

  private Key testKey1;
  private Key testKey2;
  private Key testKey3;
  private ManagedKeyData keyData1;
  private ManagedKeyData keyData2;
  private ManagedKeyData keyData3;
  private Path keyPath1;
  private Path keyPath2;
  private Path keyPath3;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Create test keys
    testKey1 = new SecretKeySpec("test-key-1-bytes".getBytes(), "AES");
    testKey2 = new SecretKeySpec("test-key-2-bytes".getBytes(), "AES");
    testKey3 = new SecretKeySpec("test-key-3-bytes".getBytes(), "AES");

    // Create test key data with different checksums
    keyData1 = new ManagedKeyData(ManagedKeyIdentityUtils
      .fullKeyIdentityFromMetadata(TEST_CUSTODIAN, TEST_NAMESPACE_BYTES, TEST_METADATA_1), testKey1,
      ManagedKeyState.ACTIVE, TEST_METADATA_1, 1000L);
    keyData2 = new ManagedKeyData(ManagedKeyIdentityUtils
      .fullKeyIdentityFromMetadata(TEST_CUSTODIAN, TEST_NAMESPACE_BYTES, TEST_METADATA_2), testKey2,
      ManagedKeyState.ACTIVE, TEST_METADATA_2, 2000L);
    keyData3 = new ManagedKeyData(ManagedKeyIdentityUtils
      .fullKeyIdentityFromMetadata(TEST_CUSTODIAN, TEST_NAMESPACE_BYTES, TEST_METADATA_3), testKey3,
      ManagedKeyState.ACTIVE, TEST_METADATA_3, 3000L);

    // Create test paths
    keyPath1 = new Path("/system/keys/key1");
    keyPath2 = new Path("/system/keys/key2");
    keyPath3 = new Path("/system/keys/key3");
  }

  @Test
  public void testCreateCacheWithSingleSystemKey() throws Exception {
    // Setup
    List<Path> keyPaths = Collections.singletonList(keyPath1);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNotNull(cache);
    assertSame(keyData1, cache.getLatestSystemKey());
    assertSame(keyData1,
      cache.getSystemKeyByIdentity(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(keyData1.getKeyCustodian(),
          keyData1.getKeyNamespaceBytes(), keyData1.getPartialIdentity())));
    assertNull(cache.getSystemKeyByIdentity(new byte[] { 1, 2, 3 })); // Non-existent identity

    verify(mockAccessor).getAllSystemKeyFiles();
    verify(mockAccessor).loadSystemKey(keyPath1);
  }

  @Test
  public void testCreateCacheWithMultipleSystemKeys() throws Exception {
    // Setup - keys should be processed in order, first one becomes latest
    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2, keyPath3);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(keyData2);
    when(mockAccessor.loadSystemKey(keyPath3)).thenReturn(keyData3);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNotNull(cache);
    assertSame(keyData1, cache.getLatestSystemKey()); // First key becomes latest

    // All keys should be accessible by full identity
    assertSame(keyData1,
      cache.getSystemKeyByIdentity(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(keyData1.getKeyCustodian(),
          keyData1.getKeyNamespaceBytes(), keyData1.getPartialIdentity())));
    assertSame(keyData2,
      cache.getSystemKeyByIdentity(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(keyData2.getKeyCustodian(),
          keyData2.getKeyNamespaceBytes(), keyData2.getPartialIdentity())));
    assertSame(keyData3,
      cache.getSystemKeyByIdentity(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(keyData3.getKeyCustodian(),
          keyData3.getKeyNamespaceBytes(), keyData3.getPartialIdentity())));

    // Non-existent identity should return null
    assertNull(cache.getSystemKeyByIdentity(new byte[] { 1, 2, 3 }));

    verify(mockAccessor).getAllSystemKeyFiles();
    verify(mockAccessor).loadSystemKey(keyPath1);
    verify(mockAccessor).loadSystemKey(keyPath2);
    verify(mockAccessor).loadSystemKey(keyPath3);
  }

  @Test
  public void testCreateCacheWithNoSystemKeyFiles() throws Exception {
    // Setup - this covers the uncovered lines 46-47
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(Collections.emptyList());

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNull(cache);
    verify(mockAccessor).getAllSystemKeyFiles();
  }

  @Test
  public void testCreateCacheWithEmptyKeyFilesList() throws Exception {
    // Setup - alternative empty scenario
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(new ArrayList<>());

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNull(cache);
    verify(mockAccessor).getAllSystemKeyFiles();
  }

  @Test
  public void testGetLatestSystemKeyConsistency() throws Exception {
    // Setup
    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(keyData2);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify - latest key should be consistent across calls
    ManagedKeyData latest1 = cache.getLatestSystemKey();
    ManagedKeyData latest2 = cache.getLatestSystemKey();

    assertNotNull(latest1);
    assertSame(latest1, latest2);
    assertSame(keyData1, latest1); // First key should be latest
  }

  @Test
  public void testGetSystemKeyByIdentityWithDifferentKeys() throws Exception {
    // Setup
    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2, keyPath3);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(keyData2);
    when(mockAccessor.loadSystemKey(keyPath3)).thenReturn(keyData3);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify each key can be retrieved by its unique full identity
    byte[] identity1 = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
      keyData1.getKeyCustodian(), keyData1.getKeyNamespaceBytes(), keyData1.getPartialIdentity());
    byte[] identity2 = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
      keyData2.getKeyCustodian(), keyData2.getKeyNamespaceBytes(), keyData2.getPartialIdentity());
    byte[] identity3 = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
      keyData3.getKeyCustodian(), keyData3.getKeyNamespaceBytes(), keyData3.getPartialIdentity());

    // Identities should be different
    assert !java.util.Arrays.equals(identity1, identity2);
    assert !java.util.Arrays.equals(identity2, identity3);
    assert !java.util.Arrays.equals(identity1, identity3);

    // Each key should be retrievable by its full identity
    assertSame(keyData1, cache.getSystemKeyByIdentity(identity1));
    assertSame(keyData2, cache.getSystemKeyByIdentity(identity2));
    assertSame(keyData3, cache.getSystemKeyByIdentity(identity3));
  }

  @Test
  public void testGetSystemKeyByIdentityWithNonExistentIdentity() throws Exception {
    // Setup
    List<Path> keyPaths = Collections.singletonList(keyPath1);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNotNull(cache);

    // Test various non-existent identities
    assertNull(cache.getSystemKeyByIdentity(null));
    assertNull(cache.getSystemKeyByIdentity(new byte[0]));
    assertNull(cache.getSystemKeyByIdentity(new byte[] { 1, 2, 3 }));

    // But the actual full identity should work
    assertSame(keyData1,
      cache.getSystemKeyByIdentity(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(keyData1.getKeyCustodian(),
          keyData1.getKeyNamespaceBytes(), keyData1.getPartialIdentity())));
  }

  @Test(expected = IOException.class)
  public void testCreateCacheWithAccessorIOException() throws Exception {
    // Setup - accessor throws IOException
    when(mockAccessor.getAllSystemKeyFiles()).thenThrow(new IOException("File system error"));

    // Execute - should propagate the IOException
    SystemKeyCache.createCache(mockAccessor);
  }

  @Test(expected = IOException.class)
  public void testCreateCacheWithLoadSystemKeyIOException() throws Exception {
    // Setup - loading key throws IOException
    List<Path> keyPaths = Collections.singletonList(keyPath1);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenThrow(new IOException("Key load error"));

    // Execute - should propagate the IOException
    SystemKeyCache.createCache(mockAccessor);
  }

  @Test
  public void testCacheWithKeysHavingSameFullIdentity() throws Exception {
    // Setup - two keys with same custodian/namespace/metadata so same full identity
    Key sameKey1 = new SecretKeySpec("identical-bytes".getBytes(), "AES");
    Key sameKey2 = new SecretKeySpec("identical-bytes".getBytes(), "AES");
    String sameMetadata = "same-metadata";

    ManagedKeyData sameManagedKey1 =
      new ManagedKeyData(ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(TEST_CUSTODIAN,
        TEST_NAMESPACE_BYTES, sameMetadata), sameKey1, ManagedKeyState.ACTIVE, sameMetadata, 1000L);
    ManagedKeyData sameManagedKey2 =
      new ManagedKeyData(ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(TEST_CUSTODIAN,
        TEST_NAMESPACE_BYTES, sameMetadata), sameKey2, ManagedKeyState.ACTIVE, sameMetadata, 2000L);

    // Same custodian/namespace/metadata -> same partial identity -> same full identity
    assertTrue(java.util.Arrays.equals(
      ManagedKeyIdentityUtils.constructRowKeyForIdentity(sameManagedKey1.getKeyCustodian(),
        sameManagedKey1.getKeyNamespaceBytes(), sameManagedKey1.getPartialIdentity()),
      ManagedKeyIdentityUtils.constructRowKeyForIdentity(sameManagedKey2.getKeyCustodian(),
        sameManagedKey2.getKeyNamespaceBytes(), sameManagedKey2.getPartialIdentity())));

    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(sameManagedKey1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(sameManagedKey2);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify - second key should overwrite first in the map due to same full identity
    assertNotNull(cache);
    assertSame(sameManagedKey1, cache.getLatestSystemKey()); // First is still latest

    ManagedKeyData retrievedKey = cache.getSystemKeyByIdentity(
      ManagedKeyIdentityUtils.constructRowKeyForIdentity(sameManagedKey1.getKeyCustodian(),
        sameManagedKey1.getKeyNamespaceBytes(), sameManagedKey1.getPartialIdentity()));
    assertSame(sameManagedKey2, retrievedKey); // Last one wins
  }

  @Test
  public void testCreateCacheWithUnexpectedNullKeyData() throws Exception {
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(Arrays.asList(keyPath1));
    when(mockAccessor.loadSystemKey(keyPath1)).thenThrow(new RuntimeException("Key load error"));

    RuntimeException ex = assertThrows(RuntimeException.class, () -> {
      SystemKeyCache.createCache(mockAccessor);
    });
    assertTrue(ex.getMessage().equals("Key load error"));
  }
}
