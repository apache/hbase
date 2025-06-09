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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.TestKeyProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for SystemKeyCache class
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestSystemKeyCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSystemKeyCache.class);

  @Mock
  private SystemKeyAccessor mockAccessor;

  private static final byte[] TEST_CUSTODIAN = "test-custodian".getBytes();
  private static final String TEST_NAMESPACE = "test-namespace";
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
    keyData1 = new ManagedKeyData(TEST_CUSTODIAN, TEST_NAMESPACE, testKey1,
      ManagedKeyState.ACTIVE, TEST_METADATA_1, 1000L, 0, 0);
    keyData2 = new ManagedKeyData(TEST_CUSTODIAN, TEST_NAMESPACE, testKey2,
      ManagedKeyState.ACTIVE, TEST_METADATA_2, 2000L, 0, 0);
    keyData3 = new ManagedKeyData(TEST_CUSTODIAN, TEST_NAMESPACE, testKey3,
      ManagedKeyState.ACTIVE, TEST_METADATA_3, 3000L, 0, 0);

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
    assertSame(keyData1, cache.getSystemKeyByChecksum(keyData1.getKeyChecksum()));
    assertNull(cache.getSystemKeyByChecksum(999L)); // Non-existent checksum

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

    // All keys should be accessible by checksum
    assertSame(keyData1, cache.getSystemKeyByChecksum(keyData1.getKeyChecksum()));
    assertSame(keyData2, cache.getSystemKeyByChecksum(keyData2.getKeyChecksum()));
    assertSame(keyData3, cache.getSystemKeyByChecksum(keyData3.getKeyChecksum()));

    // Non-existent checksum should return null
    assertNull(cache.getSystemKeyByChecksum(999L));

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
  public void testGetSystemKeyByChecksumWithDifferentKeys() throws Exception {
    // Setup
    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2, keyPath3);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(keyData2);
    when(mockAccessor.loadSystemKey(keyPath3)).thenReturn(keyData3);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify each key can be retrieved by its unique checksum
    long checksum1 = keyData1.getKeyChecksum();
    long checksum2 = keyData2.getKeyChecksum();
    long checksum3 = keyData3.getKeyChecksum();

    // Checksums should be different
    assert checksum1 != checksum2;
    assert checksum2 != checksum3;
    assert checksum1 != checksum3;

    // Each key should be retrievable by its checksum
    assertSame(keyData1, cache.getSystemKeyByChecksum(checksum1));
    assertSame(keyData2, cache.getSystemKeyByChecksum(checksum2));
    assertSame(keyData3, cache.getSystemKeyByChecksum(checksum3));
  }

  @Test
  public void testGetSystemKeyByChecksumWithNonExistentChecksum() throws Exception {
    // Setup
    List<Path> keyPaths = Collections.singletonList(keyPath1);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(keyData1);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify
    assertNotNull(cache);

    // Test various non-existent checksums
    assertNull(cache.getSystemKeyByChecksum(0L));
    assertNull(cache.getSystemKeyByChecksum(-1L));
    assertNull(cache.getSystemKeyByChecksum(Long.MAX_VALUE));
    assertNull(cache.getSystemKeyByChecksum(Long.MIN_VALUE));

    // But the actual checksum should work
    assertSame(keyData1, cache.getSystemKeyByChecksum(keyData1.getKeyChecksum()));
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
  public void testCacheWithKeysHavingSameChecksum() throws Exception {
    // Setup - create two keys that will have the same checksum (same content)
    Key sameKey1 = new SecretKeySpec("identical-bytes".getBytes(), "AES");
    Key sameKey2 = new SecretKeySpec("identical-bytes".getBytes(), "AES");

    ManagedKeyData sameManagedKey1 = new ManagedKeyData(TEST_CUSTODIAN, TEST_NAMESPACE,
      sameKey1, ManagedKeyState.ACTIVE, "metadata-A", 1000L, 0, 0);
    ManagedKeyData sameManagedKey2 = new ManagedKeyData(TEST_CUSTODIAN, TEST_NAMESPACE,
      sameKey2, ManagedKeyState.ACTIVE, "metadata-B", 2000L, 0, 0);

    // Verify they have the same checksum
    assertEquals(sameManagedKey1.getKeyChecksum(), sameManagedKey2.getKeyChecksum());

    List<Path> keyPaths = Arrays.asList(keyPath1, keyPath2);
    when(mockAccessor.getAllSystemKeyFiles()).thenReturn(keyPaths);
    when(mockAccessor.loadSystemKey(keyPath1)).thenReturn(sameManagedKey1);
    when(mockAccessor.loadSystemKey(keyPath2)).thenReturn(sameManagedKey2);

    // Execute
    SystemKeyCache cache = SystemKeyCache.createCache(mockAccessor);

    // Verify - second key should overwrite first in the map due to same checksum
    assertNotNull(cache);
    assertSame(sameManagedKey1, cache.getLatestSystemKey()); // First is still latest

    // The map should contain the second key for the shared checksum
    ManagedKeyData retrievedKey = cache.getSystemKeyByChecksum(sameManagedKey1.getKeyChecksum());
    assertSame(sameManagedKey2, retrievedKey); // Last one wins in TreeMap
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