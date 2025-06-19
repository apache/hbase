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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import javax.crypto.KeyGenerator;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyData {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyData.class);

  private byte[] keyCust;
  private String keyNamespace;
  private Key theKey;
  private ManagedKeyState keyState;
  private String keyMetadata;
  private ManagedKeyData managedKeyData;

  @Before
  public void setUp() throws NoSuchAlgorithmException {
    keyCust = "testCustodian".getBytes();
    keyNamespace = "testNamespace";
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    theKey = keyGen.generateKey();
    keyState = ManagedKeyState.ACTIVE;
    keyMetadata = "testMetadata";
    managedKeyData = new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata);
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
  public void testConstructorWithCounts() {
    long refreshTimestamp = System.currentTimeMillis();
    long readOpCount = 10;
    long writeOpCount = 5;
    ManagedKeyData keyDataWithCounts =
      new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata, refreshTimestamp,
        readOpCount, writeOpCount);

    assertEquals(refreshTimestamp, keyDataWithCounts.getRefreshTimestamp());
    assertEquals(readOpCount, keyDataWithCounts.getReadOpCount());
    assertEquals(writeOpCount, keyDataWithCounts.getWriteOpCount());
  }

  @Test
  public void testConstructorNullChecks() {
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(null, keyNamespace, theKey, keyState, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, null, theKey, keyState, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, keyNamespace, theKey, null, keyMetadata));
    assertThrows(NullPointerException.class,
      () -> new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, null));
  }

  @Test
  public void testConstructorNegativeCountChecks() {
    assertThrows(IllegalArgumentException.class,
      () -> new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata, 0, -1, 0));
    assertThrows(IllegalArgumentException.class,
      () -> new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata, 0, 0, -1));
  }

  @Test
  public void testCloneWithoutKey() {
    ManagedKeyData cloned = managedKeyData.cloneWithoutKey();
    assertNull(cloned.getTheKey());
    assertEquals(managedKeyData.getKeyCustodian(), cloned.getKeyCustodian());
    assertEquals(managedKeyData.getKeyNamespace(), cloned.getKeyNamespace());
    assertEquals(managedKeyData.getKeyState(), cloned.getKeyState());
    assertEquals(managedKeyData.getKeyMetadata(), cloned.getKeyMetadata());
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
      new ManagedKeyData(keyCust, keyNamespace, null, keyState, keyMetadata);
    assertEquals(0, nullKeyData.getKeyChecksum());
  }

  @Test
  public void testConstructKeyChecksum() {
    byte[] data = "testData".getBytes();
    long checksum = ManagedKeyData.constructKeyChecksum(data);
    assertNotEquals(0, checksum);
  }

  @Test
  public void testGetKeyMetadataHash() {
    byte[] hash = managedKeyData.getKeyMetadataHash();
    assertNotNull(hash);
    assertEquals(16, hash.length); // MD5 hash is 16 bytes long
  }

  @Test
  public void testGetKeyMetadataHashEncoded() {
    String encodedHash = managedKeyData.getKeyMetadataHashEncoded();
    assertNotNull(encodedHash);
    assertEquals(24, encodedHash.length()); // Base64 encoded MD5 hash is 24 characters long
  }

  @Test
  public void testConstructMetadataHash() {
    byte[] hash = ManagedKeyData.constructMetadataHash(keyMetadata);
    assertNotNull(hash);
    assertEquals(16, hash.length); // MD5 hash is 16 bytes long
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
    ManagedKeyData same = new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata);
    assertEquals(managedKeyData, same);

    ManagedKeyData different =
      new ManagedKeyData("differentCust".getBytes(), keyNamespace, theKey, keyState, keyMetadata);
    assertNotEquals(managedKeyData, different);
  }

  @Test
  public void testHashCode() {
    ManagedKeyData same = new ManagedKeyData(keyCust, keyNamespace, theKey, keyState, keyMetadata);
    assertEquals(managedKeyData.hashCode(), same.hashCode());

    ManagedKeyData different =
      new ManagedKeyData("differentCust".getBytes(), keyNamespace, theKey, keyState, keyMetadata);
    assertNotEquals(managedKeyData.hashCode(), different.hashCode());
  }

  @Test
  public void testConstants() {
    assertEquals("*", ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(ManagedKeyProvider.encodeToStr(ManagedKeyData.KEY_SPACE_GLOBAL.getBytes()),
      ManagedKeyData.KEY_GLOBAL_CUSTODIAN);
  }
}
