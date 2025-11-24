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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Key;
import java.security.KeyException;
import javax.crypto.KeyGenerator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests KeyManagementUtils for the difficult to cover error paths.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestKeyManagementUtils {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyManagementUtils.class);

  private ManagedKeyProvider mockProvider;
  private KeymetaTableAccessor mockAccessor;
  private byte[] keyCust;
  private String keyNamespace;
  private String keyMetadata;
  private byte[] wrappedKey;
  private Key testKey;

  @Before
  public void setUp() throws Exception {
    mockProvider = mock(ManagedKeyProvider.class);
    mockAccessor = mock(KeymetaTableAccessor.class);
    keyCust = "testCustodian".getBytes();
    keyNamespace = "testNamespace";
    keyMetadata = "testMetadata";
    wrappedKey = new byte[] { 1, 2, 3, 4 };

    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    testKey = keyGen.generateKey();
  }

  @Test
  public void testRetrieveKeyWithNullResponse() throws Exception {
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(null);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust, keyCust, keyNamespace,
        keyMetadata, wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true, exception.getMessage().contains("Invalid key that is null"));
  }

  @Test
  public void testRetrieveKeyWithNullMetadata() throws Exception {
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    // Create a mock that returns null for getKeyMetadata()
    ManagedKeyData mockKeyData = mock(ManagedKeyData.class);
    when(mockKeyData.getKeyMetadata()).thenReturn(null);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(mockKeyData);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust, keyCust, keyNamespace,
        keyMetadata, wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true, exception.getMessage().contains("Invalid key that is null"));
  }

  @Test
  public void testRetrieveKeyWithMismatchedMetadata() throws Exception {
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    String differentMetadata = "differentMetadata";
    ManagedKeyData keyDataWithDifferentMetadata =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, differentMetadata);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(keyDataWithDifferentMetadata);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust, keyCust, keyNamespace,
        keyMetadata, wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true, exception.getMessage().contains("invalid metadata"));
  }

  @Test
  public void testRetrieveKeyWithDisabledState() throws Exception {
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    ManagedKeyData keyDataWithDisabledState =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.DISABLED, keyMetadata);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(keyDataWithDisabledState);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust, keyCust, keyNamespace,
        keyMetadata, wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true,
      exception.getMessage().contains("Invalid key that is null or having invalid metadata"));
  }

  @Test
  public void testRetrieveKeySuccess() throws Exception {
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    ManagedKeyData validKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(validKeyData);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust,
      keyCust, keyNamespace, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(keyMetadata, result.getKeyMetadata());
    assertEquals(ManagedKeyState.ACTIVE, result.getKeyState());
  }

  @Test
  public void testRetrieveKeyWithFailedState() throws Exception {
    // FAILED state is allowed (unlike DISABLED), so this should succeed
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    ManagedKeyData keyDataWithFailedState =
      new ManagedKeyData(keyCust, keyNamespace, null, ManagedKeyState.FAILED, keyMetadata);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(keyDataWithFailedState);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust,
      keyCust, keyNamespace, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(ManagedKeyState.FAILED, result.getKeyState());
  }

  @Test
  public void testRetrieveKeyWithInactiveState() throws Exception {
    // INACTIVE state is allowed, so this should succeed
    String encKeyCust = ManagedKeyProvider.encodeToStr(keyCust);
    ManagedKeyData keyDataWithInactiveState =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.INACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(), any())).thenReturn(keyDataWithInactiveState);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, encKeyCust,
      keyCust, keyNamespace, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(ManagedKeyState.INACTIVE, result.getKeyState());
  }
}

