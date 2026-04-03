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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import javax.crypto.KeyGenerator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
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
  private ManagedKeyIdentity custNamespacePrefix;
  private byte[] keyCust;
  private byte[] keyNamespace;
  private String keyMetadata;
  private byte[] wrappedKey;
  private Key testKey;

  @Before
  public void setUp() throws Exception {
    mockProvider = mock(ManagedKeyProvider.class);
    mockAccessor = mock(KeymetaTableAccessor.class);
    keyCust = "testCustodian".getBytes();
    keyNamespace = Bytes.toBytes("testNamespace");
    custNamespacePrefix = new KeyIdentityPrefixBytesBacked(keyCust, keyNamespace);
    keyMetadata = "testMetadata";
    wrappedKey = new byte[] { 1, 2, 3, 4 };

    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    testKey = keyGen.generateKey();
  }

  @Test
  public void testRetrieveKeyWithNullResponse() throws Exception {
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any())).thenReturn(null);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(exception.getMessage(), true, exception.getMessage()
      .contains("Invalid key received from key provider (null/metadata/state)"));
  }

  @Test
  public void testRetrieveKeyWithNullMetadata() throws Exception {
    // Create a mock that returns null for getKeyMetadata()
    ManagedKeyData mockKeyData = mock(ManagedKeyData.class);
    when(mockKeyData.getKeyMetadata()).thenReturn(null);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(mockKeyData);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(exception.getMessage(), true, exception.getMessage()
      .contains("Invalid key received from key provider (null/metadata/state)"));
  }

  @Test
  public void testRetrieveKeyWithMismatchedMetadata() throws Exception {
    String differentMetadata = "differentMetadata";
    ManagedKeyData keyDataWithDifferentMetadata =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, differentMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithDifferentMetadata);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(exception.getMessage(), true,
      exception.getMessage().contains("Invalid key received from key provider"));
  }

  @Test
  public void testRetrieveKeyWithDisabledState() throws Exception {
    ManagedKeyData keyDataWithDisabledState =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.DISABLED, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithDisabledState);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(exception.getMessage(), true,
      exception.getMessage().contains("Invalid key received from key provider"));
  }

  @Test
  public void testRetrieveKeySuccess() throws Exception {
    ManagedKeyData validKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(validKeyData);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor,
      custNamespacePrefix, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(keyMetadata, result.getKeyMetadata());
    assertEquals(ManagedKeyState.ACTIVE, result.getKeyState());
  }

  @Test
  public void testRetrieveKeyWithFailedState() throws Exception {
    // FAILED state is allowed (unlike DISABLED), so this should succeed
    ManagedKeyData keyDataWithFailedState =
      new ManagedKeyData(keyCust, keyNamespace, null, ManagedKeyState.FAILED, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithFailedState);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor,
      custNamespacePrefix, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(ManagedKeyState.FAILED, result.getKeyState());
  }

  @Test
  public void testRetrieveKeyWithInactiveState() throws Exception {
    // INACTIVE state is allowed, so this should succeed
    ManagedKeyData keyDataWithInactiveState =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.INACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithInactiveState);

    ManagedKeyData result = KeyManagementUtils.retrieveKey(mockProvider, mockAccessor,
      custNamespacePrefix, keyMetadata, wrappedKey);

    assertNotNull(result);
    assertEquals(ManagedKeyState.INACTIVE, result.getKeyState());
  }

  @Test
  public void testRetrieveKeyWithMismatchedCustodian() throws Exception {
    ManagedKeyData keyDataWithWrongCustodian = new ManagedKeyData("otherCustodian".getBytes(),
      keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithWrongCustodian);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true, exception.getMessage().contains("scope does not match request"));
    verify(mockAccessor, never()).addKey(any());
  }

  @Test
  public void testRetrieveKeyWithMismatchedNamespace() throws Exception {
    ManagedKeyData keyDataWithWrongNamespace = new ManagedKeyData(keyCust,
      Bytes.toBytes("otherNamespace"), testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(keyDataWithWrongNamespace);

    KeyException exception = assertThrows(KeyException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertNotNull(exception.getMessage());
    assertEquals(true, exception.getMessage().contains("scope does not match request"));
    verify(mockAccessor, never()).addKey(any());
  }

  @Test
  public void testRetrieveKeyPersistenceFailure_ThrowsIOException() throws Exception {
    ManagedKeyData validKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(validKeyData);
    doThrow(new IOException("persist failed")).when(mockAccessor).addKey(any());

    IOException exception = assertThrows(IOException.class, () -> {
      KeyManagementUtils.retrieveKey(mockProvider, mockAccessor, custNamespacePrefix, keyMetadata,
        wrappedKey);
    });

    assertEquals("persist failed", exception.getMessage());
  }

  @Test
  public void testRefreshKeyWithNoChange_NullAccessor() throws Exception {
    doTestRefreshKey_OnNoChange(null);
  }

  @Test
  public void testRefreshKey_updateRefreshTimestamp_OnNoChange() throws Exception {
    doTestRefreshKey_OnNoChange(mockAccessor);
  }

  private void doTestRefreshKey_OnNoChange(KeymetaTableAccessor accessor) throws Exception {
    ManagedKeyData keyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any())).thenReturn(keyData);

    ManagedKeyData result = KeyManagementUtils.refreshKey(mockProvider, accessor, keyData);
    assertNotNull(result);
    assertEquals(keyData, result);
    if (accessor != null) {
      verify(accessor).updateRefreshTimestamp(keyData);
    }
  }

  @Test
  public void testRefreshKeyWithChange_NullAccessor() throws Exception {
    doTestRefreshKeyWith_StateChange(null);
  }

  @Test
  public void testRefreshKeyWith_StateChange() throws Exception {
    doTestRefreshKeyWith_StateChange(mockAccessor);
  }

  private void doTestRefreshKeyWith_StateChange(KeymetaTableAccessor accessor) throws Exception {
    ManagedKeyData keyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    ManagedKeyData newKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.INACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(newKeyData);

    ManagedKeyData result = KeyManagementUtils.refreshKey(mockProvider, accessor, keyData);
    assertNotNull(result);
    assertEquals(newKeyData, result);
    if (accessor != null) {
      verify(accessor).updateActiveState(keyData, ManagedKeyState.INACTIVE, false);
    }
  }

  /** Test that when refresh fails and returns a FAILED key, we keep the current good key intact. */
  @Test
  public void testRefreshKeyFailedState_NullAccessor() throws Exception {
    doTestRefreshKey_OnFailedState(null);
  }

  @Test
  public void testRefreshKeyFailedState() throws Exception {
    doTestRefreshKey_OnFailedState(mockAccessor);
  }

  private void doTestRefreshKey_OnFailedState(KeymetaTableAccessor accessor) throws Exception {
    ManagedKeyData keyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    ManagedKeyData newKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.FAILED, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(newKeyData);

    ManagedKeyData result = KeyManagementUtils.refreshKey(mockProvider, accessor, keyData);
    assertNotNull(result);
    assertEquals(keyData, result);
    if (accessor != null) {
      verify(accessor).updateRefreshTimestamp(keyData);
    }
  }

  /** Test that when refresh throws an exception, we keep the current good key intact. */
  @Test
  public void testRefreshKeyException_NullAccessor() throws Exception {
    ManagedKeyData keyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenThrow(new IOException("Test exception"));

    ManagedKeyData result = KeyManagementUtils.refreshKey(mockProvider, null, keyData);
    assertNotNull(result);
    assertEquals(keyData, result);
  }

  @Test
  public void testRefreshKey_KeyDisabled_NullAccessor() throws Exception {
    doTestRefreshKey_KeyDisabled(null);
  }

  @Test
  public void testRefreshKey_KeyDisabled() throws Exception {
    doTestRefreshKey_KeyDisabled(mockAccessor);
  }

  private void doTestRefreshKey_KeyDisabled(KeymetaTableAccessor accessor) throws Exception {
    ManagedKeyData keyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.ACTIVE, keyMetadata);
    ManagedKeyData newKeyData =
      new ManagedKeyData(keyCust, keyNamespace, testKey, ManagedKeyState.DISABLED, keyMetadata);
    when(mockProvider.unwrapKey(any(ManagedKeyIdentity.class), any(), any()))
      .thenReturn(newKeyData);

    ManagedKeyData result = KeyManagementUtils.refreshKey(mockProvider, accessor, keyData);
    assertNotNull(result);
    assertEquals(newKeyData, result);
    if (accessor != null) {
      verify(accessor).disableKey(keyData);
    }
  }
}
