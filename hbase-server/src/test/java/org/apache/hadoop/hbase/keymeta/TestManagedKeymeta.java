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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.KeyException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ManagedKeysProtos;

/**
 * Tests the admin API via both RPC and local calls.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestManagedKeymeta extends ManagedKeyTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeymeta.class);

  /**
   * Functional interface for setup operations that can throw ServiceException.
   */
  @FunctionalInterface
  interface SetupFunction {
    void setup(ManagedKeysProtos.ManagedKeysService.BlockingInterface mockStub,
      ServiceException networkError) throws ServiceException;
  }

  /**
   * Functional interface for test operations that can throw checked exceptions.
   */
  @FunctionalInterface
  interface TestFunction {
    void test(KeymetaAdminClient client) throws IOException, KeyException;
  }

  @Test
  public void testEnableLocal() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeymetaAdmin keymetaAdmin = master.getKeymetaAdmin();
    doTestEnable(keymetaAdmin);
  }

  @Test
  public void testEnableOverRPC() throws Exception {
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    doTestEnable(adminClient);
  }

  private void doTestEnable(KeymetaAdmin adminClient) throws IOException, KeyException {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider managedKeyProvider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    String cust = "cust1";
    byte[] custBytes = cust.getBytes();
    ManagedKeyData managedKey =
      adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataSingleKey(managedKey, ManagedKeyState.ACTIVE);

    List<ManagedKeyData> managedKeys =
      adminClient.getManagedKeys(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(managedKeyProvider.getLastGeneratedKeyData(cust, ManagedKeyData.KEY_SPACE_GLOBAL)
      .cloneWithoutSensitiveData(), managedKeys.get(0).cloneWithoutSensitiveData());

    String nonExistentCust = "nonExistentCust";
    byte[] nonExistentBytes = nonExistentCust.getBytes();
    managedKeyProvider.setMockedKeyState(nonExistentCust, ManagedKeyState.FAILED);
    ManagedKeyData managedKey1 =
      adminClient.enableKeyManagement(nonExistentBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataSingleKey(managedKey1, ManagedKeyState.FAILED);

    String disabledCust = "disabledCust";
    byte[] disabledBytes = disabledCust.getBytes();
    managedKeyProvider.setMockedKeyState(disabledCust, ManagedKeyState.DISABLED);
    ManagedKeyData managedKey2 =
      adminClient.enableKeyManagement(disabledBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataSingleKey(managedKey2, ManagedKeyState.DISABLED);
  }

  private static void assertKeyDataSingleKey(ManagedKeyData managedKeyState,
    ManagedKeyState keyState) {
    assertNotNull(managedKeyState);
    assertEquals(keyState, managedKeyState.getKeyState());
  }

  @Test
  public void testEnableKeyManagementWithExceptionOnGetManagedKey() throws Exception {
    MockManagedKeyProvider managedKeyProvider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(TEST_UTIL.getConfiguration());
    managedKeyProvider.setShouldThrowExceptionOnGetManagedKey(true);
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    IOException exception = assertThrows(IOException.class,
      () -> adminClient.enableKeyManagement(new byte[0], "namespace"));
    assertTrue(exception.getMessage().contains("key_cust must not be empty"));
  }

  @Test
  public void testEnableKeyManagementWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException((mockStub,
      networkError) -> when(mockStub.enableKeyManagement(any(), any())).thenThrow(networkError),
      (client) -> client.enableKeyManagement(new byte[0], "namespace"));
  }

  @Test
  public void testGetManagedKeysWithClientSideServiceException() throws Exception {
    // Similar test for getManagedKeys method
    doTestWithClientSideServiceException((mockStub,
      networkError) -> when(mockStub.getManagedKeys(any(), any())).thenThrow(networkError),
      (client) -> client.getManagedKeys(new byte[0], "namespace"));
  }

  @Test
  public void testRotateSTKLocal() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeymetaAdmin keymetaAdmin = master.getKeymetaAdmin();
    doTestRotateSTK(keymetaAdmin);
  }

  @Test
  public void testRotateSTKOverRPC() throws Exception {
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    doTestRotateSTK(adminClient);
  }

  private void doTestRotateSTK(KeymetaAdmin adminClient) throws IOException {
    // Call rotateSTK - since no actual system key change has occurred,
    // this should return false (no rotation performed)
    boolean result = adminClient.rotateSTK();
    assertFalse("rotateSTK should return false when no key change is detected", result);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    ManagedKeyData currentSystemKey = master.getSystemKeyCache().getLatestSystemKey();

    MockManagedKeyProvider managedKeyProvider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(TEST_UTIL.getConfiguration());
    // Once we enable multikeyGenMode on MockManagedKeyProvider, every call should return a new key
    // which should trigger a rotation.
    managedKeyProvider.setMultikeyGenMode(true);
    result = adminClient.rotateSTK();
    assertTrue("rotateSTK should return true when a new key is detected", result);

    ManagedKeyData newSystemKey = master.getSystemKeyCache().getLatestSystemKey();
    assertNotEquals("newSystemKey should be different from currentSystemKey", currentSystemKey,
      newSystemKey);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    assertEquals("regionServer should have the same new system key", newSystemKey,
      regionServer.getSystemKeyCache().getLatestSystemKey());

  }

  @Test
  public void testRotateSTKWithExceptionOnGetSystemKey() throws Exception {
    MockManagedKeyProvider managedKeyProvider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(TEST_UTIL.getConfiguration());
    managedKeyProvider.setShouldThrowExceptionOnGetSystemKey(true);
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    IOException exception = assertThrows(IOException.class, () -> adminClient.rotateSTK());
    assertTrue(exception.getMessage().contains("Test exception on getSystemKey"));
  }

  @Test
  public void testRotateSTKWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException(
      (mockStub, networkError) -> when(mockStub.rotateSTK(any(), any())).thenThrow(networkError),
      (client) -> client.rotateSTK());
  }

  private void doTestWithClientSideServiceException(SetupFunction setupFunction,
    TestFunction testFunction) throws Exception {
    ManagedKeysProtos.ManagedKeysService.BlockingInterface mockStub =
      mock(ManagedKeysProtos.ManagedKeysService.BlockingInterface.class);

    ServiceException networkError = new ServiceException("Network error");
    networkError.initCause(new IOException("Network error"));

    KeymetaAdminClient client = new KeymetaAdminClient(TEST_UTIL.getConnection());
    // Use reflection to set the stub
    Field stubField = KeymetaAdminClient.class.getDeclaredField("stub");
    stubField.setAccessible(true);
    stubField.set(client, mockStub);

    // Setup the mock
    setupFunction.setup(mockStub, networkError);

    // Execute test function and expect IOException
    IOException exception = assertThrows(IOException.class, () -> testFunction.test(client));

    assertTrue(exception.getMessage().contains("Network error"));
  }

  @Test
  public void testDisableKeyManagementLocal() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeymetaAdmin keymetaAdmin = master.getKeymetaAdmin();
    doTestDisableKeyManagement(keymetaAdmin);
  }

  @Test
  public void testDisableKeyManagementOverRPC() throws Exception {
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    doTestDisableKeyManagement(adminClient);
  }

  private void doTestDisableKeyManagement(KeymetaAdmin adminClient)
    throws IOException, KeyException {
    String cust = "cust2";
    byte[] custBytes = cust.getBytes();

    // First enable key management
    ManagedKeyData managedKey =
      adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertNotNull(managedKey);
    assertKeyDataSingleKey(managedKey, ManagedKeyState.ACTIVE);

    // Now disable it
    List<ManagedKeyData> disabledKeys =
      adminClient.disableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertNotNull(disabledKeys);
    assertEquals(1, disabledKeys.size());
    assertEquals(ManagedKeyState.DISABLED, disabledKeys.get(0).getKeyState());
  }

  @Test
  public void testDisableKeyManagementWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException(
      (mockStub, networkError) -> when(mockStub.disableKeyManagement(any(), any()))
        .thenThrow(networkError),
      (client) -> client.disableKeyManagement(new byte[0], "namespace"));
  }

  @Test
  public void testDisableManagedKeyLocal() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeymetaAdmin keymetaAdmin = master.getKeymetaAdmin();
    doTestDisableManagedKey(keymetaAdmin);
  }

  @Test
  public void testDisableManagedKeyOverRPC() throws Exception {
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    doTestDisableManagedKey(adminClient);
  }

  private void doTestDisableManagedKey(KeymetaAdmin adminClient) throws IOException, KeyException {
    String cust = "cust3";
    byte[] custBytes = cust.getBytes();

    // First enable key management to create a key
    ManagedKeyData managedKey =
      adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertNotNull(managedKey);
    assertKeyDataSingleKey(managedKey, ManagedKeyState.ACTIVE);
    byte[] keyMetadataHash = managedKey.getKeyMetadataHash();

    // Now disable the specific key
    ManagedKeyData disabledKey =
      adminClient.disableManagedKey(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL, keyMetadataHash);
    assertNotNull(disabledKey);
    assertEquals(ManagedKeyState.DISABLED, disabledKey.getKeyState());
  }

  @Test
  public void testDisableManagedKeyWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException(
      (mockStub, networkError) -> when(mockStub.disableManagedKey(any(), any()))
        .thenThrow(networkError),
      (client) -> client.disableManagedKey(new byte[0], "namespace", new byte[0]));
  }

  // Note: Integration tests for rotateManagedKey are complex due to keymeta table persistence.
  // The business logic is already tested in TestKeymetaAdminImpl with mocks.
  // Here we test that the RPC layer properly handles exceptions.

  @Test
  public void testRotateManagedKeyWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException((mockStub,
      networkError) -> when(mockStub.rotateManagedKey(any(), any())).thenThrow(networkError),
      (client) -> client.rotateManagedKey(new byte[0], "namespace"));
  }

  // Note: Integration tests for refreshManagedKeys are complex due to keymeta table persistence.
  // The business logic is already tested in TestKeymetaAdminImpl with mocks.
  // Here we test that the RPC layer properly handles exceptions.

  @Test
  public void testRefreshManagedKeysWithClientSideServiceException() throws Exception {
    doTestWithClientSideServiceException((mockStub,
      networkError) -> when(mockStub.refreshManagedKeys(any(), any())).thenThrow(networkError),
      (client) -> client.refreshManagedKeys(new byte[0], "namespace"));
  }
}
