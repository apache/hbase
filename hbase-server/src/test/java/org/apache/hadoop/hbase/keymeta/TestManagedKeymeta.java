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
import java.util.function.BiFunction;
import java.util.function.Function;
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
      .cloneWithoutKey(), managedKeys.get(0).cloneWithoutKey());

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
    doTestWithClientSideServiceException((mockStub, networkError) -> {
      try {
        when(mockStub.enableKeyManagement(any(), any())).thenThrow(networkError);
      } catch (ServiceException e) {
        // We are just setting up the mock, so no exception is expected here.
        throw new RuntimeException("Unexpected ServiceException", e);
      }
      return null;
    }, (client) -> {
      try {
        client.enableKeyManagement(new byte[0], "namespace");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void testGetManagedKeysWithClientSideServiceException() throws Exception {
    // Similar test for getManagedKeys method
    doTestWithClientSideServiceException((mockStub, networkError) -> {
      try {
        when(mockStub.getManagedKeys(any(), any())).thenThrow(networkError);
      } catch (ServiceException e) {
        // We are just setting up the mock, so no exception is expected here.
        throw new RuntimeException("Unexpected ServiceException", e);
      }
      return null;
    }, (client) -> {
      try {
        client.getManagedKeys(new byte[0], "namespace");
      } catch (IOException | KeyException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
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
    doTestWithClientSideServiceException((mockStub, networkError) -> {
      try {
        when(mockStub.rotateSTK(any(), any())).thenThrow(networkError);
      } catch (ServiceException e) {
        // We are just setting up the mock, so no exception is expected here.
        throw new RuntimeException("Unexpected ServiceException", e);
      }
      return null;
    }, (client) -> {
      try {
        client.rotateSTK();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  private void
    doTestWithClientSideServiceException(BiFunction<
      ManagedKeysProtos.ManagedKeysService.BlockingInterface, ServiceException, Void> setupFunction,
      Function<KeymetaAdminClient, Void> testFunction) throws Exception {
    ManagedKeysProtos.ManagedKeysService.BlockingInterface mockStub =
      mock(ManagedKeysProtos.ManagedKeysService.BlockingInterface.class);

    ServiceException networkError = new ServiceException("Network error");
    networkError.initCause(new IOException("Network error"));

    KeymetaAdminClient client = new KeymetaAdminClient(TEST_UTIL.getConnection());
    // Use reflection to set the stub
    Field stubField = KeymetaAdminClient.class.getDeclaredField("stub");
    stubField.setAccessible(true);
    stubField.set(client, mockStub);

    setupFunction.apply(mockStub, networkError);

    IOException exception = assertThrows(IOException.class, () -> {
      try {
        testFunction.apply(client);
      } catch (RuntimeException e) {
        throw e.getCause();
      }
    });

    assertTrue(exception.getMessage().contains("Network error"));
  }
}
