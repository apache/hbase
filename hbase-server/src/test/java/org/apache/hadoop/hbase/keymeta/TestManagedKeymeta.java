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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

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
    String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
    List<ManagedKeyData> managedKeyStates =
      adminClient.enableKeyManagement(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(managedKeyStates, ManagedKeyState.ACTIVE);

    List<ManagedKeyData> managedKeys =
      adminClient.getManagedKeys(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(1, managedKeys.size());
    assertEquals(managedKeyProvider.getLastGeneratedKeyData(cust, ManagedKeyData.KEY_SPACE_GLOBAL)
      .cloneWithoutKey(), managedKeys.get(0).cloneWithoutKey());

    String nonExistentCust = "nonExistentCust";
    managedKeyProvider.setMockedKeyState(nonExistentCust, ManagedKeyState.FAILED);
    List<ManagedKeyData> keyDataList1 = adminClient.enableKeyManagement(
      ManagedKeyProvider.encodeToStr(nonExistentCust.getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(keyDataList1, ManagedKeyState.FAILED);

    String disabledCust = "disabledCust";
    managedKeyProvider.setMockedKeyState(disabledCust, ManagedKeyState.DISABLED);
    List<ManagedKeyData> keyDataList2 = adminClient.enableKeyManagement(
      ManagedKeyProvider.encodeToStr(disabledCust.getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(keyDataList2, ManagedKeyState.DISABLED);
  }

  private static void assertKeyDataListSingleKey(List<ManagedKeyData> managedKeyStates,
    ManagedKeyState keyState) {
    assertNotNull(managedKeyStates);
    assertEquals(1, managedKeyStates.size());
    assertEquals(keyState, managedKeyStates.get(0).getKeyState());
  }

  @Test
  public void testEnableKeyManagementWithServiceException() throws Exception {
    ManagedKeysProtos.ManagedKeysService.BlockingInterface mockStub =
      mock(ManagedKeysProtos.ManagedKeysService.BlockingInterface.class);

    ServiceException networkError = new ServiceException("Network error");
    networkError.initCause(new IOException("Network error"));
    when(mockStub.enableKeyManagement(any(), any())).thenThrow(networkError);

    KeymetaAdminClient client = new KeymetaAdminClient(TEST_UTIL.getConnection());
    // Use reflection to set the stub
    Field stubField = KeymetaAdminClient.class.getDeclaredField("stub");
    stubField.setAccessible(true);
    stubField.set(client, mockStub);

    IOException exception = assertThrows(IOException.class, () -> {
      client.enableKeyManagement("cust", "namespace");
    });

    assertTrue(exception.getMessage().contains("Network error"));
  }

  @Test
  public void testGetManagedKeysWithServiceException() throws Exception {
    // Similar test for getManagedKeys method
    ManagedKeysProtos.ManagedKeysService.BlockingInterface mockStub =
      mock(ManagedKeysProtos.ManagedKeysService.BlockingInterface.class);

    ServiceException networkError = new ServiceException("Network error");
    networkError.initCause(new IOException("Network error"));
    when(mockStub.getManagedKeys(any(), any())).thenThrow(networkError);

    KeymetaAdminClient client = new KeymetaAdminClient(TEST_UTIL.getConnection());
    Field stubField = KeymetaAdminClient.class.getDeclaredField("stub");
    stubField.setAccessible(true);
    stubField.set(client, mockStub);

    IOException exception = assertThrows(IOException.class, () -> {
      client.getManagedKeys("cust", "namespace");
    });

    assertTrue(exception.getMessage().contains("Network error"));
  }
}
