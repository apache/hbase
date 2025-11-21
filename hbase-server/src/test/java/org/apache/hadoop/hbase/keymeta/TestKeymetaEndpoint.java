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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.keymeta.KeymetaServiceEndpoint.KeymetaAdminServiceImpl;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyState;

@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeymetaEndpoint.class);

  private static final String KEY_CUST = "keyCust";
  private static final String KEY_NAMESPACE = "keyNamespace";
  private static final String KEY_METADATA1 = "keyMetadata1";
  private static final String KEY_METADATA2 = "keyMetadata2";

  @Mock
  private RpcController controller;
  @Mock
  private MasterServices master;
  @Mock
  private RpcCallback<ManagedKeyResponse> enableKeyManagementDone;
  @Mock
  private RpcCallback<GetManagedKeysResponse> getManagedKeysDone;
  @Mock
  private RpcCallback<GetManagedKeysResponse> disableKeyManagementDone;
  @Mock
  private RpcCallback<ManagedKeyResponse> disableManagedKeyDone;
  @Mock
  private RpcCallback<ManagedKeyResponse> rotateManagedKeyDone;
  @Mock
  private RpcCallback<EmptyMsg> refreshManagedKeysDone;

  KeymetaServiceEndpoint keymetaServiceEndpoint;
  private ManagedKeyResponse.Builder responseBuilder;
  private ManagedKeyRequest.Builder requestBuilder;
  private KeymetaAdminServiceImpl keyMetaAdminService;
  private ManagedKeyData keyData1;
  private ManagedKeyData keyData2;

  @Mock
  private KeymetaAdmin keymetaAdmin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    keymetaServiceEndpoint = new KeymetaServiceEndpoint();
    CoprocessorEnvironment env =
      mock(CoprocessorEnvironment.class, withSettings().extraInterfaces(HasMasterServices.class));
    when(((HasMasterServices) env).getMasterServices()).thenReturn(master);
    keymetaServiceEndpoint.start(env);
    keyMetaAdminService =
      (KeymetaAdminServiceImpl) keymetaServiceEndpoint.getServices().iterator().next();
    responseBuilder = ManagedKeyResponse.newBuilder().setKeyState(ManagedKeyState.KEY_ACTIVE);
    requestBuilder =
      ManagedKeyRequest.newBuilder().setKeyNamespace(ManagedKeyData.KEY_SPACE_GLOBAL);
    keyData1 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key1".getBytes(), "AES"), ACTIVE, KEY_METADATA1);
    keyData2 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key2".getBytes(), "AES"), ACTIVE, KEY_METADATA2);
    when(master.getKeymetaAdmin()).thenReturn(keymetaAdmin);
  }

  @Test
  public void testCreateResponseBuilderValid() throws IOException {
    byte[] cust = "testKey".getBytes();
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.copyFrom(cust)).build();

    ManagedKeyResponse.Builder result = ManagedKeyResponse.newBuilder();
    KeymetaServiceEndpoint.initManagedKeyResponseBuilder(controller, request, result);

    assertNotNull(result);
    assertArrayEquals(cust, result.getKeyCust().toByteArray());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testCreateResponseBuilderEmptyCust() throws IOException {
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    IOException exception = assertThrows(IOException.class, () -> KeymetaServiceEndpoint
      .initManagedKeyResponseBuilder(controller, request, ManagedKeyResponse.newBuilder()));

    assertEquals("key_cust must not be empty", exception.getMessage());
  }

  @Test
  public void testGenerateKeyStateResponse() throws Exception {
    // Arrange
    ManagedKeyResponse response =
      responseBuilder.setKeyCust(ByteString.copyFrom(keyData1.getKeyCustodian()))
        .setKeyNamespace(keyData1.getKeyNamespace()).build();
    List<ManagedKeyData> managedKeyStates = Arrays.asList(keyData1, keyData2);

    // Act
    GetManagedKeysResponse result =
      KeymetaServiceEndpoint.generateKeyStateResponse(managedKeyStates, responseBuilder);

    // Assert
    assertNotNull(response);
    assertNotNull(result.getStateList());
    assertEquals(2, result.getStateList().size());
    assertEquals(ManagedKeyState.KEY_ACTIVE, result.getStateList().get(0).getKeyState());
    assertEquals(0, Bytes.compareTo(keyData1.getKeyCustodian(),
      result.getStateList().get(0).getKeyCust().toByteArray()));
    assertEquals(keyData1.getKeyNamespace(), result.getStateList().get(0).getKeyNamespace());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGenerateKeyStateResponse_Empty() throws Exception {
    // Arrange
    ManagedKeyResponse response =
      responseBuilder.setKeyCust(ByteString.copyFrom(keyData1.getKeyCustodian()))
        .setKeyNamespace(keyData1.getKeyNamespace()).build();
    List<ManagedKeyData> managedKeyStates = new ArrayList<>();

    // Act
    GetManagedKeysResponse result =
      KeymetaServiceEndpoint.generateKeyStateResponse(managedKeyStates, responseBuilder);

    // Assert
    assertNotNull(response);
    assertNotNull(result.getStateList());
    assertEquals(0, result.getStateList().size());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGenerateKeyStatResponse_Success() throws Exception {
    doTestServiceCallForSuccess((controller, request, done) -> keyMetaAdminService
      .enableKeyManagement(controller, request, done), enableKeyManagementDone);
  }

  @Test
  public void testGetManagedKeys_Success() throws Exception {
    doTestServiceCallForSuccess(
      (controller, request, done) -> keyMetaAdminService.getManagedKeys(controller, request, done),
      getManagedKeysDone);
  }

  private <T> void doTestServiceCallForSuccess(ServiceCall<T> svc, RpcCallback<T> done)
    throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenReturn(keyData1);

    // Act
    svc.call(controller, request, done);

    // Assert
    verify(done).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  private interface ServiceCall<T> {
    void call(RpcController controller, ManagedKeyRequest request, RpcCallback<T> done)
      throws Exception;
  }

  @Test
  public void testGenerateKeyStateResponse_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.EMPTY).setKeyNamespace(KEY_NAMESPACE).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, enableKeyManagementDone);

    // Assert
    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).enableKeyManagement(any(), any());
    verify(enableKeyManagementDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testGenerateKeyStateResponse_IOException() throws Exception {
    // Arrange
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenThrow(IOException.class);
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, enableKeyManagementDone);

    // Assert
    verify(controller).setFailed(contains("IOException"));
    verify(keymetaAdmin).enableKeyManagement(any(), any());
    verify(enableKeyManagementDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testGetManagedKeys_IOException() throws Exception {
    doTestGetManagedKeysError(IOException.class);
  }

  @Test
  public void testGetManagedKeys_KeyException() throws Exception {
    doTestGetManagedKeysError(KeyException.class);
  }

  private void doTestGetManagedKeysError(Class<? extends Exception> exType) throws Exception {
    // Arrange
    when(keymetaAdmin.getManagedKeys(any(), any())).thenThrow(exType);
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.getManagedKeys(controller, request, getManagedKeysDone);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).getManagedKeys(any(), any());
    verify(getManagedKeysDone).run(GetManagedKeysResponse.getDefaultInstance());
  }

  @Test
  public void testGetManagedKeys_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    keyMetaAdminService.getManagedKeys(controller, request, getManagedKeysDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).getManagedKeys(any(), any());
    verify(getManagedKeysDone).run(argThat(response -> response.getStateList().isEmpty()));
  }

  @Test
  public void testDisableKeyManagement_Success() throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();
    List<ManagedKeyData> disabledKeys = Arrays.asList(keyData1, keyData2);
    when(keymetaAdmin.disableKeyManagement(any(), any())).thenReturn(disabledKeys);

    // Act
    keyMetaAdminService.disableKeyManagement(controller, request, disableKeyManagementDone);

    // Assert
    verify(disableKeyManagementDone).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testDisableKeyManagement_IOException() throws Exception {
    doTestDisableKeyManagementError(IOException.class);
  }

  @Test
  public void testDisableKeyManagement_KeyException() throws Exception {
    doTestDisableKeyManagementError(KeyException.class);
  }

  private void doTestDisableKeyManagementError(Class<? extends Exception> exType) throws Exception {
    // Arrange
    when(keymetaAdmin.disableKeyManagement(any(), any())).thenThrow(exType);
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.disableKeyManagement(controller, request, disableKeyManagementDone);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).disableKeyManagement(any(), any());
    verify(disableKeyManagementDone).run(GetManagedKeysResponse.getDefaultInstance());
  }

  @Test
  public void testDisableKeyManagement_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    keyMetaAdminService.disableKeyManagement(controller, request, disableKeyManagementDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).disableKeyManagement(any(), any());
    verify(disableKeyManagementDone).run(argThat(response -> response.getStateList().isEmpty()));
  }

  @Test
  public void testDisableKeyManagement_InvalidNamespace() throws Exception {
    // Arrange
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes()))
      .setKeyNamespace("").build();

    keyMetaAdminService.disableKeyManagement(controller, request, disableKeyManagementDone);

    verify(controller).setFailed(contains("key_namespace must not be empty"));
    verify(keymetaAdmin, never()).disableKeyManagement(any(), any());
    verify(disableKeyManagementDone).run(argThat(response -> response.getStateList().isEmpty()));
  }

  @Test
  public void testDisableManagedKey_Success() throws Exception {
    // Arrange
    ManagedKeyEntryRequest request = ManagedKeyEntryRequest.newBuilder()
      .setKeyCustNs(requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build())
      .setKeyMetadataHash(ByteString.copyFrom(keyData1.getKeyMetadataHash())).build();
    when(keymetaAdmin.disableManagedKey(any(), any(), any())).thenReturn(keyData1);

    // Act
    keyMetaAdminService.disableManagedKey(controller, request, disableManagedKeyDone);

    // Assert
    verify(disableManagedKeyDone).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testDisableManagedKey_IOException() throws Exception {
    doTestDisableManagedKeyError(IOException.class);
  }

  @Test
  public void testDisableManagedKey_KeyException() throws Exception {
    doTestDisableManagedKeyError(KeyException.class);
  }

  private void doTestDisableManagedKeyError(Class<? extends Exception> exType) throws Exception {
    // Arrange
    when(keymetaAdmin.disableManagedKey(any(), any(), any())).thenThrow(exType);
    ManagedKeyEntryRequest request = ManagedKeyEntryRequest.newBuilder()
      .setKeyCustNs(requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build())
      .setKeyMetadataHash(ByteString.copyFrom(keyData1.getKeyMetadataHash())).build();

    // Act
    keyMetaAdminService.disableManagedKey(controller, request, disableManagedKeyDone);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).disableManagedKey(any(), any(), any());
    verify(disableManagedKeyDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testDisableManagedKey_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyEntryRequest request = ManagedKeyEntryRequest.newBuilder()
      .setKeyCustNs(
        requestBuilder.setKeyCust(ByteString.EMPTY).setKeyNamespace(KEY_NAMESPACE).build())
      .setKeyMetadataHash(ByteString.copyFrom(keyData1.getKeyMetadataHash())).build();

    keyMetaAdminService.disableManagedKey(controller, request, disableManagedKeyDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).disableManagedKey(any(), any(), any());
    verify(disableManagedKeyDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testDisableManagedKey_InvalidNamespace() throws Exception {
    // Arrange
    ManagedKeyEntryRequest request = ManagedKeyEntryRequest.newBuilder()
      .setKeyCustNs(requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes()))
        .setKeyNamespace("").build())
      .setKeyMetadataHash(ByteString.copyFrom(keyData1.getKeyMetadataHash())).build();

    keyMetaAdminService.disableManagedKey(controller, request, disableManagedKeyDone);

    verify(controller).setFailed(contains("key_namespace must not be empty"));
    verify(keymetaAdmin, never()).disableManagedKey(any(), any(), any());
    verify(disableManagedKeyDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testRotateManagedKey_Success() throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();
    when(keymetaAdmin.rotateManagedKey(any(), any())).thenReturn(keyData1);

    // Act
    keyMetaAdminService.rotateManagedKey(controller, request, rotateManagedKeyDone);

    // Assert
    verify(rotateManagedKeyDone).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testRotateManagedKey_IOException() throws Exception {
    doTestRotateManagedKeyError(IOException.class);
  }

  @Test
  public void testRotateManagedKey_KeyException() throws Exception {
    doTestRotateManagedKeyError(KeyException.class);
  }

  private void doTestRotateManagedKeyError(Class<? extends Exception> exType) throws Exception {
    // Arrange
    when(keymetaAdmin.rotateManagedKey(any(), any())).thenThrow(exType);
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.rotateManagedKey(controller, request, rotateManagedKeyDone);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).rotateManagedKey(any(), any());
    verify(rotateManagedKeyDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testRotateManagedKey_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.EMPTY).setKeyNamespace(KEY_NAMESPACE).build();

    keyMetaAdminService.rotateManagedKey(controller, request, rotateManagedKeyDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).rotateManagedKey(any(), any());
    verify(rotateManagedKeyDone)
      .run(argThat(response -> response.getKeyState() == ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testRefreshManagedKeys_Success() throws Exception {
    // Arrange
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.refreshManagedKeys(controller, request, refreshManagedKeysDone);

    // Assert
    verify(refreshManagedKeysDone).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testRefreshManagedKeys_IOException() throws Exception {
    doTestRefreshManagedKeysError(IOException.class);
  }

  @Test
  public void testRefreshManagedKeys_KeyException() throws Exception {
    doTestRefreshManagedKeysError(KeyException.class);
  }

  private void doTestRefreshManagedKeysError(Class<? extends Exception> exType) throws Exception {
    // Arrange
    Mockito.doThrow(exType).when(keymetaAdmin).refreshManagedKeys(any(), any());
    ManagedKeyRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.refreshManagedKeys(controller, request, refreshManagedKeysDone);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).refreshManagedKeys(any(), any());
    verify(refreshManagedKeysDone).run(EmptyMsg.getDefaultInstance());
  }

  @Test
  public void testRefreshManagedKeys_InvalidCust() throws Exception {
    // Arrange
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    keyMetaAdminService.refreshManagedKeys(controller, request, refreshManagedKeysDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).refreshManagedKeys(any(), any());
    verify(refreshManagedKeysDone).run(EmptyMsg.getDefaultInstance());
  }

  @Test
  public void testRefreshManagedKeys_InvalidNamespace() throws Exception {
    // Arrange
    ManagedKeyRequest request = requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes()))
      .setKeyNamespace("").build();

    // Act
    keyMetaAdminService.refreshManagedKeys(controller, request, refreshManagedKeysDone);

    // Assert
    verify(controller).setFailed(contains("key_namespace must not be empty"));
    verify(keymetaAdmin, never()).refreshManagedKeys(any(), any());
    verify(refreshManagedKeysDone).run(EmptyMsg.getDefaultInstance());
  }
}
