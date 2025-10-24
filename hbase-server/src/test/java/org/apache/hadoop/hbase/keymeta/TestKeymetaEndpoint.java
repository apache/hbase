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
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

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
  private RpcCallback<ManagedKeysResponse> enableKeyManagementDone;
  @Mock
  private RpcCallback<GetManagedKeysResponse> getManagedKeysDone;

  KeymetaServiceEndpoint keymetaServiceEndpoint;
  private ManagedKeysResponse.Builder responseBuilder;
  private ManagedKeysRequest.Builder requestBuilder;
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
    responseBuilder =
      ManagedKeysResponse.newBuilder().setKeyState(ManagedKeysProtos.ManagedKeyState.KEY_ACTIVE);
    requestBuilder =
      ManagedKeysRequest.newBuilder().setKeyNamespace(ManagedKeyData.KEY_SPACE_GLOBAL);
    keyData1 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key1".getBytes(), "AES"), ACTIVE, KEY_METADATA1);
    keyData2 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key2".getBytes(), "AES"), ACTIVE, KEY_METADATA2);
    when(master.getKeymetaAdmin()).thenReturn(keymetaAdmin);
  }

  @Test
  public void testCreateResponseBuilderValid() throws IOException {
    byte[] cust = "testKey".getBytes();
    ManagedKeysRequest request = requestBuilder.setKeyCust(ByteString.copyFrom(cust)).build();

    ManagedKeysResponse.Builder result = ManagedKeysResponse.newBuilder();
    KeymetaServiceEndpoint.initManagedKeysResponseBuilder(controller, request, result);

    assertNotNull(result);
    assertArrayEquals(cust, result.getKeyCust().toByteArray());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testCreateResponseBuilderEmptyCust() throws IOException {
    ManagedKeysRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    IOException exception = assertThrows(IOException.class, () -> KeymetaServiceEndpoint
      .initManagedKeysResponseBuilder(controller, request, ManagedKeysResponse.newBuilder()));

    assertEquals("key_cust must not be empty", exception.getMessage());
  }

  @Test
  public void testGenerateKeyStateResponse() throws Exception {
    // Arrange
    ManagedKeysResponse response =
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
    assertEquals(ManagedKeysProtos.ManagedKeyState.KEY_ACTIVE,
      result.getStateList().get(0).getKeyState());
    assertEquals(0, Bytes.compareTo(keyData1.getKeyCustodian(),
      result.getStateList().get(0).getKeyCust().toByteArray()));
    assertEquals(keyData1.getKeyNamespace(), result.getStateList().get(0).getKeyNamespace());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGenerateKeyStateResponse_Empty() throws Exception {
    // Arrange
    ManagedKeysResponse response =
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
    ManagedKeysRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenReturn(keyData1);

    // Act
    svc.call(controller, request, done);

    // Assert
    verify(done).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  private interface ServiceCall<T> {
    void call(RpcController controller, ManagedKeysRequest request, RpcCallback<T> done)
      throws Exception;
  }

  @Test
  public void testGenerateKeyStateResponse_InvalidCust() throws Exception {
    // Arrange
    ManagedKeysRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, enableKeyManagementDone);

    // Assert
    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).enableKeyManagement(any(), any());
    verify(enableKeyManagementDone).run(
      argThat(response -> response.getKeyState() == ManagedKeysProtos.ManagedKeyState.KEY_FAILED));
  }

  @Test
  public void testGenerateKeyStateResponse_IOException() throws Exception {
    // Arrange
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenThrow(IOException.class);
    ManagedKeysRequest request =
      requestBuilder.setKeyCust(ByteString.copyFrom(KEY_CUST.getBytes())).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, enableKeyManagementDone);

    // Assert
    verify(controller).setFailed(contains("IOException"));
    verify(keymetaAdmin).enableKeyManagement(any(), any());
    verify(enableKeyManagementDone).run(
      argThat(response -> response.getKeyState() == ManagedKeysProtos.ManagedKeyState.KEY_FAILED));
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
    ManagedKeysRequest request =
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
    ManagedKeysRequest request = requestBuilder.setKeyCust(ByteString.EMPTY).build();

    keyMetaAdminService.getManagedKeys(controller, request, getManagedKeysDone);

    verify(controller).setFailed(contains("key_cust must not be empty"));
    verify(keymetaAdmin, never()).getManagedKeys(any(), any());
    verify(getManagedKeysDone).run(argThat(response -> response.getStateList().isEmpty()));
  }
}
