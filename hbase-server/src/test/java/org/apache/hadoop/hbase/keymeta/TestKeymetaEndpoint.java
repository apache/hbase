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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.keymeta.KeymetaServiceEndpoint.KeymetaAdminServiceImpl;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeyState.KEY_ACTIVE;
import static org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeyState.KEY_FAILED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
  private RpcCallback done;
  @Mock
  private KeymetaAdmin keymetaAdmin;

  KeymetaServiceEndpoint keymetaServiceEndpoint;
  private ManagedKeysResponse.Builder responseBuilder;
  private ManagedKeysRequest.Builder requestBuilder;
  private KeymetaAdminServiceImpl keyMetaAdminService;
  private ManagedKeyData keyData1;
  private ManagedKeyData keyData2;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    keymetaServiceEndpoint = new KeymetaServiceEndpoint();
    CoprocessorEnvironment env = mock(CoprocessorEnvironment.class,
      withSettings().extraInterfaces(HasMasterServices.class));
    when(((HasMasterServices) env).getMasterServices()).thenReturn(master);
    keymetaServiceEndpoint.start(env);
    keyMetaAdminService = (KeymetaAdminServiceImpl) keymetaServiceEndpoint.getServices()
      .iterator().next();
    responseBuilder = ManagedKeysResponse.newBuilder().setKeyState(KEY_ACTIVE);
    requestBuilder = ManagedKeysRequest.newBuilder()
      .setKeyNamespace(ManagedKeyData.KEY_SPACE_GLOBAL);
    keyData1 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key1".getBytes(), "AES"), ACTIVE, KEY_METADATA1);
    keyData2 = new ManagedKeyData(KEY_CUST.getBytes(), KEY_NAMESPACE,
      new SecretKeySpec("key2".getBytes(), "AES"), ACTIVE, KEY_METADATA2);
    when(master.getKeymetaAdmin()).thenReturn(keymetaAdmin);
  }

  @Test
  public void testConvertToKeyCustBytesValid() {
    // Arrange
    String validBase64 = Base64.getEncoder().encodeToString("testKey".getBytes());
    ManagedKeysRequest request = requestBuilder.setKeyCust(validBase64).build();

    // Act
    byte[] result =
      KeymetaServiceEndpoint.convertToKeyCustBytes(controller, request, responseBuilder);

    // Assert
    assertNotNull(result);
    assertArrayEquals("testKey".getBytes(), result);
    assertEquals(KEY_ACTIVE, responseBuilder.getKeyState());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testConvertToKeyCustBytesInvalid() {
    // Arrange
    String invalidBase64 = "invalid!Base64@String";
    ManagedKeysRequest request = requestBuilder.setKeyCust(invalidBase64).build();

    // Act
    byte[] result = KeymetaServiceEndpoint.convertToKeyCustBytes(controller, request,
      responseBuilder);

    // Assert
    assertNull(result);
    assertEquals(KEY_FAILED, responseBuilder.getKeyState());
    verify(controller).setFailed(anyString());
  }

  @Test
  public void testGetResponseBuilder() {
    // Arrange
    String keyCust = Base64.getEncoder().encodeToString("testKey".getBytes());
    String keyNamespace = "testNamespace";
    ManagedKeysRequest request = requestBuilder.setKeyCust(keyCust)
      .setKeyNamespace(keyNamespace)
      .build();

    // Act
    ManagedKeysResponse.Builder result = KeymetaServiceEndpoint.getResponseBuilder(controller,
      request);

    // Assert
    assertNotNull(result);
    assertEquals(keyNamespace, result.getKeyNamespace());
    assertArrayEquals("testKey".getBytes(), result.getKeyCustBytes().toByteArray());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGetResponseBuilderWithInvalidBase64() {
    // Arrange
    String keyCust = "invalidBase64!";
    String keyNamespace = "testNamespace";
    ManagedKeysRequest request = requestBuilder.setKeyCust(keyCust)
      .setKeyNamespace(keyNamespace)
      .build();

    // Act
    ManagedKeysResponse.Builder result = KeymetaServiceEndpoint.getResponseBuilder(controller,
        request);

    // Assert
    assertNotNull(result);
    assertEquals(keyNamespace, result.getKeyNamespace());
    assertEquals(KEY_FAILED, result.getKeyState());
    verify(controller).setFailed(contains("Failed to decode specified prefix as Base64 string"));
  }

  @Test
  public void testGenerateKeyStateResponse() throws Exception {
    // Arrange
    ManagedKeysResponse response = responseBuilder.setKeyCustBytes(ByteString.copyFrom(
        keyData1.getKeyCustodian()))
      .setKeyNamespace(keyData1.getKeyNamespace())
      .build();
    List<ManagedKeyData> managedKeyStates = Arrays.asList(keyData1, keyData2);

    // Act
    GetManagedKeysResponse result = KeymetaServiceEndpoint.generateKeyStateResponse(
      managedKeyStates, responseBuilder);

    // Assert
    assertNotNull(response);
    assertNotNull(result.getStateList());
    assertEquals(2, result.getStateList().size());
    assertEquals(KEY_ACTIVE, result.getStateList().get(0).getKeyState());
    assertEquals(0, Bytes.compareTo(keyData1.getKeyCustodian(),
      result.getStateList().get(0).getKeyCustBytes().toByteArray()));
    assertEquals(keyData1.getKeyNamespace(), result.getStateList().get(0).getKeyNamespace());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGenerateKeyStateResponse_Empty() throws Exception {
    // Arrange
    ManagedKeysResponse response = responseBuilder.setKeyCustBytes(ByteString.copyFrom(
        keyData1.getKeyCustodian()))
      .setKeyNamespace(keyData1.getKeyNamespace())
      .build();
    List<ManagedKeyData> managedKeyStates = new ArrayList<>();

    // Act
    GetManagedKeysResponse result = KeymetaServiceEndpoint.generateKeyStateResponse(
      managedKeyStates, responseBuilder);

    // Assert
    assertNotNull(response);
    assertNotNull(result.getStateList());
    assertEquals(0, result.getStateList().size());
    verify(controller, never()).setFailed(anyString());
  }

  @Test
  public void testGenerateKeyStatResponse_Success() throws Exception {
    doTestServiceCallForSuccess(
      (controller, request, done) ->
        keyMetaAdminService.enableKeyManagement(controller, request, done));
  }

  @Test
  public void testGetManagedKeys_Success() throws Exception {
    doTestServiceCallForSuccess(
      (controller, request, done) ->
        keyMetaAdminService.getManagedKeys(controller, request, done));
  }

  private void doTestServiceCallForSuccess(ServiceCall svc) throws Exception {
    // Arrange
    ManagedKeysRequest request = requestBuilder.setKeyCust(KEY_CUST).build();
    List<ManagedKeyData> managedKeyStates = Arrays.asList(keyData1);
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenReturn(managedKeyStates);

    // Act
    svc.call(controller, request, done);

    // Assert
    verify(done).run(any());
    verify(controller, never()).setFailed(anyString());
  }

  private interface ServiceCall {
    void call(RpcController controller, ManagedKeysRequest request,
      RpcCallback<GetManagedKeysResponse> done) throws Exception;
  }

  @Test
  public void testGenerateKeyStateResponse_InvalidCust() throws Exception {
    // Arrange
    String invalidBase64 = "invalid!Base64@String";
    ManagedKeysRequest request = requestBuilder.setKeyCust(invalidBase64).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, done);

    // Assert
    verify(controller).setFailed(contains("IOException"));
    verify(keymetaAdmin, never()).enableKeyManagement(any(), any());
    verify(done, never()).run(any());
  }

  @Test
  public void testGenerateKeyStateResponse_IOException() throws Exception {
    // Arrange
    when(keymetaAdmin.enableKeyManagement(any(), any())).thenThrow(IOException.class);
    ManagedKeysRequest request = requestBuilder.setKeyCust(KEY_CUST).build();

    // Act
    keyMetaAdminService.enableKeyManagement(controller, request, done);

    // Assert
    verify(controller).setFailed(contains("IOException"));
    verify(keymetaAdmin).enableKeyManagement(any(), any());
    verify(done, never()).run(any());
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
    ManagedKeysRequest request = requestBuilder.setKeyCust(KEY_CUST).build();

    // Act
    keyMetaAdminService.getManagedKeys(controller, request, done);

    // Assert
    verify(controller).setFailed(contains(exType.getSimpleName()));
    verify(keymetaAdmin).getManagedKeys(any(), any());
    verify(done, never()).run(any());
  }

  @Test
  public void testGetManagedKeys_InvalidCust() throws Exception {
    // Arrange
    String invalidBase64 = "invalid!Base64@String";
    ManagedKeysRequest request = requestBuilder.setKeyCust(invalidBase64).build();

    // Act
    keyMetaAdminService.getManagedKeys(controller, request, done);

    // Assert
    verify(controller).setFailed(contains("IOException"));
    verify(keymetaAdmin, never()).getManagedKeys(any(), any());
    verify(done, never()).run(any());
  }
}
