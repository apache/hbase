/**
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.JMXListener;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.HasPermissionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.HasPermissionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.SetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests that the MasterRpcServices is correctly searching for implementations of the
 * Coprocessor Service and not just the "default" implementations of those services.
 */
@Category({SmallTests.class})
public class TestMasterCoprocessorServices {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterCoprocessorServices.class);

  private static class MockAccessController implements AccessControlService.Interface,
      MasterCoprocessor, RegionCoprocessor, MasterObserver, RegionObserver {

    @Override
    public void grant(RpcController controller, GrantRequest request,
        RpcCallback<GrantResponse> done) {}

    @Override
    public void revoke(RpcController controller, RevokeRequest request,
        RpcCallback<RevokeResponse> done) {}

    @Override
    public void getUserPermissions(RpcController controller, GetUserPermissionsRequest request,
        RpcCallback<GetUserPermissionsResponse> done) {}

    @Override
    public void checkPermissions(RpcController controller, CheckPermissionsRequest request,
        RpcCallback<CheckPermissionsResponse> done) {}

    @Override
    public void hasPermission(RpcController controller, HasPermissionRequest request,
        RpcCallback<HasPermissionResponse> done) {
    }
  }

  private static class MockVisibilityController implements VisibilityLabelsService.Interface,
      MasterCoprocessor, RegionCoprocessor, MasterObserver, RegionObserver {

    @Override
    public void addLabels(RpcController controller, VisibilityLabelsRequest request,
        RpcCallback<VisibilityLabelsResponse> done) {
    }

    @Override
    public void setAuths(RpcController controller, SetAuthsRequest request,
        RpcCallback<VisibilityLabelsResponse> done) {
    }

    @Override
    public void clearAuths(RpcController controller, SetAuthsRequest request,
        RpcCallback<VisibilityLabelsResponse> done) {
    }

    @Override
    public void getAuths(RpcController controller, GetAuthsRequest request,
        RpcCallback<GetAuthsResponse> done) {
    }

    @Override
    public void listLabels(RpcController controller, ListLabelsRequest request,
        RpcCallback<ListLabelsResponse> done) {
    }
  }

  private MasterRpcServices masterServices;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    masterServices = mock(MasterRpcServices.class);
    when(masterServices.hasAccessControlServiceCoprocessor(
        any(MasterCoprocessorHost.class))).thenCallRealMethod();
    when(masterServices.hasVisibilityLabelsServiceCoprocessor(
        any(MasterCoprocessorHost.class))).thenCallRealMethod();
    when(masterServices.checkCoprocessorWithService(
        any(List.class), any(Class.class))).thenCallRealMethod();
  }

  @Test
  public void testAccessControlServices() {
    MasterCoprocessor defaultImpl = new AccessController();
    MasterCoprocessor customImpl = new MockAccessController();
    MasterCoprocessor unrelatedImpl = new JMXListener();
    assertTrue(masterServices.checkCoprocessorWithService(
        Collections.singletonList(defaultImpl), AccessControlService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Collections.singletonList(customImpl), AccessControlService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        Collections.emptyList(), AccessControlService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        null, AccessControlService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        Collections.singletonList(unrelatedImpl), AccessControlService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Arrays.asList(unrelatedImpl, customImpl), AccessControlService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Arrays.asList(unrelatedImpl, defaultImpl), AccessControlService.Interface.class));
  }

  @Test
  public void testVisibilityLabelServices() {
    MasterCoprocessor defaultImpl = new VisibilityController();
    MasterCoprocessor customImpl = new MockVisibilityController();
    MasterCoprocessor unrelatedImpl = new JMXListener();
    assertTrue(masterServices.checkCoprocessorWithService(
        Collections.singletonList(defaultImpl), VisibilityLabelsService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Collections.singletonList(customImpl), VisibilityLabelsService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        Collections.emptyList(), VisibilityLabelsService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        null, VisibilityLabelsService.Interface.class));
    assertFalse(masterServices.checkCoprocessorWithService(
        Collections.singletonList(unrelatedImpl), VisibilityLabelsService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Arrays.asList(unrelatedImpl, customImpl), VisibilityLabelsService.Interface.class));
    assertTrue(masterServices.checkCoprocessorWithService(
        Arrays.asList(unrelatedImpl, defaultImpl), VisibilityLabelsService.Interface.class));
  }
}
