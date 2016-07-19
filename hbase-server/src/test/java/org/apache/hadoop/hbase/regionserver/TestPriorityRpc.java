/*
 *
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Get;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.ByteString;

/**
 * Tests that verify certain RPCs get a higher QoS.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestPriorityRpc {
  private HRegionServer regionServer = null;
  private PriorityFunction priority = null;

  @Before
  public void setup() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.testing.nocluster", true); // No need to do ZK
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.getDataTestDir(this.getClass().getName());
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);
    regionServer = HRegionServer.constructRegionServer(HRegionServer.class, conf, cp);
    priority = regionServer.rpcServices.getPriority();
  }

  @Test
  public void testQosFunctionForMeta() throws IOException {
    priority = regionServer.rpcServices.getPriority();
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    //create a rpc request that has references to hbase:meta region and also
    //uses one of the known argument classes (known argument classes are
    //listed in HRegionServer.QosFunctionImpl.knownArgumentClasses)
    headerBuilder.setMethodName("foo");

    GetRequest.Builder getRequestBuilder = GetRequest.newBuilder();
    RegionSpecifier.Builder regionSpecifierBuilder = RegionSpecifier.newBuilder();
    regionSpecifierBuilder.setType(RegionSpecifierType.REGION_NAME);
    ByteString name = ByteStringer.wrap(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    regionSpecifierBuilder.setValue(name);
    RegionSpecifier regionSpecifier = regionSpecifierBuilder.build();
    getRequestBuilder.setRegion(regionSpecifier);
    Get.Builder getBuilder = Get.newBuilder();
    getBuilder.setRow(ByteStringer.wrap("somerow".getBytes()));
    getRequestBuilder.setGet(getBuilder.build());
    GetRequest getRequest = getRequestBuilder.build();
    RequestHeader header = headerBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    RSRpcServices mockRpc = Mockito.mock(RSRpcServices.class);
    Mockito.when(mockRS.getRSRpcServices()).thenReturn(mockRpc);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    Mockito.when(mockRpc.getRegion((RegionSpecifier) Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isSystemTable()).thenReturn(true);
    // Presume type.
    ((AnnotationReadingPriorityFunction)priority).setRegionServer(mockRS);
    assertEquals(HConstants.SYSTEMTABLE_QOS, priority.getPriority(header, getRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));
  }

  @Test
  public void testQosFunctionWithoutKnownArgument() throws IOException {
    //The request is not using any of the
    //known argument classes (it uses one random request class)
    //(known argument classes are listed in
    //HRegionServer.QosFunctionImpl.knownArgumentClasses)
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    headerBuilder.setMethodName("foo");
    RequestHeader header = headerBuilder.build();
    PriorityFunction qosFunc = regionServer.rpcServices.getPriority();
    assertEquals(HConstants.NORMAL_QOS, qosFunc.getPriority(header, null,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));
  }

  @Test
  public void testQosFunctionForScanMethod() throws IOException {
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    headerBuilder.setMethodName("Scan");
    RequestHeader header = headerBuilder.build();

    //build an empty scan request
    ScanRequest.Builder scanBuilder = ScanRequest.newBuilder();
    ScanRequest scanRequest = scanBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    RSRpcServices mockRpc = Mockito.mock(RSRpcServices.class);
    Mockito.when(mockRS.getRSRpcServices()).thenReturn(mockRpc);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    Mockito.when(mockRpc.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isSystemTable()).thenReturn(false);
    // Presume type.
    ((AnnotationReadingPriorityFunction)priority).setRegionServer(mockRS);
    int qos = priority.getPriority(header, scanRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"}));
    assertTrue ("" + qos, qos == HConstants.NORMAL_QOS);

    //build a scan request with scannerID
    scanBuilder = ScanRequest.newBuilder();
    scanBuilder.setScannerId(12345);
    scanRequest = scanBuilder.build();
    //mock out a high priority type handling and see the QoS returned
    RegionScanner mockRegionScanner = Mockito.mock(RegionScanner.class);
    Mockito.when(mockRpc.getScanner(12345)).thenReturn(mockRegionScanner);
    Mockito.when(mockRegionScanner.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRpc.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isSystemTable()).thenReturn(true);

    // Presume type.
    ((AnnotationReadingPriorityFunction)priority).setRegionServer(mockRS);

    assertEquals(HConstants.SYSTEMTABLE_QOS, priority.getPriority(header, scanRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));

    //the same as above but with non-meta region
    Mockito.when(mockRegionInfo.isSystemTable()).thenReturn(false);
    assertEquals(HConstants.NORMAL_QOS, priority.getPriority(header, scanRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));
  }
}
