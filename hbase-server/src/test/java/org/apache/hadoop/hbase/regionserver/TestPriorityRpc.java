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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Get;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import org.apache.hadoop.hbase.regionserver.HRegionServer.QosFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.ByteString;
/**
 * Tests that verify certain RPCs get a higher QoS.
 */
@Category(MediumTests.class)
public class TestPriorityRpc {
  static HRegionServer regionServer = null;
  static QosFunction qosFunction = null;
  @BeforeClass
  public static void onetimeSetup() {
    Configuration conf = HBaseConfiguration.create();
    regionServer =
        HRegionServer.constructRegionServer(HRegionServer.class, conf);
    qosFunction = regionServer.getQosFunction();
  }
  @Test
  public void testQosFunctionForMeta() throws IOException {
    qosFunction = regionServer.getQosFunction();
    RpcRequestBody.Builder rpcRequestBuilder = RpcRequestBody.newBuilder();
    //create a rpc request that has references to META region and also
    //uses one of the known argument classes (known argument classes are
    //listed in HRegionServer.QosFunction.knownArgumentClasses)
    rpcRequestBuilder = RpcRequestBody.newBuilder();
    rpcRequestBuilder.setMethodName("foo");

    GetRequest.Builder getRequestBuilder = GetRequest.newBuilder();
    RegionSpecifier.Builder regionSpecifierBuilder = RegionSpecifier.newBuilder();
    regionSpecifierBuilder.setType(RegionSpecifierType.REGION_NAME);
    ByteString name =
        ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    regionSpecifierBuilder.setValue(name);
    RegionSpecifier regionSpecifier = regionSpecifierBuilder.build();
    getRequestBuilder.setRegion(regionSpecifier);
    Get.Builder getBuilder = Get.newBuilder();
    getBuilder.setRow(ByteString.copyFrom("somerow".getBytes()));
    getRequestBuilder.setGet(getBuilder.build());
    rpcRequestBuilder.setRequest(getRequestBuilder.build().toByteString());
    rpcRequestBuilder.setRequestClassName(GetRequest.class.getCanonicalName());
    RpcRequestBody rpcRequest = rpcRequestBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    Mockito.when(mockRS.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(true);
    qosFunction.setRegionServer(mockRS);
    assertTrue (qosFunction.apply(rpcRequest) == HConstants.HIGH_QOS);
  }

  @Test
  public void testQosFunctionWithoutKnownArgument() throws IOException {
    //The request is not using any of the
    //known argument classes (it uses one random request class)
    //(known argument classes are listed in
    //HRegionServer.QosFunction.knownArgumentClasses)
    RpcRequestBody.Builder rpcRequestBuilder = RpcRequestBody.newBuilder();
    rpcRequestBuilder.setMethodName("foo");
    rpcRequestBuilder.setRequestClassName(GetOnlineRegionRequest.class.getCanonicalName());
    RpcRequestBody rpcRequest = rpcRequestBuilder.build();
    QosFunction qosFunc = regionServer.getQosFunction();
    assertTrue (qosFunc.apply(rpcRequest) == HConstants.NORMAL_QOS);
  }

  @Test
  public void testQosFunctionForScanMethod() throws IOException {
    RpcRequestBody.Builder rpcRequestBuilder = RpcRequestBody.newBuilder();
    rpcRequestBuilder.setMethodName("scan");

    //build an empty scan request
    ScanRequest.Builder scanBuilder = ScanRequest.newBuilder();
    ByteString requestBody = scanBuilder.build().toByteString();
    rpcRequestBuilder.setRequest(requestBody);
    RpcRequestBody rpcRequest = rpcRequestBuilder.build();
    assertTrue (qosFunction.apply(rpcRequest) == HConstants.NORMAL_QOS);

    //build a scan request with scannerID
    scanBuilder = ScanRequest.newBuilder();
    scanBuilder.setScannerId(12345);
    requestBody = scanBuilder.build().toByteString();
    rpcRequestBuilder.setRequest(requestBody);
    rpcRequestBuilder.setRequestClassName(ScanRequest.class.getCanonicalName());
    rpcRequest = rpcRequestBuilder.build();
    //mock out a high priority type handling and see the QoS returned
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    RegionScanner mockRegionScanner = Mockito.mock(RegionScanner.class);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    HRegion mockRegion = Mockito.mock(HRegion.class);
    Mockito.when(mockRS.getScanner(12345)).thenReturn(mockRegionScanner);
    Mockito.when(mockRegionScanner.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRS.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(true);

    qosFunction.setRegionServer(mockRS);

    assertTrue (qosFunction.apply(rpcRequest) == HConstants.HIGH_QOS);

    //the same as above but with non-meta region
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(false);
    assertTrue (qosFunction.apply(rpcRequest) == HConstants.NORMAL_QOS);
  }

}
