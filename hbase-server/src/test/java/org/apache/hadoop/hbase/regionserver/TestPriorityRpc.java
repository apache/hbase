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
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Get;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
/**
 * Tests that verify certain RPCs get a higher QoS.
 */
@Category(MediumTests.class)
public class TestPriorityRpc {
  private HRegionServer regionServer = null;
  private QosFunction qosFunction = null;

  @Before
  public void setup() {
    Configuration conf = HBaseConfiguration.create();
    regionServer = HRegionServer.constructRegionServer(HRegionServer.class, conf);
    qosFunction = regionServer.getQosFunction();
  }

  @Test
  public void testQosFunctionForMeta() throws IOException {
    qosFunction = regionServer.getQosFunction();
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    //create a rpc request that has references to hbase:meta region and also
    //uses one of the known argument classes (known argument classes are
    //listed in HRegionServer.QosFunction.knownArgumentClasses)
    headerBuilder.setMethodName("foo");

    GetRequest.Builder getRequestBuilder = GetRequest.newBuilder();
    RegionSpecifier.Builder regionSpecifierBuilder = RegionSpecifier.newBuilder();
    regionSpecifierBuilder.setType(RegionSpecifierType.REGION_NAME);
    ByteString name = ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    regionSpecifierBuilder.setValue(name);
    RegionSpecifier regionSpecifier = regionSpecifierBuilder.build();
    getRequestBuilder.setRegion(regionSpecifier);
    Get.Builder getBuilder = Get.newBuilder();
    getBuilder.setRow(ByteString.copyFrom("somerow".getBytes()));
    getRequestBuilder.setGet(getBuilder.build());
    GetRequest getRequest = getRequestBuilder.build();
    RequestHeader header = headerBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    Mockito.when(mockRS.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isMetaTable()).thenReturn(true);
    qosFunction.setRegionServer(mockRS);
    assertTrue (qosFunction.apply(new Pair<RequestHeader, Message>(header, getRequest)) ==
      HConstants.HIGH_QOS);
  }

  @Test
  public void testQosFunctionWithoutKnownArgument() throws IOException {
    //The request is not using any of the
    //known argument classes (it uses one random request class)
    //(known argument classes are listed in
    //HRegionServer.QosFunction.knownArgumentClasses)
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    headerBuilder.setMethodName("foo");
    RequestHeader header = headerBuilder.build();
    QosFunction qosFunc = regionServer.getQosFunction();
    assertTrue (qosFunc.apply(new Pair<RequestHeader, Message>(header, null)) ==
      HConstants.NORMAL_QOS);
  }

  @Test
  public void testQosFunctionForScanMethod() throws IOException {
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    headerBuilder.setMethodName("scan");
    RequestHeader header = headerBuilder.build();

    //build an empty scan request
    ScanRequest.Builder scanBuilder = ScanRequest.newBuilder();
    ScanRequest scanRequest = scanBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
    Mockito.when(mockRS.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(false);
    qosFunction.setRegionServer(mockRS);
    int qos = qosFunction.apply(new Pair<RequestHeader, Message>(header, scanRequest));
    assertTrue ("" + qos, qos == HConstants.NORMAL_QOS);

    //build a scan request with scannerID
    scanBuilder = ScanRequest.newBuilder();
    scanBuilder.setScannerId(12345);
    scanRequest = scanBuilder.build();
    //mock out a high priority type handling and see the QoS returned
    RegionScanner mockRegionScanner = Mockito.mock(RegionScanner.class);
    Mockito.when(mockRS.getScanner(12345)).thenReturn(mockRegionScanner);
    Mockito.when(mockRegionScanner.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRS.getRegion((RegionSpecifier)Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(true);

    qosFunction.setRegionServer(mockRS);

    assertTrue (qosFunction.apply(new Pair<RequestHeader, Message>(header, scanRequest)) ==
      HConstants.HIGH_QOS);

    //the same as above but with non-meta region
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(false);
    assertTrue (qosFunction.apply(new Pair<RequestHeader, Message>(header, scanRequest)) ==
      HConstants.NORMAL_QOS);
  }
}
