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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.AnnotationReadingPriorityFunction.ScanDeadlineOnly;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Get;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Tests that verify certain RPCs get a higher QoS.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestPriorityRpc {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPriorityRpc.class);

  private HRegionServer regionServer = null;
  private PriorityFunction priority = null;

  @Before
  public void setup() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.testing.nocluster", true); // No need to do ZK
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.getDataTestDir(this.getClass().getName());
    regionServer = HRegionServer.constructRegionServer(HRegionServer.class, conf);
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
    ByteString name = UnsafeByteOperations.unsafeWrap(
        RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName());
    regionSpecifierBuilder.setValue(name);
    RegionSpecifier regionSpecifier = regionSpecifierBuilder.build();
    getRequestBuilder.setRegion(regionSpecifier);
    Get.Builder getBuilder = Get.newBuilder();
    getBuilder.setRow(UnsafeByteOperations.unsafeWrap(Bytes.toBytes("somerow")));
    getRequestBuilder.setGet(getBuilder.build());
    GetRequest getRequest = getRequestBuilder.build();
    RequestHeader header = headerBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    RSRpcServices mockRpc = Mockito.mock(RSRpcServices.class);
    Mockito.when(mockRS.getRSRpcServices()).thenReturn(mockRpc);
    RegionInfo mockRegionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(mockRpc.getRegion(Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.getTable())
        .thenReturn(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
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
    RequestHeader header = createRequestHeader("foo");
    PriorityFunction qosFunc = regionServer.rpcServices.getPriority();
    assertEquals(HConstants.NORMAL_QOS, qosFunc.getPriority(header, null,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));
  }

  @Test
  public void testQosFunctionForScanMethod() throws IOException {
    RequestHeader header = createRequestHeader("Scan");

    //build an empty scan request
    ScanRequest.Builder scanBuilder = ScanRequest.newBuilder();
    ScanRequest scanRequest = scanBuilder.build();
    HRegion mockRegion = Mockito.mock(HRegion.class);
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    RSRpcServices mockRpc = Mockito.mock(RSRpcServices.class);
    Mockito.when(mockRS.getRSRpcServices()).thenReturn(mockRpc);
    RegionInfo mockRegionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(mockRpc.getRegion(Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    // make isSystemTable return false
    Mockito.when(mockRegionInfo.getTable()).thenReturn(TableName.valueOf("testQosFunctionForScanMethod"));
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
    Mockito.when(mockRegionInfo.getTable()).thenReturn(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());

    // Presume type.
    ((AnnotationReadingPriorityFunction)priority).setRegionServer(mockRS);

    assertEquals(HConstants.SYSTEMTABLE_QOS, priority.getPriority(header, scanRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));

    //the same as above but with non-meta region
    // make isSystemTable return false
    Mockito.when(mockRegionInfo.getTable()).thenReturn(TableName.valueOf("testQosFunctionForScanMethod"));
    assertEquals(HConstants.NORMAL_QOS, priority.getPriority(header, scanRequest,
      User.createUserForTesting(regionServer.conf, "someuser", new String[]{"somegroup"})));
  }

  private static HRegionServer prepareDeadlineTest(
    TableName tableName, boolean isMeta, long newDelay) throws Exception {
    HRegionServer mockRS = Mockito.mock(HRegionServer.class);
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.testing.nocluster", true);
    HRegion mockRegion = Mockito.mock(HRegion.class);
    RSRpcServices mockRpc = Mockito.mock(RSRpcServices.class);
    Mockito.when(mockRS.getRSRpcServices()).thenReturn(mockRpc);
    RegionInfo mockRegionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(mockRpc.getRegion(Mockito.any())).thenReturn(mockRegion);
    Mockito.when(mockRpc.isMaster()).thenReturn(false);
    Mockito.when(mockRpc.getConfiguration()).thenReturn(conf);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    Mockito.when(mockRegionInfo.getTable()).thenReturn(tableName);
    Mockito.when(mockRegionInfo.isMetaRegion()).thenReturn(isMeta);
    TableDescriptor td = Mockito.mock(TableDescriptor.class);
    Mockito.when(td.isMetaRegion()).thenReturn(isMeta);
    Mockito.when(mockRegion.getTableDescriptor()).thenReturn(td);
    Mockito.when(mockRpc.getScannerExpirationDelayMs(null)).thenReturn(newDelay);
    return mockRS;
  }

  private void addMockScanner(
      RSRpcServices mockRpc, long id, long delayMs, long vtime) throws Exception {
    RegionScanner mockRegionScanner = Mockito.mock(RegionScanner.class);
    Mockito.when(mockRpc.getScanner(id)).thenReturn(mockRegionScanner);
    HRegion mockRegion = mockRpc.getRegion(null);
    RegionInfo mockRi = mockRegion.getRegionInfo();
    Mockito.when(mockRegionScanner.getRegionInfo()).thenReturn(mockRi);
    Mockito.when(mockRpc.getScannerExpirationDelayMs(id)).thenReturn(delayMs);
    Mockito.when(mockRpc.getScannerVirtualTime(id)).thenReturn(vtime);
  }

  private AnnotationReadingPriorityFunction prepareDeadlineTestFn(
    AnnotationReadingPriorityFunction.ScanDeadlineOnly cfg, RSRpcServices rpc) {
    rpc.getConfiguration().set(
      AnnotationReadingPriorityFunction.SCAN_DEADLINE_PRIORITY, cfg.name());
    return new AnnotationReadingPriorityFunction(rpc);
  }

  private static RequestHeader createRequestHeader(String type) {
    RequestHeader.Builder headerBuilder = RequestHeader.newBuilder();
    headerBuilder.setMethodName(type);
    return headerBuilder.build();
  }

  @Test
  public void testScanDeadline() throws Exception {
    RequestHeader header = createRequestHeader("Scan");

    ScanRequest.Builder scanBuilder = ScanRequest.newBuilder();
    RegionSpecifier.Builder regionSpecifierBuilder = RegionSpecifier.newBuilder();
    // Hack-ish - ERN will be passed directly to the mock and ignored, so we can supply no value.
    regionSpecifierBuilder.setType(RegionSpecifierType.ENCODED_REGION_NAME);
    regionSpecifierBuilder.setValue(ByteString.EMPTY);
    scanBuilder.setRegion(regionSpecifierBuilder.build());
    ScanRequest newReqNoMeta = scanBuilder.build();

    scanBuilder = ScanRequest.newBuilder();
    regionSpecifierBuilder = RegionSpecifier.newBuilder();
    regionSpecifierBuilder.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifierBuilder.setValue(UnsafeByteOperations.unsafeWrap(
      RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName()));
    scanBuilder.setRegion(regionSpecifierBuilder.build());
    ScanRequest newReqMeta = scanBuilder.build();

    final long SCANNER_ID = 1;
    scanBuilder = ScanRequest.newBuilder();
    scanBuilder.setScannerId(SCANNER_ID);
    ScanRequest oldReq = scanBuilder.build(); // Meta-ness is derived from RSRpcServices

    final long VTIMESQRT = 10, ACTUAL_DELAY = 123, DEFAULT_DELAY = 456;

    HRegionServer mockRS = prepareDeadlineTest(TableName.valueOf("a"), false, DEFAULT_DELAY);
    addMockScanner(mockRS.getRSRpcServices(), SCANNER_ID, ACTUAL_DELAY, VTIMESQRT * VTIMESQRT);

    // Test new reqs and non-meta scan here, as well as old non-meta scanner.
    AnnotationReadingPriorityFunction fn = prepareDeadlineTestFn(
      ScanDeadlineOnly.NONE, mockRS.getRSRpcServices());
    assertEquals(0L, fn.getDeadline(header, newReqNoMeta));
    assertEquals(0L, fn.getDeadline(header, newReqMeta));
    assertEquals(VTIMESQRT, fn.getDeadline(header, oldReq));
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(DEFAULT_DELAY, fn.getDeadline(header, newReqNoMeta));
    assertEquals(DEFAULT_DELAY, fn.getDeadline(header, newReqMeta));
    assertEquals(DEFAULT_DELAY + VTIMESQRT, fn.getDeadline(header, oldReq));
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.ALL, mockRS.getRSRpcServices());
    assertEquals(DEFAULT_DELAY, fn.getDeadline(header, newReqNoMeta));
    assertEquals(DEFAULT_DELAY, fn.getDeadline(header, newReqMeta));
    assertEquals(ACTUAL_DELAY, fn.getDeadline(header, oldReq));

    // Old scanner against meta.
    mockRS = prepareDeadlineTest(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable(),
      true, DEFAULT_DELAY);
    addMockScanner(mockRS.getRSRpcServices(), SCANNER_ID, ACTUAL_DELAY, VTIMESQRT * VTIMESQRT);
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.NONE, mockRS.getRSRpcServices());
    assertEquals(VTIMESQRT, fn.getDeadline(header, oldReq));
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(ACTUAL_DELAY, fn.getDeadline(header, oldReq));
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.ALL, mockRS.getRSRpcServices());
    assertEquals(ACTUAL_DELAY, fn.getDeadline(header, oldReq));

    // Meta on master shortcut - just check the config.
    mockRS = prepareDeadlineTest(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable(),
      true, DEFAULT_DELAY);
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(ScanDeadlineOnly.META_ONLY, fn.getScanDeadlineOnly());
    Configuration conf = mockRS.getRSRpcServices().getConfiguration();
    conf.setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    conf.setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, true);
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(ScanDeadlineOnly.META_ONLY, fn.getScanDeadlineOnly());
    Mockito.when(mockRS.getRSRpcServices().isMaster()).thenReturn(true);
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(ScanDeadlineOnly.ALL, fn.getScanDeadlineOnly());
    conf.setBoolean(LoadBalancer.TABLES_ON_MASTER, false);
    fn = prepareDeadlineTestFn(ScanDeadlineOnly.META_ONLY, mockRS.getRSRpcServices());
    assertEquals(ScanDeadlineOnly.META_ONLY, fn.getScanDeadlineOnly());
  }
}
