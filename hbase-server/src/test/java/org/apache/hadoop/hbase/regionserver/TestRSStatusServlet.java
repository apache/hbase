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

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.MetricsHBaseServer;
import org.apache.hadoop.hbase.ipc.MetricsHBaseServerWrapperStub;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tmpl.regionserver.RSStatusTmpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;

/**
 * Tests for the region server status page and its template.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestRSStatusServlet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSStatusServlet.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSStatusServlet.class);
  private HRegionServer rs;
  private RSRpcServices rpcServices;
  private RpcServerInterface rpcServer;

  static final int FAKE_IPC_PORT = 1585;
  static final int FAKE_WEB_PORT = 1586;

  private final ServerName fakeServerName =
      ServerName.valueOf("localhost", FAKE_IPC_PORT, 11111);
  private final GetServerInfoResponse fakeResponse =
    ResponseConverter.buildGetServerInfoResponse(fakeServerName, FAKE_WEB_PORT);

  private final ServerName fakeMasterAddress =
      ServerName.valueOf("localhost", 60010, 1212121212);

  @Rule
  public TestName name = new TestName();

  @Before
  public void setupBasicMocks() throws IOException, ServiceException {
    rs = Mockito.mock(HRegionServer.class);
    rpcServices = Mockito.mock(RSRpcServices.class);
    rpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.doReturn(HBaseConfiguration.create()).when(rs).getConfiguration();
    Mockito.doReturn(rpcServices).when(rs).getRSRpcServices();
    Mockito.doReturn(rpcServer).when(rs).getRpcServer();
    Mockito.doReturn(fakeResponse).when(rpcServices).getServerInfo(Mockito.any(), Mockito.any());
    // Fake ZKW
    ZKWatcher zkw = Mockito.mock(ZKWatcher.class);
    Mockito.doReturn("fakequorum").when(zkw).getQuorum();
    Mockito.doReturn(zkw).when(rs).getZooKeeper();

    // Fake BlockCache
    LOG.warn("The " + HConstants.HFILE_BLOCK_CACHE_SIZE_KEY + " is set to 0");
    Mockito.doReturn(Optional.empty()).when(rs).getBlockCache();

    // Fake MasterAddressTracker
    MasterAddressTracker mat = Mockito.mock(MasterAddressTracker.class);
    Mockito.doReturn(fakeMasterAddress).when(mat).getMasterAddress();
    Mockito.doReturn(mat).when(rs).getMasterAddressTracker();

    MetricsRegionServer rms = Mockito.mock(MetricsRegionServer.class);
    Mockito.doReturn(new MetricsRegionServerWrapperStub()).when(rms).getRegionServerWrapper();
    Mockito.doReturn(rms).when(rs).getMetrics();

    MetricsHBaseServer ms = Mockito.mock(MetricsHBaseServer.class);
    Mockito.doReturn(new MetricsHBaseServerWrapperStub()).when(ms).getHBaseServerWrapper();
    Mockito.doReturn(ms).when(rpcServer).getMetrics();
    Mockito.doReturn(ByteBuffAllocator.HEAP).when(rpcServer).getByteBuffAllocator();
  }

  @Test
  public void testBasic() throws IOException, ServiceException {
    new RSStatusTmpl().render(new StringWriter(), rs);
  }

  @Test
  public void testWithRegions() throws IOException, ServiceException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    List<RegionInfo> regions = Lists.newArrayList(
      RegionInfoBuilder.newBuilder(htd.getTableName()).setStartKey(Bytes.toBytes("a"))
          .setEndKey(Bytes.toBytes("d")).build(),
      RegionInfoBuilder.newBuilder(htd.getTableName()).setStartKey(Bytes.toBytes("d"))
          .setEndKey(Bytes.toBytes("z")).build());
    Mockito.doReturn(ResponseConverter.buildGetOnlineRegionResponse(regions)).when(rpcServices)
        .getOnlineRegion(Mockito.any(), Mockito.any());
    new RSStatusTmpl().render(new StringWriter(), rs);
  }
}
