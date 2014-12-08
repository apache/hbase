/**
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

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tmpl.regionserver.RSStatusTmpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Tests for the region server status page and its template.
 */
@Category(SmallTests.class)
public class TestRSStatusServlet {
  private HRegionServer rs;
  
  static final int FAKE_IPC_PORT = 1585;
  static final int FAKE_WEB_PORT = 1586;
  
  private final ServerName fakeServerName =
      ServerName.valueOf("localhost", FAKE_IPC_PORT, 11111);
  private final GetServerInfoResponse fakeResponse =
    ResponseConverter.buildGetServerInfoResponse(fakeServerName, FAKE_WEB_PORT);

  private final ServerName fakeMasterAddress =
      ServerName.valueOf("localhost", 60010, 1212121212);

  @Before
  public void setupBasicMocks() throws IOException, ServiceException {
    rs = Mockito.mock(HRegionServer.class);
    Mockito.doReturn(HBaseConfiguration.create())
      .when(rs).getConfiguration();
    Mockito.doReturn(fakeResponse).when(rs).getServerInfo(
      (RpcController)Mockito.any(), (GetServerInfoRequest)Mockito.any());
    // Fake ZKW
    ZooKeeperWatcher zkw = Mockito.mock(ZooKeeperWatcher.class);
    Mockito.doReturn("fakequorum").when(zkw).getQuorum();
    Mockito.doReturn(zkw).when(rs).getZooKeeper();

    // Fake MasterAddressTracker
    MasterAddressTracker mat = Mockito.mock(MasterAddressTracker.class);
    Mockito.doReturn(fakeMasterAddress).when(mat).getMasterAddress();
    Mockito.doReturn(mat).when(rs).getMasterAddressTracker();

    MetricsRegionServer rms = Mockito.mock(MetricsRegionServer.class);
    Mockito.doReturn(new MetricsRegionServerWrapperStub()).when(rms).getRegionServerWrapper();
    Mockito.doReturn(rms).when(rs).getMetrics();
  }
  
  @Test
  public void testBasic() throws IOException, ServiceException {
    new RSStatusTmpl().render(new StringWriter(), rs);
  }
  
  @Test
  public void testWithRegions() throws IOException, ServiceException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("mytable"));
    List<HRegionInfo> regions = Lists.newArrayList(
        new HRegionInfo(htd.getTableName(), Bytes.toBytes("a"), Bytes.toBytes("d")),
        new HRegionInfo(htd.getTableName(), Bytes.toBytes("d"), Bytes.toBytes("z"))
        );
    Mockito.doReturn(ResponseConverter.buildGetOnlineRegionResponse(
      regions)).when(rs).getOnlineRegion((RpcController)Mockito.any(),
        (GetOnlineRegionRequest)Mockito.any());
    
    new RSStatusTmpl().render(new StringWriter(), rs);
  }
  
  

}

