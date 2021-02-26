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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

@Category({MasterTests.class, SmallTests.class})
public class TestClockSkewDetection {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClockSkewDetection.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestClockSkewDetection.class);

  @Test
  public void testClockSkewDetection() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    ServerManager sm = new ServerManager(new MockNoopMasterServices(conf) {
      @Override
      public ClusterConnection getClusterConnection() {
        ClusterConnection conn = mock(ClusterConnection.class);
        when(conn.getRpcControllerFactory()).thenReturn(mock(RpcControllerFactory.class));
        return conn;
      }
    });

    LOG.debug("regionServerStartup 1");
    InetAddress ia1 = InetAddress.getLocalHost();
    RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
    request.setPort(1234);
    request.setServerStartCode(-1);
    request.setServerCurrentTime(System.currentTimeMillis());
    sm.regionServerStartup(request.build(), 0, "0.0.0", ia1);

    final Configuration c = HBaseConfiguration.create();
    long maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    long warningSkew = c.getLong("hbase.master.warningclockskew", 1000);

    try {
      //Master Time > Region Server Time
      LOG.debug("Test: Master Time > Region Server Time");
      LOG.debug("regionServerStartup 2");
      InetAddress ia2 = InetAddress.getLocalHost();
      request = RegionServerStartupRequest.newBuilder();
      request.setPort(1235);
      request.setServerStartCode(-1);
      request.setServerCurrentTime(System.currentTimeMillis() - maxSkew * 2);
      sm.regionServerStartup(request.build(), 0, "0.0.0", ia2);
      fail("HMaster should have thrown a ClockOutOfSyncException but didn't.");
    } catch(ClockOutOfSyncException e) {
      //we want an exception
      LOG.info("Received expected exception: "+e);
    }

    try {
      // Master Time < Region Server Time
      LOG.debug("Test: Master Time < Region Server Time");
      LOG.debug("regionServerStartup 3");
      InetAddress ia3 = InetAddress.getLocalHost();
      request = RegionServerStartupRequest.newBuilder();
      request.setPort(1236);
      request.setServerStartCode(-1);
      request.setServerCurrentTime(System.currentTimeMillis() + maxSkew * 2);
      sm.regionServerStartup(request.build(), 0, "0.0.0", ia3);
      fail("HMaster should have thrown a ClockOutOfSyncException but didn't.");
    } catch (ClockOutOfSyncException e) {
      // we want an exception
      LOG.info("Received expected exception: " + e);
    }

    // make sure values above warning threshold but below max threshold don't kill
    LOG.debug("regionServerStartup 4");
    InetAddress ia4 = InetAddress.getLocalHost();
    request = RegionServerStartupRequest.newBuilder();
    request.setPort(1237);
    request.setServerStartCode(-1);
    request.setServerCurrentTime(System.currentTimeMillis() - warningSkew * 2);
    sm.regionServerStartup(request.build(), 0, "0.0.0", ia4);

    // make sure values above warning threshold but below max threshold don't kill
    LOG.debug("regionServerStartup 5");
    InetAddress ia5 = InetAddress.getLocalHost();
    request = RegionServerStartupRequest.newBuilder();
    request.setPort(1238);
    request.setServerStartCode(-1);
    request.setServerCurrentTime(System.currentTimeMillis() + warningSkew * 2);
    sm.regionServerStartup(request.build(), 0, "0.0.0", ia5);
  }
}