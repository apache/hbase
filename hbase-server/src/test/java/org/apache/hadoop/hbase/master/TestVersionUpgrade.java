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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestVersionUpgrade {
  private static HBaseTestingUtility testingUtility;
  private static LocalHBaseCluster cluster;
  private static final int SLEEP_FOR_BIG_OFFHEAP_CACHE = 20 * 1000;

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniDFSCluster(1);
    testingUtility.startMiniZKCluster(1);
    testingUtility.createRootDir();
    cluster = new LocalHBaseCluster(testingUtility.getConfiguration(), 0, 0);
  }

  public static class SlowlyInitializingRegionServerWithHigherVersion
    extends MiniHBaseClusterRegionServer {
    private static final AtomicInteger version = new AtomicInteger();

    private static String getTestVersion() {
      return "0.0." + version.getAndIncrement();
    }

    public SlowlyInitializingRegionServerWithHigherVersion(Configuration conf, CoordinatedStateManager cp)
      throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    protected void instantiateBlockCache() {
      try {
        super.instantiateBlockCache();
        Thread.sleep(SLEEP_FOR_BIG_OFFHEAP_CACHE);
      } catch (InterruptedException e) {
        // Ignore
      }
    }

    @Override
    protected void createMyEphemeralNode() throws KeeperException, IOException {
      HBaseProtos.RegionServerInfo.Builder rsInfo = HBaseProtos.RegionServerInfo.newBuilder();
      rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo().toBuilder().setVersion(getTestVersion()));
      byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
      ZKUtil.createEphemeralNodeAndWatch(zooKeeper, getMyEphemeralNodePath(), data);
    }
  }

  @Test
  public void metaRegionShouldBeMovedIntoRegionServerWithLatestVersionASAP() throws Exception {
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 1);
    cluster.getConfiguration().setClass(HConstants.REGION_SERVER_IMPL,
      SlowlyInitializingRegionServerWithHigherVersion.class,
      HRegionServer.class);

    final MasterThread master = cluster.addMaster();
    final RegionServerThread rs1 = cluster.addRegionServer();
    master.start();
    rs1.start();

    waitForClusterOnline(master, 1);

    final RegionServerThread rs2 = cluster.addRegionServer();
    rs2.start();

    waitForClusterOnline(master, 2);

    Waiter.waitFor(cluster.getConfiguration(), 5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        final RegionStates regionStates =
          master.getMaster().getAssignmentManager().getRegionStates();
        return !regionStates.isMetaRegionInTransition() &&
          regionStates.getServerRegions(rs2.getRegionServer().getServerName())
            .contains(HRegionInfo.FIRST_META_REGIONINFO);
      }
    });
  }

  private static void waitForClusterOnline(MasterThread master, int numRs)
    throws InterruptedException, KeeperException {
    while (!master.getMaster().isInitialized()
      || master.getMaster().getNumLiveRegionServers() != numRs) {
      Thread.sleep(100);
    }
  }
}
