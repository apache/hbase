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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterMetrics.class);
  private static final MetricsAssertHelper metricsHelper =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private static SingleProcessHBaseCluster cluster;
  private static HMaster master;
  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  public static class MyMaster extends HMaster {

    public MyMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    protected MasterRpcServices createRpcServices() throws IOException {
      return new MasterRpcServices(this) {

        @Override
        public RegionServerStartupResponse regionServerStartup(RpcController controller,
          RegionServerStartupRequest request) throws ServiceException {
          RegionServerStartupResponse resp = super.regionServerStartup(controller, request);
          ServerManager serverManager = getServerManager();
          // to let the region server actual online otherwise we can not assign meta region
          new HashMap<>(serverManager.getOnlineServers()).forEach((sn, sm) -> {
            if (sm.getLastReportTimestamp() <= 0) {
              try {
                serverManager.regionServerReport(sn,
                  ServerMetricsBuilder.newBuilder(sn).setVersionNumber(sm.getVersionNumber())
                    .setVersion(sm.getVersion())
                    .setLastReportTimestamp(EnvironmentEdgeManager.currentTime()).build());
              } catch (YouAreDeadException e) {
                throw new UncheckedIOException(e);
              }
            }
          });
          return resp;
        }
      };
    }
  }

  public static class MyRegionServer
    extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {

    public MyRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime) {
      // do nothing
    }
  }

  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    // Set master class and use default values for other options.
    StartTestingClusterOption option = StartTestingClusterOption.builder()
      .masterClass(MyMaster.class).rsClass(MyRegionServer.class).build();
    TEST_UTIL.startMiniCluster(option);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    master.stopMaster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClusterRequests() throws Exception {
    // sending fake request to master to see how metric value has changed
    RegionServerStatusProtos.RegionServerReportRequest.Builder request =
      RegionServerStatusProtos.RegionServerReportRequest.newBuilder();
    ServerName serverName = cluster.getMaster(0).getServerName();
    request.setServer(ProtobufUtil.toServerName(serverName));
    long expectedRequestNumber = 10000;

    MetricsMasterSource masterSource = master.getMasterMetrics().getMetricsSource();
    ClusterStatusProtos.ServerLoad sl = ClusterStatusProtos.ServerLoad.newBuilder()
      .setTotalNumberOfRequests(expectedRequestNumber).setReadRequestsCount(expectedRequestNumber)
      .setWriteRequestsCount(expectedRequestNumber).build();
    request.setLoad(sl);

    master.getMasterRpcServices().regionServerReport(null, request.build());
    metricsHelper.assertCounter("cluster_requests", expectedRequestNumber, masterSource);
    metricsHelper.assertCounter("cluster_read_requests", expectedRequestNumber, masterSource);
    metricsHelper.assertCounter("cluster_write_requests", expectedRequestNumber, masterSource);

    expectedRequestNumber = 15000;

    sl = ClusterStatusProtos.ServerLoad.newBuilder().setTotalNumberOfRequests(expectedRequestNumber)
      .setReadRequestsCount(expectedRequestNumber).setWriteRequestsCount(expectedRequestNumber)
      .build();
    request.setLoad(sl);

    master.getMasterRpcServices().regionServerReport(null, request.build());
    metricsHelper.assertCounter("cluster_requests", expectedRequestNumber, masterSource);
    metricsHelper.assertCounter("cluster_read_requests", expectedRequestNumber, masterSource);
    metricsHelper.assertCounter("cluster_write_requests", expectedRequestNumber, masterSource);
  }

  @Test
  public void testDefaultMasterMetrics() throws Exception {
    MetricsMasterSource masterSource = master.getMasterMetrics().getMetricsSource();
    metricsHelper.assertGauge("numRegionServers", 1, masterSource);
    metricsHelper.assertGauge("averageLoad", 1, masterSource);
    metricsHelper.assertGauge("numDeadRegionServers", 0, masterSource);
    metricsHelper.assertGauge("numDrainingRegionServers", 0, masterSource);

    metricsHelper.assertGauge("masterStartTime", master.getMasterStartTime(), masterSource);
    metricsHelper.assertGauge("masterActiveTime", master.getMasterActiveTime(), masterSource);

    metricsHelper.assertTag("isActiveMaster", "true", masterSource);
    metricsHelper.assertTag("serverName", master.getServerName().toString(), masterSource);
    metricsHelper.assertTag("clusterId", master.getClusterId(), masterSource);
    metricsHelper.assertTag("zookeeperQuorum", master.getZooKeeper().getQuorum(), masterSource);

    metricsHelper.assertCounter(MetricsMasterSource.SERVER_CRASH_METRIC_PREFIX + "SubmittedCount",
      0, masterSource);
    metricsHelper.assertGauge("oldWALsDirSize", master.getMasterWalManager().getOldWALsDirSize(),
      masterSource);
  }

  @Test
  public void testDefaultMasterProcMetrics() throws Exception {
    MetricsMasterProcSource masterSource = master.getMasterMetrics().getMetricsProcSource();
    metricsHelper.assertGauge("numMasterWALs", master.getNumWALFiles(), masterSource);
  }

  @Test
  public void testClusterMetricsMetaTableSkipping() throws Exception {
    TableName replicaMetaTable = TableName.valueOf("hbase", "meta_replica");
    TableDescriptor replicaMetaDescriptor = TableDescriptorBuilder.newBuilder(replicaMetaTable)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("info")).build();
    master.getTableDescriptors().update(replicaMetaDescriptor, true);
    try {
      ClusterMetrics metrics = master.getClusterMetricsWithoutCoprocessor(
        EnumSet.of(ClusterMetrics.Option.TABLE_TO_REGIONS_COUNT));
      Map<TableName, RegionStatesCount> tableRegionStatesCount =
        metrics.getTableRegionStatesCount();

      assertFalse("Foreign meta table should not be present",
        tableRegionStatesCount.containsKey(replicaMetaTable));
      assertTrue("Local meta should be present",
        tableRegionStatesCount.containsKey(TableName.META_TABLE_NAME));

    } finally {
      master.getTableDescriptors().remove(replicaMetaTable);
    }
  }
}
