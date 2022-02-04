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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ServerTask;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UserMetrics;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for RegionsRecoveryChore
 */
@Category({MasterTests.class, SmallTests.class})
public class TestRegionsRecoveryChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionsRecoveryChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionsRecoveryChore.class);

  private static final HBaseTestingUtil HBASE_TESTING_UTILITY = new HBaseTestingUtil();

  private static final String UTF_8_CHARSET = StandardCharsets.UTF_8.name();

  private HMaster hMaster;

  private AssignmentManager assignmentManager;

  private RegionsRecoveryChore regionsRecoveryChore;

  private static int regionNo;
  public static final byte[][] REGION_NAME_LIST = new byte[][]{
    new byte[]{114, 101, 103, 105, 111, 110, 50, 49, 95, 51},
    new byte[]{114, 101, 103, 105, 111, 110, 50, 53, 95, 51},
    new byte[]{114, 101, 103, 105, 111, 110, 50, 54, 95, 52},
    new byte[]{114, 101, 103, 105, 111, 110, 51, 50, 95, 53},
    new byte[]{114, 101, 103, 105, 111, 110, 51, 49, 95, 52},
    new byte[]{114, 101, 103, 105, 111, 110, 51, 48, 95, 51},
    new byte[]{114, 101, 103, 105, 111, 110, 50, 48, 95, 50},
    new byte[]{114, 101, 103, 105, 111, 110, 50, 52, 95, 50},
    new byte[]{114, 101, 103, 105, 111, 110, 50, 57, 95, 50},
    new byte[]{114, 101, 103, 105, 111, 110, 51, 53, 95, 50},
    new byte[]{114, 101, 103, 105, 111, 110, 49, 48, 56, 95, 49, 49}
  };

  private Configuration getCustomConf() {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setInt("hbase.master.regions.recovery.check.interval", 100);
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    this.hMaster = Mockito.mock(HMaster.class);
    this.assignmentManager = Mockito.mock(AssignmentManager.class);
  }

  @After
  public void tearDown() throws Exception {
    Mockito.verifyNoMoreInteractions(this.hMaster);
    Mockito.verifyNoMoreInteractions(this.assignmentManager);
  }

  @Test
  public void testRegionReopensWithStoreRefConfig() throws Exception {
    regionNo = 0;
    ClusterMetrics clusterMetrics = TestRegionsRecoveryChore.getClusterMetrics(4);
    final Map<ServerName, ServerMetrics> serverMetricsMap =
      clusterMetrics.getLiveServerMetrics();
    LOG.debug("All Region Names with refCount....");
    for (ServerMetrics serverMetrics : serverMetricsMap.values()) {
      Map<byte[], RegionMetrics> regionMetricsMap = serverMetrics.getRegionMetrics();
      for (RegionMetrics regionMetrics : regionMetricsMap.values()) {
        LOG.debug("name: " + new String(regionMetrics.getRegionName()) + " refCount: " +
          regionMetrics.getStoreRefCount());
      }
    }
    Mockito.when(hMaster.getClusterMetrics()).thenReturn(clusterMetrics);
    Mockito.when(hMaster.getAssignmentManager()).thenReturn(assignmentManager);
    for (byte[] regionName : REGION_NAME_LIST) {
      Mockito.when(assignmentManager.getRegionInfo(regionName))
        .thenReturn(TestRegionsRecoveryChore.getRegionInfo(regionName));
    }
    Stoppable stoppable = new StoppableImplementation();
    Configuration configuration = getCustomConf();
    configuration.setInt("hbase.regions.recovery.store.file.ref.count", 300);
    regionsRecoveryChore = new RegionsRecoveryChore(stoppable, configuration, hMaster);
    regionsRecoveryChore.chore();

    // Verify that we need to reopen regions of 2 tables
    Mockito.verify(hMaster, Mockito.times(2)).reopenRegions(Mockito.any(), Mockito.anyList(),
      Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(hMaster, Mockito.times(1)).getClusterMetrics();

    // Verify that we need to reopen total 3 regions that have refCount > 300
    Mockito.verify(hMaster, Mockito.times(3)).getAssignmentManager();
    Mockito.verify(assignmentManager, Mockito.times(3))
      .getRegionInfo(Mockito.any());
  }

  @Test
  public void testRegionReopensWithLessThreshold() throws Exception {
    regionNo = 0;
    ClusterMetrics clusterMetrics = TestRegionsRecoveryChore.getClusterMetrics(4);
    final Map<ServerName, ServerMetrics> serverMetricsMap =
      clusterMetrics.getLiveServerMetrics();
    LOG.debug("All Region Names with refCount....");
    for (ServerMetrics serverMetrics : serverMetricsMap.values()) {
      Map<byte[], RegionMetrics> regionMetricsMap = serverMetrics.getRegionMetrics();
      for (RegionMetrics regionMetrics : regionMetricsMap.values()) {
        LOG.debug("name: " + new String(regionMetrics.getRegionName()) + " refCount: " +
          regionMetrics.getStoreRefCount());
      }
    }
    Mockito.when(hMaster.getClusterMetrics()).thenReturn(clusterMetrics);
    Mockito.when(hMaster.getAssignmentManager()).thenReturn(assignmentManager);
    for (byte[] regionName : REGION_NAME_LIST) {
      Mockito.when(assignmentManager.getRegionInfo(regionName))
        .thenReturn(TestRegionsRecoveryChore.getRegionInfo(regionName));
    }
    Stoppable stoppable = new StoppableImplementation();
    Configuration configuration = getCustomConf();
    configuration.setInt("hbase.regions.recovery.store.file.ref.count", 400);
    regionsRecoveryChore = new RegionsRecoveryChore(stoppable, configuration, hMaster);
    regionsRecoveryChore.chore();

    // Verify that we need to reopen regions of only 1 table
    Mockito.verify(hMaster, Mockito.times(1)).reopenRegions(Mockito.any(), Mockito.anyList(),
      Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(hMaster, Mockito.times(1)).getClusterMetrics();

    // Verify that we need to reopen only 1 region with refCount > 400
    Mockito.verify(hMaster, Mockito.times(1)).getAssignmentManager();
    Mockito.verify(assignmentManager, Mockito.times(1))
      .getRegionInfo(Mockito.any());
  }

  @Test
  public void testRegionReopensWithoutStoreRefConfig() throws Exception {
    regionNo = 0;
    ClusterMetrics clusterMetrics = TestRegionsRecoveryChore.getClusterMetrics(10);
    final Map<ServerName, ServerMetrics> serverMetricsMap =
      clusterMetrics.getLiveServerMetrics();
    LOG.debug("All Region Names with refCount....");
    for (ServerMetrics serverMetrics : serverMetricsMap.values()) {
      Map<byte[], RegionMetrics> regionMetricsMap = serverMetrics.getRegionMetrics();
      for (RegionMetrics regionMetrics : regionMetricsMap.values()) {
        LOG.debug("name: " + new String(regionMetrics.getRegionName()) + " refCount: " +
          regionMetrics.getStoreRefCount());
      }
    }
    Mockito.when(hMaster.getClusterMetrics()).thenReturn(clusterMetrics);
    Mockito.when(hMaster.getAssignmentManager()).thenReturn(assignmentManager);
    for (byte[] regionName : REGION_NAME_LIST) {
      Mockito.when(assignmentManager.getRegionInfo(regionName))
        .thenReturn(TestRegionsRecoveryChore.getRegionInfo(regionName));
    }
    Stoppable stoppable = new StoppableImplementation();
    Configuration configuration = getCustomConf();
    configuration.unset("hbase.regions.recovery.store.file.ref.count");
    regionsRecoveryChore = new RegionsRecoveryChore(stoppable, configuration, hMaster);
    regionsRecoveryChore.chore();

    // Verify that by default the feature is turned off so no regions
    // should be reopened
    Mockito.verify(hMaster, Mockito.times(0)).reopenRegions(Mockito.any(), Mockito.anyList(),
      Mockito.anyLong(), Mockito.anyLong());

    // default maxCompactedStoreFileRefCount is -1 (no regions to be reopened using AM)
    Mockito.verify(hMaster, Mockito.times(0)).getAssignmentManager();
    Mockito.verify(assignmentManager, Mockito.times(0))
      .getRegionInfo(Mockito.any());
  }

  private static ClusterMetrics getClusterMetrics(int noOfLiveServer) {
    ClusterMetrics clusterMetrics = new ClusterMetrics() {

      @Nullable
      @Override
      public String getHBaseVersion() {
        return null;
      }

      @Override
      public List<ServerName> getDeadServerNames() {
        return null;
      }

      @Override
      public Map<ServerName, ServerMetrics> getLiveServerMetrics() {
        Map<ServerName, ServerMetrics> liveServerMetrics = new HashMap<>();
        for (int i = 0; i < noOfLiveServer; i++) {
          ServerName serverName = ServerName.valueOf("rs_" + i, 16010, 12345);
          liveServerMetrics.put(serverName, TestRegionsRecoveryChore.getServerMetrics(i + 3));
        }
        return liveServerMetrics;
      }

      @Nullable
      @Override
      public ServerName getMasterName() {
        return null;
      }

      @Override
      public List<ServerName> getBackupMasterNames() {
        return null;
      }

      @Override
      public List<RegionState> getRegionStatesInTransition() {
        return null;
      }

      @Nullable
      @Override
      public String getClusterId() {
        return null;
      }

      @Override
      public List<String> getMasterCoprocessorNames() {
        return null;
      }

      @Nullable
      @Override
      public Boolean getBalancerOn() {
        return null;
      }

      @Override
      public int getMasterInfoPort() {
        return 0;
      }

      @Override
      public List<ServerName> getServersName() {
        return null;
      }

      @Override
      public Map<TableName, RegionStatesCount> getTableRegionStatesCount() {
        return null;
      }

      @Override
      public List<ServerTask> getMasterTasks() {
        return null;
      }

    };
    return clusterMetrics;
  }

  private static ServerMetrics getServerMetrics(int noOfRegions) {
    ServerMetrics serverMetrics = new ServerMetrics() {

      @Override
      public ServerName getServerName() {
        return null;
      }

      @Override
      public long getRequestCountPerSecond() {
        return 0;
      }

      @Override
      public long getRequestCount() {
        return 0;
      }

      @Override
      public long getReadRequestsCount() {
        return 0;
      }

      @Override
      public long getWriteRequestsCount() {
        return 0;
      }

      @Override
      public Size getUsedHeapSize() {
        return null;
      }

      @Override
      public Size getMaxHeapSize() {
        return null;
      }

      @Override
      public int getInfoServerPort() {
        return 0;
      }

      @Override
      public List<ReplicationLoadSource> getReplicationLoadSourceList() {
        return null;
      }

      @Override
      public Map<String, List<ReplicationLoadSource>> getReplicationLoadSourceMap() {
        return null;
      }

      @Nullable
      @Override
      public ReplicationLoadSink getReplicationLoadSink() {
        return null;
      }

      @Override
      public Map<byte[], RegionMetrics> getRegionMetrics() {
        Map<byte[], RegionMetrics> regionMetricsMap = new HashMap<>();
        for (int i = 0; i < noOfRegions; i++) {
          byte[] regionName = Bytes.toBytes("region" + regionNo + "_" + i);
          regionMetricsMap.put(regionName,
            TestRegionsRecoveryChore.getRegionMetrics(regionName, 100 * i));
          ++regionNo;
        }
        return regionMetricsMap;
      }

      @Override public Map<byte[], UserMetrics> getUserMetrics() {
        return new HashMap<>();
      }

      @Override
      public Set<String> getCoprocessorNames() {
        return null;
      }

      @Override
      public long getReportTimestamp() {
        return 0;
      }

      @Override
      public long getLastReportTimestamp() {
        return 0;
      }

      @Override
      public List<ServerTask> getTasks() {
        return null;
      }

    };
    return serverMetrics;
  }

  private static RegionMetrics getRegionMetrics(byte[] regionName, int compactedStoreRefCount) {
    RegionMetrics regionMetrics = new RegionMetrics() {

      @Override
      public byte[] getRegionName() {
        return regionName;
      }

      @Override
      public int getStoreCount() {
        return 0;
      }

      @Override
      public int getStoreFileCount() {
        return 0;
      }

      @Override
      public Size getStoreFileSize() {
        return null;
      }

      @Override
      public Size getMemStoreSize() {
        return null;
      }

      @Override
      public long getReadRequestCount() {
        return 0;
      }

      @Override
      public long getWriteRequestCount() {
        return 0;
      }

      @Override
      public long getCpRequestCount() {
        return 0;
      }

      @Override
      public long getFilteredReadRequestCount() {
        return 0;
      }

      @Override
      public Size getStoreFileIndexSize() {
        return null;
      }

      @Override
      public Size getStoreFileRootLevelIndexSize() {
        return null;
      }

      @Override
      public Size getStoreFileUncompressedDataIndexSize() {
        return null;
      }

      @Override
      public Size getBloomFilterSize() {
        return null;
      }

      @Override
      public long getCompactingCellCount() {
        return 0;
      }

      @Override
      public long getCompactedCellCount() {
        return 0;
      }

      @Override
      public long getCompletedSequenceId() {
        return 0;
      }

      @Override
      public Map<byte[], Long> getStoreSequenceId() {
        return null;
      }

      @Override
      public Size getUncompressedStoreFileSize() {
        return null;
      }

      @Override
      public float getDataLocality() {
        return 0;
      }

      @Override
      public long getLastMajorCompactionTimestamp() {
        return 0;
      }

      @Override
      public int getStoreRefCount() {
        return compactedStoreRefCount;
      }

      @Override
      public int getMaxCompactedStoreFileRefCount() {
        return compactedStoreRefCount;
      }

      @Override
      public float getDataLocalityForSsd() {
        return 0;
      }

      @Override
      public long getBlocksLocalWeight() {
        return 0;
      }

      @Override
      public long getBlocksLocalWithSsdWeight() {
        return 0;
      }

      @Override
      public long getBlocksTotalWeight() {
        return 0;
      }

      @Override
      public CompactionState getCompactionState() {
        return null;
      }
    };
    return regionMetrics;
  }

  private static RegionInfo getRegionInfo(byte[] regionNameBytes) {
    RegionInfo regionInfo = new RegionInfo() {

      @Override
      public String getShortNameToLog() {
        return null;
      }

      @Override
      public long getRegionId() {
        return 0;
      }

      @Override
      public byte[] getRegionName() {
        return new byte[0];
      }

      @Override
      public String getRegionNameAsString() {
        try {
          return new String(regionNameBytes, UTF_8_CHARSET);
        } catch (UnsupportedEncodingException e) {
          return "";
        }
      }

      @Override
      public String getEncodedName() {
        return null;
      }

      @Override
      public byte[] getEncodedNameAsBytes() {
        return new byte[0];
      }

      @Override
      public byte[] getStartKey() {
        return new byte[0];
      }

      @Override
      public byte[] getEndKey() {
        return new byte[0];
      }

      @Override
      public TableName getTable() {
        String regionName;
        try {
          regionName = new String(regionNameBytes, UTF_8_CHARSET);
        } catch (UnsupportedEncodingException e) {
          regionName = "";
        }
        int regionNo = Integer.parseInt(regionName.split("_")[1]);
        TableName tableName = TableName.valueOf("table_" + regionNo % 3);
        return tableName;
      }

      @Override
      public int getReplicaId() {
        return 0;
      }

      @Override
      public boolean isSplit() {
        return false;
      }

      @Override
      public boolean isOffline() {
        return false;
      }

      @Override
      public boolean isSplitParent() {
        return false;
      }

      @Override
      public boolean isMetaRegion() {
        return false;
      }

      @Override
      public boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey) {
        return false;
      }

      @Override
      public boolean containsRow(byte[] row) {
        return false;
      }

    };
    return regionInfo;
  }

  /**
   * Simple helper class that just keeps track of whether or not its stopped.
   */
  private static class StoppableImplementation implements Stoppable {

    private volatile boolean stop = false;

    @Override
    public void stop(String why) {
      this.stop = true;
    }

    @Override
    public boolean isStopped() {
      return this.stop;
    }

  }

}
