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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.OFFSET_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_TABLE_NAME;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.RS_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.TIMESTAMP_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.WAL_NAME_COLUMN;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test creates 2 mini hbase cluster. One cluster with
 * "hbase.regionserver.replication.marker.enabled" conf key. This will create
 * {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore} which will create
 * marker rows to be replicated to sink cluster. Second cluster with
 * "hbase.regionserver.replication.sink.tracker.enabled" conf key enabled. This will persist the
 * marker rows coming from peer cluster to persist to REPLICATION.SINK_TRACKER table.
 **/
@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationMarker {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationMarker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationMarker.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static HBaseTestingUtil utility1;
  private static HBaseTestingUtil utility2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf2 = new Configuration(conf1);
    // Run the replication marker chore in cluster1.
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    conf1.setLong(REPLICATION_MARKER_CHORE_DURATION_KEY, 1000); // 1 sec
    utility1 = new HBaseTestingUtil(conf1);

    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    // Enable the replication sink tracker for cluster 2
    conf2.setBoolean(REPLICATION_SINK_TRACKER_ENABLED_KEY, true);
    utility2 = new HBaseTestingUtil(conf2);

    // Start cluster 2 first so that hbase:replicationsinktracker table gets created first.
    utility2.startMiniCluster(1);
    waitForReplicationTrackerTableCreation();

    // Start cluster1
    utility1.startMiniCluster(1);
    Admin admin1 = utility1.getAdmin();
    ReplicationPeerConfigBuilder rpcBuilder = ReplicationPeerConfig.newBuilder();
    rpcBuilder.setClusterKey(utility2.getRpcConnnectionURI());
    admin1.addReplicationPeer("1", rpcBuilder.build());

    ReplicationSourceManager manager = utility1.getHBaseCluster().getRegionServer(0)
      .getReplicationSourceService().getReplicationManager();
    // Wait until the peer gets established.
    Waiter.waitFor(conf1, 10000, (Waiter.Predicate) () -> manager.getSources().size() == 1);
  }

  private static void waitForReplicationTrackerTableCreation() {
    Waiter.waitFor(conf2, 10000, (Waiter.Predicate) () -> utility2.getAdmin()
      .tableExists(REPLICATION_SINK_TRACKER_TABLE_NAME));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    utility1.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
  }

  @Test
  public void testReplicationMarkerRow() throws Exception {
    // We have configured ReplicationTrackerChore to run every second. Sleeping so that it will
    // create enough sentinel rows.
    Thread.sleep(5000);
    WAL wal1 = utility1.getHBaseCluster().getRegionServer(0).getWAL(null);
    String walName1ForCluster1 = ((AbstractFSWAL) wal1).getCurrentFileName().getName();
    String rs1Name = utility1.getHBaseCluster().getRegionServer(0).getServerName().getHostname();
    // Since we sync the marker edits while appending to wal, all the edits should be visible
    // to Replication threads immediately.
    assertTrue(getReplicatedEntries() >= 5);
    // Force log roll.
    wal1.rollWriter(true);
    String walName2ForCluster1 = ((AbstractFSWAL) wal1).getCurrentFileName().getName();
    Connection connection2 = utility2.getMiniHBaseCluster().getRegionServer(0).getConnection();
    // Sleep for 5 more seconds to get marker rows with new wal name.
    Thread.sleep(5000);
    // Wait for cluster 2 to have atleast 8 tracker rows from cluster1.
    utility2.waitFor(5000, () -> getTableCount(connection2) >= 8);
    // Get replication marker rows from cluster2
    List<ReplicationSinkTrackerRow> list = getRows(connection2);
    for (ReplicationSinkTrackerRow desc : list) {
      // All the tracker rows should have same region server name i.e. rs of cluster1
      assertEquals(rs1Name, desc.getRegionServerName());
      // All the tracker rows will have either wal1 or wal2 name.
      assertTrue(walName1ForCluster1.equals(desc.getWalName())
        || walName2ForCluster1.equals(desc.getWalName()));
    }

    // This table shouldn't exist on cluster1 since
    // hbase.regionserver.replication.sink.tracker.enabled is not enabled on this cluster.
    assertFalse(utility1.getAdmin().tableExists(REPLICATION_SINK_TRACKER_TABLE_NAME));
    // This table shouldn't exist on cluster1 since
    // hbase.regionserver.replication.sink.tracker.enabled is enabled on this cluster.
    assertTrue(utility2.getAdmin().tableExists(REPLICATION_SINK_TRACKER_TABLE_NAME));
  }

  /*
   * Get rows for replication sink tracker table.
   */
  private List<ReplicationSinkTrackerRow> getRows(Connection connection) throws IOException {
    List<ReplicationSinkTrackerRow> list = new ArrayList<>();
    Scan scan = new Scan();
    Table table = connection.getTable(REPLICATION_SINK_TRACKER_TABLE_NAME);
    ResultScanner scanner = table.getScanner(scan);

    Result r;
    while ((r = scanner.next()) != null) {
      List<Cell> cells = r.listCells();
      list.add(getPayload(cells));
    }
    return list;
  }

  private ReplicationSinkTrackerRow getPayload(List<Cell> cells) {
    String rsName = null, walName = null;
    Long offset = null;
    long timestamp = 0L;
    for (Cell cell : cells) {
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      byte[] value = CellUtil.cloneValue(cell);

      if (Bytes.equals(RS_COLUMN, qualifier)) {
        rsName = Bytes.toString(value);
      } else if (Bytes.equals(WAL_NAME_COLUMN, qualifier)) {
        walName = Bytes.toString(value);
      } else if (Bytes.equals(TIMESTAMP_COLUMN, qualifier)) {
        timestamp = Bytes.toLong(value);
      } else if (Bytes.equals(OFFSET_COLUMN, qualifier)) {
        offset = Bytes.toLong(value);
      }
    }
    ReplicationSinkTrackerRow row =
      new ReplicationSinkTrackerRow(rsName, walName, timestamp, offset);
    return row;
  }

  static class ReplicationSinkTrackerRow {
    private String region_server_name;
    private String wal_name;
    private long timestamp;
    private long offset;

    public ReplicationSinkTrackerRow(String region_server_name, String wal_name, long timestamp,
      long offset) {
      this.region_server_name = region_server_name;
      this.wal_name = wal_name;
      this.timestamp = timestamp;
      this.offset = offset;
    }

    public String getRegionServerName() {
      return region_server_name;
    }

    public String getWalName() {
      return wal_name;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public long getOffset() {
      return offset;
    }

    @Override
    public String toString() {
      return "ReplicationSinkTrackerRow{" + "region_server_name='" + region_server_name + '\''
        + ", wal_name='" + wal_name + '\'' + ", timestamp=" + timestamp + ", offset=" + offset
        + '}';
    }
  }

  private int getTableCount(Connection connection) throws Exception {
    Table table = connection.getTable(REPLICATION_SINK_TRACKER_TABLE_NAME);
    ResultScanner resultScanner = table.getScanner(new Scan().setReadType(Scan.ReadType.STREAM));
    int count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    LOG.info("Table count: " + count);
    return count;
  }

  /*
   * Return replicated entries from cluster1.
   */
  private long getReplicatedEntries() {
    ReplicationSourceManager manager = utility1.getHBaseCluster().getRegionServer(0)
      .getReplicationSourceService().getReplicationManager();
    List<ReplicationSourceInterface> sources = manager.getSources();
    assertEquals(1, sources.size());
    ReplicationSource source = (ReplicationSource) sources.get(0);
    return source.getTotalReplicatedEdits();
  }
}
