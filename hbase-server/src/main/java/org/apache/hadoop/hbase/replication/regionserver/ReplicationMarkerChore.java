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

import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_TABLE_NAME;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This chore is responsible to create replication marker rows with special WALEdit with family as
 * {@link org.apache.hadoop.hbase.wal.WALEdit#METAFAMILY} and column qualifier as
 * {@link WALEdit#REPLICATION_MARKER} and empty value. If config key
 * {@link #REPLICATION_MARKER_ENABLED_KEY} is set to true, then we will create 1 marker row every
 * {@link #REPLICATION_MARKER_CHORE_DURATION_KEY} ms
 * {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReader} will populate
 * the Replication Marker edit with region_server_name, wal_name and wal_offset encoded in
 * {@link org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.ReplicationMarkerDescriptor}
 * object. {@link org.apache.hadoop.hbase.replication.regionserver.Replication} will change the
 * REPLICATION_SCOPE for this edit to GLOBAL so that it can replicate. On the sink cluster,
 * {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationSink} will convert the
 * ReplicationMarkerDescriptor into a Put mutation to REPLICATION_SINK_TRACKER_TABLE_NAME_STR table.
 */
@InterfaceAudience.Private
public class ReplicationMarkerChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationMarkerChore.class);
  private static final MultiVersionConcurrencyControl MVCC = new MultiVersionConcurrencyControl();
  public static final RegionInfo REGION_INFO =
    RegionInfoBuilder.newBuilder(REPLICATION_SINK_TRACKER_TABLE_NAME).build();
  private static final String DELIMITER = "_";
  private final Configuration conf;
  private final RegionServerServices rsServices;
  private WAL wal;

  public static final String REPLICATION_MARKER_ENABLED_KEY =
    "hbase.regionserver.replication.marker.enabled";
  public static final boolean REPLICATION_MARKER_ENABLED_DEFAULT = false;

  public static final String REPLICATION_MARKER_CHORE_DURATION_KEY =
    "hbase.regionserver.replication.marker.chore.duration";
  public static final int REPLICATION_MARKER_CHORE_DURATION_DEFAULT = 30 * 1000; // 30 seconds

  public ReplicationMarkerChore(final Stoppable stopper, final RegionServerServices rsServices,
    int period, Configuration conf) {
    super("ReplicationTrackerChore", stopper, period);
    this.conf = conf;
    this.rsServices = rsServices;
  }

  @Override
  protected void chore() {
    if (wal == null) {
      try {
        // TODO: We need to add support for multi WAL implementation.
        wal = rsServices.getWAL(null);
      } catch (IOException ioe) {
        LOG.warn("Unable to get WAL ", ioe);
        // Shouldn't happen. Ignore and wait for the next chore run.
        return;
      }
    }
    String serverName = rsServices.getServerName().getServerName();
    long timeStamp = EnvironmentEdgeManager.currentTime();
    // We only have timestamp in ReplicationMarkerDescriptor and the remaining properties walname,
    // regionserver name and wal offset at ReplicationSourceWALReaderThread.
    byte[] rowKey = getRowKey(serverName, timeStamp);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating replication marker edit.");
    }

    // This creates a new ArrayList of all the online regions for every call.
    List<HRegion> regions = rsServices.getRegions();

    if (regions.isEmpty()) {
      LOG.info("There are no online regions for this server, so skipping adding replication marker"
        + " rows for this regionserver");
      return;
    }
    HRegion region = regions.get(ThreadLocalRandom.current().nextInt(regions.size()));
    try {
      WALUtil.writeReplicationMarkerAndSync(wal, MVCC, region.getRegionInfo(), rowKey, timeStamp);
    } catch (IOException ioe) {
      LOG.error("Exception while sync'ing replication tracker edit", ioe);
      // TODO: Should we stop region server or add a metric and keep going.
    }
  }

  /**
   * Creates a rowkey with region server name and timestamp.
   * @param serverName region server name
   * @param timestamp  timestamp
   */
  public static byte[] getRowKey(String serverName, long timestamp) {
    // converting to string since this will help seeing the timestamp in string format using
    // hbase shell commands.
    String timestampStr = String.valueOf(timestamp);
    final String rowKeyStr = serverName + DELIMITER + timestampStr;
    return Bytes.toBytes(rowKeyStr);
  }
}
