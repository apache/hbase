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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to clean the useless barriers in {@link HConstants#REPLICATION_BARRIER_FAMILY_STR} family in
 * meta table.
 */
@InterfaceAudience.Private
public class ReplicationBarrierCleaner extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationBarrierCleaner.class);

  private static final String REPLICATION_BARRIER_CLEANER_INTERVAL =
    "hbase.master.cleaner.replication.barrier.interval";

  // 12 hour. Usually regions will not be moved so the barrier are rarely updated. Use a large
  // interval.
  private static final int DEFAULT_REPLICATION_BARRIER_CLEANER_INTERVAL = 12 * 60 * 60 * 1000;

  private final Connection conn;

  private final ReplicationPeerManager peerManager;

  public ReplicationBarrierCleaner(Configuration conf, Stoppable stopper, Connection conn,
      ReplicationPeerManager peerManager) {
    super("ReplicationBarrierCleaner", stopper, conf.getInt(REPLICATION_BARRIER_CLEANER_INTERVAL,
      DEFAULT_REPLICATION_BARRIER_CLEANER_INTERVAL));
    this.conn = conn;
    this.peerManager = peerManager;
  }

  @Override
  // Public so can be run out of MasterRpcServices. Synchronized so only one
  // running instance at a time.
  public synchronized void chore() {
    long totalRows = 0;
    long cleanedRows = 0;
    long deletedRows = 0;
    long deletedBarriers = 0;
    long deletedLastPushedSeqIds = 0;
    TableName tableName = null;
    List<String> peerIds = null;
    try (Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
        ResultScanner scanner = metaTable.getScanner(
          new Scan().addFamily(HConstants.REPLICATION_BARRIER_FAMILY).readAllVersions())) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        totalRows++;
        long[] barriers = MetaTableAccessor.getReplicationBarriers(result);
        if (barriers.length == 0) {
          continue;
        }
        byte[] regionName = result.getRow();
        TableName tn = RegionInfo.getTable(regionName);
        if (!tn.equals(tableName)) {
          tableName = tn;
          peerIds = peerManager.getSerialPeerIdsBelongsTo(tableName);
        }
        if (peerIds.isEmpty()) {
          // no serial replication
          // check if the region has already been removed, i.e, no catalog family
          if (metaTable.exists(new Get(regionName).addFamily(HConstants.CATALOG_FAMILY))) {
            // exists, then only keep the newest barrier
            Cell cell = result.getColumnLatestCell(HConstants.REPLICATION_BARRIER_FAMILY,
              HConstants.SEQNUM_QUALIFIER);
            metaTable.delete(new Delete(regionName).addFamily(HConstants.REPLICATION_BARRIER_FAMILY,
              cell.getTimestamp() - 1));
            deletedBarriers += barriers.length - 1;
          } else {
            // not exists, delete all the barriers
            metaTable
              .delete(new Delete(regionName).addFamily(HConstants.REPLICATION_BARRIER_FAMILY));
            deletedBarriers += barriers.length;
          }
          cleanedRows++;
          continue;
        }
        String encodedRegionName = RegionInfo.encodeRegionName(regionName);
        long pushedSeqId = Long.MAX_VALUE;
        for (String peerId : peerIds) {
          pushedSeqId = Math.min(pushedSeqId,
            peerManager.getQueueStorage().getLastSequenceId(encodedRegionName, peerId));
        }
        int index = Arrays.binarySearch(barriers, pushedSeqId);
        if (index == -1) {
          // beyond the first barrier, usually this should not happen but anyway let's add a check
          // for it.
          continue;
        }
        if (index < 0) {
          index = -index - 1;
        } else {
          index++;
        }
        // A special case for merged/split region, and also deleted tables, where we are in the last
        // closed range and the pushedSeqId is the last barrier minus 1.
        if (index == barriers.length - 1 && pushedSeqId == barriers[barriers.length - 1] - 1) {
          // check if the region has already been removed, i.e, no catalog family
          if (!metaTable.exists(new Get(regionName).addFamily(HConstants.CATALOG_FAMILY))) {
            ReplicationQueueStorage queueStorage = peerManager.getQueueStorage();
            for (String peerId: peerIds) {
              queueStorage.removeLastSequenceIds(peerId, Arrays.asList(encodedRegionName));
              deletedLastPushedSeqIds++;
            }
            metaTable
              .delete(new Delete(regionName).addFamily(HConstants.REPLICATION_BARRIER_FAMILY));
            deletedRows++;
            deletedBarriers += barriers.length;
            continue;
          }
        }
        // the barrier before 'index - 1'(exclusive) can be safely removed. See the algorithm in
        // SerialReplicationChecker for more details.
        if (index - 1 > 0) {
          List<Cell> cells = result.getColumnCells(HConstants.REPLICATION_BARRIER_FAMILY,
            HConstants.SEQNUM_QUALIFIER);
          // All barriers before this cell(exclusive) can be removed
          Cell cell = cells.get(cells.size() - index);
          metaTable.delete(new Delete(regionName).addFamily(HConstants.REPLICATION_BARRIER_FAMILY,
            cell.getTimestamp() - 1));
          cleanedRows++;
          deletedBarriers += index - 1;
        }
      }
    } catch (ReplicationException | IOException e) {
      LOG.warn("Failed to clean up replication barrier", e);
    }
    if (totalRows > 0) {
      LOG.info("TotalRows={}, cleanedRows={}, deletedRows={}, deletedBarriers={}, " +
          "deletedLastPushedSeqIds={}", totalRows, cleanedRows, deletedRows,
          deletedBarriers, deletedLastPushedSeqIds);
    }
  }
}
