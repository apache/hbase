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
package org.apache.hadoop.hbase.master.waleventtracker;

import static org.apache.hadoop.hbase.HConstants.NO_NONCE;
import static org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor.WAL_EVENT_TRACKER_TABLE_NAME_STR;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.namequeues.WALEventTrackerTableAccessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WALEventTracker Table creation to be used by HMaster
 */
@InterfaceAudience.Private
public final class WALEventTrackerTableCreator {
  private static final Logger LOG = LoggerFactory.getLogger(WALEventTrackerTableCreator.class);

  public static final String WAL_EVENT_TRACKER_ENABLED_KEY =
    "hbase.regionserver.wal.event.tracker.enabled";
  public static final boolean WAL_EVENT_TRACKER_ENABLED_DEFAULT = false;

  /** The walEventTracker info family as a string */
  private static final String WAL_EVENT_TRACKER_INFO_FAMILY_STR = "info";

  /** The walEventTracker info family in array of bytes */
  public static final byte[] WAL_EVENT_TRACKER_INFO_FAMILY =
    Bytes.toBytes(WAL_EVENT_TRACKER_INFO_FAMILY_STR);

  private static final long TTL = TimeUnit.DAYS.toSeconds(365); // 1 year in seconds

  private static final TableDescriptorBuilder TABLE_DESCRIPTOR_BUILDER = TableDescriptorBuilder
    .newBuilder(WALEventTrackerTableAccessor.WAL_EVENT_TRACKER_TABLE_NAME).setRegionReplication(1)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(WAL_EVENT_TRACKER_INFO_FAMILY)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBlockCacheEnabled(false).setMaxVersions(1)
      .setTimeToLive((int) TTL).build());

  /* Private default constructor */
  private WALEventTrackerTableCreator() {
  }

  /*
   * We will create this table only if hbase.regionserver.wal.event.tracker.enabled is enabled and
   * table doesn't exists already.
   */
  public static void createIfNeededAndNotExists(Configuration conf, MasterServices masterServices)
    throws IOException {
    boolean walEventTrackerEnabled =
      conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT);
    if (!walEventTrackerEnabled) {
      LOG.info("wal event tracker requests logging to table " + WAL_EVENT_TRACKER_TABLE_NAME_STR
        + " is disabled. Quitting.");
      return;
    }
    if (
      !masterServices.getTableDescriptors()
        .exists(WALEventTrackerTableAccessor.WAL_EVENT_TRACKER_TABLE_NAME)
    ) {
      LOG.info(WAL_EVENT_TRACKER_TABLE_NAME_STR + " table not found. Creating.");
      masterServices.createTable(TABLE_DESCRIPTOR_BUILDER.build(), null, 0L, NO_NONCE);
    }
  }
}
