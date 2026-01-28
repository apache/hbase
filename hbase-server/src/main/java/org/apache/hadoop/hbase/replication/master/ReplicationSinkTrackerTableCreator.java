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
package org.apache.hadoop.hbase.replication.master;

import static org.apache.hadoop.hbase.HConstants.NO_NONCE;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This will create {@link #REPLICATION_SINK_TRACKER_TABLE_NAME_STR} table if
 * hbase.regionserver.replication.sink.tracker.enabled config key is enabled and table not created
 **/
@InterfaceAudience.Private
public final class ReplicationSinkTrackerTableCreator {
  private static final Logger LOG =
    LoggerFactory.getLogger(ReplicationSinkTrackerTableCreator.class);
  private static final long TTL = TimeUnit.DAYS.toSeconds(365); // 1 year in seconds

  public static final byte[] RS_COLUMN = Bytes.toBytes("region_server_name");
  public static final byte[] WAL_NAME_COLUMN = Bytes.toBytes("wal_name");
  public static final byte[] TIMESTAMP_COLUMN = Bytes.toBytes("timestamp");
  public static final byte[] OFFSET_COLUMN = Bytes.toBytes("offset");

  /** Will create {@link #REPLICATION_SINK_TRACKER_TABLE_NAME_STR} table if this conf is enabled **/
  public static final String REPLICATION_SINK_TRACKER_ENABLED_KEY =
    "hbase.regionserver.replication.sink.tracker.enabled";
  public static final boolean REPLICATION_SINK_TRACKER_ENABLED_DEFAULT = false;

  /** The {@link #REPLICATION_SINK_TRACKER_TABLE_NAME_STR} info family as a string */
  private static final String REPLICATION_SINK_TRACKER_INFO_FAMILY_STR = "info";

  /** The {@link #REPLICATION_SINK_TRACKER_TABLE_NAME_STR} info family in array of bytes */
  public static final byte[] REPLICATION_SINK_TRACKER_INFO_FAMILY =
    Bytes.toBytes(REPLICATION_SINK_TRACKER_INFO_FAMILY_STR);

  public static final String REPLICATION_SINK_TRACKER_TABLE_NAME_STR = "REPLICATION.SINK_TRACKER";

  /* Private default constructor */
  private ReplicationSinkTrackerTableCreator() {
  }

  /**
   * {@link #REPLICATION_SINK_TRACKER_TABLE_NAME_STR} table name - can be enabled with config -
   * hbase.regionserver.replication.sink.tracker.enabled
   */
  public static final TableName REPLICATION_SINK_TRACKER_TABLE_NAME =
    TableName.valueOf(REPLICATION_SINK_TRACKER_TABLE_NAME_STR);

  private static final TableDescriptorBuilder TABLE_DESCRIPTOR_BUILDER = TableDescriptorBuilder
    .newBuilder(REPLICATION_SINK_TRACKER_TABLE_NAME).setRegionReplication(1)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(REPLICATION_SINK_TRACKER_INFO_FAMILY)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBlockCacheEnabled(false).setMaxVersions(1)
      .setTimeToLive((int) TTL).build());

  /*
   * We will create this table only if hbase.regionserver.replication.sink.tracker.enabled is
   * enabled and table doesn't exist already.
   */
  public static void createIfNeededAndNotExists(Configuration conf, MasterServices masterServices)
    throws IOException {
    boolean replicationSinkTrackerEnabled = conf.getBoolean(REPLICATION_SINK_TRACKER_ENABLED_KEY,
      REPLICATION_SINK_TRACKER_ENABLED_DEFAULT);
    if (!replicationSinkTrackerEnabled) {
      LOG.info("replication sink tracker requests logging to table {} is disabled." + " Quitting.",
        REPLICATION_SINK_TRACKER_TABLE_NAME_STR);
      return;
    }
    if (!masterServices.getTableDescriptors().exists(REPLICATION_SINK_TRACKER_TABLE_NAME)) {
      LOG.info("{} table not found. Creating.", REPLICATION_SINK_TRACKER_TABLE_NAME_STR);
      masterServices.createTable(TABLE_DESCRIPTOR_BUILDER.build(), null, 0L, NO_NONCE);
    }
  }
}
