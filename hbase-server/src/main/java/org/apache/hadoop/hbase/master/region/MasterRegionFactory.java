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
package org.apache.hadoop.hbase.master.region;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The factory class for creating a {@link MasterRegion}.
 */
@InterfaceAudience.Private
public final class MasterRegionFactory {

  // Use the character $ to let the log cleaner know that this is not the normal wal file.
  public static final String ARCHIVED_WAL_SUFFIX = "$masterlocalwal$";

  // this is a bit trick that in StoreFileInfo.validateStoreFileName, we just test if the file name
  // contains '-' to determine if it is a valid store file, so here we have to add '-'in the file
  // name to avoid being processed by normal TimeToLiveHFileCleaner.
  public static final String ARCHIVED_HFILE_SUFFIX = "$-masterlocalhfile-$";

  private static final String MAX_WALS_KEY = "hbase.master.store.region.maxwals";

  private static final int DEFAULT_MAX_WALS = 10;

  public static final String USE_HSYNC_KEY = "hbase.master.store.region.wal.hsync";

  public static final String MASTER_STORE_DIR = "MasterData";

  private static final String FLUSH_SIZE_KEY = "hbase.master.store.region.flush.size";

  private static final long DEFAULT_FLUSH_SIZE = TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE;

  private static final String FLUSH_PER_CHANGES_KEY = "hbase.master.store.region.flush.per.changes";

  private static final long DEFAULT_FLUSH_PER_CHANGES = 1_000_000;

  private static final String FLUSH_INTERVAL_MS_KEY = "hbase.master.store.region.flush.interval.ms";

  // default to flush every 15 minutes, for safety
  private static final long DEFAULT_FLUSH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(15);

  private static final String COMPACT_MIN_KEY = "hbase.master.store.region.compact.min";

  private static final int DEFAULT_COMPACT_MIN = 4;

  private static final String ROLL_PERIOD_MS_KEY = "hbase.master.store.region.walroll.period.ms";

  private static final long DEFAULT_ROLL_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);

  private static final String RING_BUFFER_SLOT_COUNT = "hbase.master.store.ringbuffer.slot.count";

  private static final int DEFAULT_RING_BUFFER_SLOT_COUNT = 128;

  public static final String TRACKER_IMPL = "hbase.master.store.region.file-tracker.impl";

  public static final TableName TABLE_NAME = TableName.valueOf("master:store");

  public static final byte[] PROC_FAMILY = Bytes.toBytes("proc");

  public static final byte[] REGION_SERVER_FAMILY = Bytes.toBytes("rs");

  private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE_NAME)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
      .setMaxVersions(HConstants.DEFAULT_HBASE_META_VERSIONS).setInMemory(true)
      .setBlocksize(HConstants.DEFAULT_HBASE_META_BLOCK_SIZE).setBloomFilterType(BloomType.ROWCOL)
      .setDataBlockEncoding(DataBlockEncoding.ROW_INDEX_V1).build())
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(PROC_FAMILY))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(REGION_SERVER_FAMILY))
    .build();

  private static TableDescriptor withTrackerConfigs(Configuration conf) {
    String trackerImpl = conf.get(TRACKER_IMPL, conf.get(StoreFileTrackerFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.DEFAULT.name()));
    Class<? extends StoreFileTracker> trackerClass =
      StoreFileTrackerFactory.getTrackerClass(trackerImpl);
    if (StoreFileTrackerFactory.isMigration(trackerClass)) {
      throw new IllegalArgumentException("Should not set store file tracker to " +
        StoreFileTrackerFactory.Trackers.MIGRATION.name() + " for master local region");
    }
    StoreFileTracker tracker = ReflectionUtils.newInstance(trackerClass, conf, true, null);
    return tracker.updateWithTrackerConfigs(TableDescriptorBuilder.newBuilder(TABLE_DESC)).build();
  }

  public static MasterRegion create(Server server) throws IOException {
    Configuration conf = server.getConfiguration();
    MasterRegionParams params = new MasterRegionParams().server(server)
      .regionDirName(MASTER_STORE_DIR).tableDescriptor(withTrackerConfigs(conf));
    long flushSize = conf.getLong(FLUSH_SIZE_KEY, DEFAULT_FLUSH_SIZE);
    long flushPerChanges = conf.getLong(FLUSH_PER_CHANGES_KEY, DEFAULT_FLUSH_PER_CHANGES);
    long flushIntervalMs = conf.getLong(FLUSH_INTERVAL_MS_KEY, DEFAULT_FLUSH_INTERVAL_MS);
    int compactMin = conf.getInt(COMPACT_MIN_KEY, DEFAULT_COMPACT_MIN);
    params.flushSize(flushSize).flushPerChanges(flushPerChanges).flushIntervalMs(flushIntervalMs)
      .compactMin(compactMin);
    int maxWals = conf.getInt(MAX_WALS_KEY, DEFAULT_MAX_WALS);
    params.maxWals(maxWals);
    if (conf.get(USE_HSYNC_KEY) != null) {
      params.useHsync(conf.getBoolean(USE_HSYNC_KEY, false));
    }
    params.ringBufferSlotCount(conf.getInt(RING_BUFFER_SLOT_COUNT, DEFAULT_RING_BUFFER_SLOT_COUNT));
    long rollPeriodMs = conf.getLong(ROLL_PERIOD_MS_KEY, DEFAULT_ROLL_PERIOD_MS);
    params.rollPeriodMs(rollPeriodMs).archivedWalSuffix(ARCHIVED_WAL_SUFFIX)
      .archivedHFileSuffix(ARCHIVED_HFILE_SUFFIX);
    return MasterRegion.create(params);
  }
}
