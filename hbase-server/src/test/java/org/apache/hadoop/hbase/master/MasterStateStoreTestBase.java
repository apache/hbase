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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.master.region.MasterRegionParams;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class MasterStateStoreTestBase {

  protected static HBaseZKTestingUtil UTIL = new HBaseZKTestingUtil();

  protected static MasterRegion REGION;

  protected static ChoreService CHORE_SERVICE;

  protected static DirScanPool HFILE_CLEANER_POOL;

  protected static DirScanPool LOG_CLEANER_POOL;

  protected static TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("test:local"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(MasterRegionFactory.STATE_FAMILY)).build();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    CHORE_SERVICE = new ChoreService("TestMasterStateStore");
    HFILE_CLEANER_POOL = DirScanPool.getHFileCleanerScanPool(conf);
    LOG_CLEANER_POOL = DirScanPool.getLogCleanerScanPool(conf);
    MasterServices server = mock(MasterServices.class);
    when(server.getConfiguration()).thenReturn(conf);
    when(server.getServerName())
      .thenReturn(ServerName.valueOf("localhost", 12345, EnvironmentEdgeManager.currentTime()));
    when(server.getChoreService()).thenReturn(CHORE_SERVICE);
    Path testDir = UTIL.getDataTestDir();
    CommonFSUtils.setRootDir(conf, testDir);
    MasterRegionParams params = new MasterRegionParams();
    TableDescriptor td = TableDescriptorBuilder
      .newBuilder(TD).setValue(StoreFileTrackerFactory.TRACKER_IMPL, conf
        .get(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()))
      .build();
    params.server(server).regionDirName("local").tableDescriptor(td)
      .flushSize(TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE).flushPerChanges(1_000_000)
      .flushIntervalMs(TimeUnit.MINUTES.toMillis(15)).compactMin(4).maxWals(32).useHsync(false)
      .ringBufferSlotCount(16).rollPeriodMs(TimeUnit.MINUTES.toMillis(15))
      .archivedWalSuffix(MasterRegionFactory.ARCHIVED_WAL_SUFFIX)
      .archivedHFileSuffix(MasterRegionFactory.ARCHIVED_HFILE_SUFFIX);
    REGION = MasterRegion.create(params);
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    REGION.close(true);
    HFILE_CLEANER_POOL.shutdownNow();
    LOG_CLEANER_POOL.shutdownNow();
    CHORE_SERVICE.shutdown();
    UTIL.shutdownMiniZKCluster();
    UTIL.cleanupTestDir();
  }

  protected static void cleanup() throws IOException {
    try (ResultScanner scanner = REGION.getScanner(new Scan())) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        REGION.update(r -> r.delete(new Delete(result.getRow())));
      }
    }
  }
}
