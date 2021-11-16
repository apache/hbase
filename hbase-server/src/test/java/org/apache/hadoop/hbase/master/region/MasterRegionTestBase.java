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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;

public class MasterRegionTestBase {

  protected HBaseCommonTestingUtility htu;

  protected MasterRegion region;

  protected ChoreService choreService;

  protected DirScanPool cleanerPool;

  protected static byte[] CF1 = Bytes.toBytes("f1");

  protected static byte[] CF2 = Bytes.toBytes("f2");

  protected static byte[] QUALIFIER = Bytes.toBytes("q");

  protected static String REGION_DIR_NAME = "local";

  protected static TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("test:local"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF1))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF2)).build();

  protected void configure(Configuration conf) throws IOException {
  }

  protected void configure(MasterRegionParams params) {
  }

  protected void postSetUp() throws IOException {
  }

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    htu.getConfiguration().setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    htu.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);

    createMasterRegion();
  }

  /**
   * Creates a new MasterRegion using an existing {@code htu} on this class.
   */
  protected void createMasterRegion() throws IOException {
    configure(htu.getConfiguration());
    choreService = new ChoreService(getClass().getSimpleName());
    cleanerPool = new DirScanPool(htu.getConfiguration());
    Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(htu.getConfiguration());
    when(server.getServerName())
      .thenReturn(ServerName.valueOf("localhost", 12345, System.currentTimeMillis()));
    when(server.getChoreService()).thenReturn(choreService);
    Path testDir = htu.getDataTestDir();
    CommonFSUtils.setRootDir(htu.getConfiguration(), testDir);
    MasterRegionParams params = new MasterRegionParams();
    params.server(server).regionDirName(REGION_DIR_NAME).tableDescriptor(TD)
      .flushSize(TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE).flushPerChanges(1_000_000)
      .flushIntervalMs(TimeUnit.MINUTES.toMillis(15)).compactMin(4).maxWals(32).useHsync(false)
      .ringBufferSlotCount(16).rollPeriodMs(TimeUnit.MINUTES.toMillis(15))
      .archivedWalSuffix(MasterRegionFactory.ARCHIVED_WAL_SUFFIX)
      .archivedHFileSuffix(MasterRegionFactory.ARCHIVED_HFILE_SUFFIX);
    configure(params);
    region = MasterRegion.create(params);
    postSetUp();
  }

  @After
  public void tearDown() throws IOException {
    region.close(true);
    cleanerPool.shutdownNow();
    choreService.shutdown();
    htu.cleanupTestDir();
  }
}
