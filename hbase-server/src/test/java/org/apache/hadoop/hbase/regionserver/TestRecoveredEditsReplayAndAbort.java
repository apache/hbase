/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBASE-21031
 * If replay edits fails, we need to make sure memstore is rollbacked
 * And if MSLAB is used, all chunk is released too.
 */
@Category({RegionServerTests.class, SmallTests.class })
public class TestRecoveredEditsReplayAndAbort {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRecoveredEditsReplayAndAbort.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestRecoveredEditsReplayAndAbort.class);

  protected final byte[] row = Bytes.toBytes("rowA");

  protected final static byte [] fam1 = Bytes.toBytes("colfamily11");

  @Rule
  public TestName name = new TestName();

  // Test names
  protected TableName tableName;
  protected String method;

  protected static HBaseTestingUtility TEST_UTIL;
  public static Configuration CONF ;
  private static FileSystem FILESYSTEM;
  private HRegion region = null;

  @Before
  public void setup() throws IOException {
    TEST_UTIL = new HBaseTestingUtility();
    FILESYSTEM = TEST_UTIL.getTestFileSystem();
    CONF = TEST_UTIL.getConfiguration();
    method = name.getMethodName();
    tableName = TableName.valueOf(method);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void test() throws Exception {
    //set flush size to 10MB
    CONF.setInt("hbase.hregion.memstore.flush.size", 1024 * 1024 * 10);
    //set the report interval to a very small value
    CONF.setInt("hbase.hstore.report.interval.edits", 1);
    CONF.setInt("hbase.hstore.report.period", 0);
    //mock a RegionServerServices
    final RegionServerAccounting rsAccounting = new RegionServerAccounting(CONF);
    RegionServerServices rs = Mockito.mock(RegionServerServices.class);
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    Mockito.when(rs.getRegionServerAccounting()).thenReturn(rsAccounting);
    Mockito.when(rs.isAborted()).thenReturn(false);
    Mockito.when(rs.getNonceManager()).thenReturn(null);
    Mockito.when(rs.getServerName()).thenReturn(ServerName
        .valueOf("test", 0, 111));
    Mockito.when(rs.getConfiguration()).thenReturn(CONF);
    //create a region
    TableName testTable = TableName.valueOf("testRecoveredEidtsReplayAndAbort");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(testTable)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam1).build())
        .build();
    HRegionInfo info = new HRegionInfo(htd.getTableName(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, false);
    Path logDir = TEST_UTIL
        .getDataTestDirOnTestFS("TestRecoveredEidtsReplayAndAbort.log");
    final WAL wal = HBaseTestingUtility.createWal(CONF, logDir, info);
    Path rootDir = TEST_UTIL.getDataTestDir();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    HRegionFileSystem
        .createRegionOnFileSystem(CONF, TEST_UTIL.getTestFileSystem(), tableDir, info);
    region = HRegion.newHRegion(tableDir, wal, TEST_UTIL.getTestFileSystem(), CONF, info,
        htd, rs);
    //create some recovered.edits
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitUtil
          .getRegionDirRecoveredEditsDir(regiondir);
      long maxSeqId = 1200;
      long minSeqId = 1000;
      long totalEdits = maxSeqId - minSeqId;
      for (long i = minSeqId; i <= maxSeqId; i += 100) {
        Path recoveredEdits = new Path(recoveredEditsDir,
            String.format("%019d", i));
        LOG.info("Begin to write recovered.edits : " + recoveredEdits);
        fs.create(recoveredEdits);
        WALProvider.Writer writer = wals
            .createRecoveredEditsWriter(fs, recoveredEdits);
        for (long j = i; j < i + 100; j++) {
          long time = System.nanoTime();
          WALEdit edit = new WALEdit();
          // 200KB kv
          byte[] value = new byte[200 * 1024];
          Bytes.random(value);
          edit.add(
              new KeyValue(row, fam1, Bytes.toBytes(j), time, KeyValue.Type.Put,
                  value));
          writer.append(new WAL.Entry(
              new WALKeyImpl(regionName, tableName, j, time,
                  HConstants.DEFAULT_CLUSTER_ID), edit));
        }
        writer.close();
      }
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      //try to replay the edits
      try {
        region.initialize(new CancelableProgressable() {
          private long replayedEdits = 0;

          @Override
          public boolean progress() {
            replayedEdits++;
            //during replay, rsAccounting should align with global memstore, because
            //there is only one memstore here
            Assert.assertEquals(rsAccounting.getGlobalMemStoreDataSize(),
                region.getMemStoreDataSize());
            Assert.assertEquals(rsAccounting.getGlobalMemStoreHeapSize(),
                region.getMemStoreHeapSize());
            Assert.assertEquals(rsAccounting.getGlobalMemStoreOffHeapSize(),
                region.getMemStoreOffHeapSize());
            // abort the replay before finishing, leaving some edits in the memory
            return replayedEdits < totalEdits - 10;
          }
        });
        Assert.fail("Should not reach here");
      } catch (IOException t) {
        LOG.info("Current memstore: " + region.getMemStoreDataSize() + ", " + region
            .getMemStoreHeapSize() + ", " + region
            .getMemStoreOffHeapSize());
      }
      //After aborting replay, there should be no data in the memory
      Assert.assertEquals(0, rsAccounting.getGlobalMemStoreDataSize());
      Assert.assertEquals(0, region.getMemStoreDataSize());
      //All the chunk in the MSLAB should be recycled, otherwise, there might be
      //a memory leak.
      Assert.assertEquals(0, ChunkCreator.getInstance().numberOfMappedChunks());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }
}
