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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ VerySlowRegionServerTests.class, LargeTests.class })
public class TestAsyncLogRolling extends AbstractTestLogRolling {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncLogRolling.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TestAsyncLogRolling.TEST_UTIL.getConfiguration();
    conf.setInt(FanOutOneBlockAsyncDFSOutputHelper.ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES, 100);
    conf.set(WALFactory.WAL_PROVIDER, "asyncfs");
    AbstractTestLogRolling.setUpBeforeClass();

    // For slow sync threshold test: roll once after a sync above this threshold
    TEST_UTIL.getConfiguration().setInt(FSHLog.ROLL_ON_SYNC_TIME_MS, 5000);
  }

  @Test
  public void testSlowSyncLogRolling() throws Exception {
    // Create the test table
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());
    int row = 1;
    try {
      // Get a reference to the AsyncFSWAL
      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      RegionInfo region = server.getRegions(desc.getTableName()).get(0).getRegionInfo();
      final AsyncFSWAL log = (AsyncFSWAL) server.getWAL(region);

      // Register a WALActionsListener to observe if a SLOW_SYNC roll is requested

      final AtomicBoolean slowSyncHookCalled = new AtomicBoolean();
      log.registerWALActionsListener(new WALActionsListener() {
        @Override
        public void logRollRequested(WALActionsListener.RollRequestReason reason) {
          switch (reason) {
            case SLOW_SYNC:
              slowSyncHookCalled.lazySet(true);
              break;
            default:
              break;
          }
        }
      });

      // Write some data

      for (int i = 0; i < 10; i++) {
        writeData(table, row++);
      }

      assertFalse("Should not have triggered log roll due to SLOW_SYNC", slowSyncHookCalled.get());

      // Set up for test
      slowSyncHookCalled.set(false);

      // Wrap the current writer with the anonymous class below that adds 5000 ms of
      // latency to any sync on the hlog.
      // This will trip the other threshold.
      final WALProvider.AsyncWriter oldWriter2 = log.getWriter();
      final WALProvider.AsyncWriter newWriter2 = new WALProvider.AsyncWriter() {
        @Override
        public void close() throws IOException {
          oldWriter2.close();
        }

        @Override
        public CompletableFuture<Long> sync(boolean forceSync) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return oldWriter2.sync(forceSync);
        }

        @Override
        public void append(WAL.Entry entry) {
          oldWriter2.append(entry);
        }

        @Override
        public long getLength() {
          return oldWriter2.getLength();
        }

        @Override
        public long getSyncedLength() {
          return oldWriter2.getSyncedLength();
        }
      };
      log.setWriter(newWriter2);

      // Write some data. Should only take one sync.

      writeData(table, row++);

      // Wait for our wait injecting writer to get rolled out, as needed.

      TEST_UTIL.waitFor(10000, 100, new Waiter.ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return log.getWriter() != newWriter2;
        }

        @Override
        public String explainFailure() throws Exception {
          return "Waited too long for our test writer to get rolled out";
        }
      });

      assertTrue("Should have triggered log roll due to SLOW_SYNC", slowSyncHookCalled.get());

      // Set up for test
      slowSyncHookCalled.set(false);

      // Write some data
      for (int i = 0; i < 10; i++) {
        writeData(table, row++);
      }

      assertFalse("Should not have triggered log roll due to SLOW_SYNC", slowSyncHookCalled.get());

    } finally {
      table.close();
    }
  }

  @Test
  public void testLogRollOnDatanodeDeath() throws IOException, InterruptedException {
    dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), 3, true, null, null);
    tableName = getName();
    Table table = createTestTable(tableName);
    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());
    doPut(table, 1);
    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    RegionInfo hri = server.getRegions(table.getName()).get(0).getRegionInfo();
    AsyncFSWAL wal = (AsyncFSWAL) server.getWAL(hri);
    int numRolledLogFiles = AsyncFSWALProvider.getNumRolledLogFiles(wal);
    DatanodeInfo[] dnInfos = wal.getPipeline();
    DataNodeProperties dnProp = TEST_UTIL.getDFSCluster().stopDataNode(dnInfos[0].getName());
    TEST_UTIL.getDFSCluster().restartDataNode(dnProp);
    doPut(table, 2);
    assertEquals(numRolledLogFiles + 1, AsyncFSWALProvider.getNumRolledLogFiles(wal));
  }
}
