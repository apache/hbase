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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLABImpl;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Provides FSHLog test cases.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLog extends AbstractTestFSWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSHLog.class);

  @Rule
  public TestName name = new TestName();

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String walDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    FSHLog wal =
      new FSHLog(fs, rootDir, walDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    wal.init();
    return wal;
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String walDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    FSHLog wal = new FSHLog(fs, rootDir, walDir, archiveDir, conf, listeners, failIfWALExists,
        prefix, suffix) {

      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    wal.init();
    return wal;
  }

  @Test
  public void testSyncRunnerIndexOverflow() throws IOException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    final String name = this.name.getMethodName();
    FSHLog log = new FSHLog(FS, FSUtils.getRootDir(CONF), name, HConstants.HREGION_OLDLOGDIR_NAME,
      CONF, null, true, null, null);
    log.init();
    try {
      Field ringBufferEventHandlerField = FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
          (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      Field syncRunnerIndexField =
          FSHLog.RingBufferEventHandler.class.getDeclaredField("syncRunnerIndex");
      syncRunnerIndexField.setAccessible(true);
      syncRunnerIndexField.set(ringBufferEventHandler, Integer.MAX_VALUE - 1);
      TableDescriptor htd =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(this.name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
      NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (byte[] fam : htd.getColumnFamilyNames()) {
        scopes.put(fam, 0);
      }
      RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
      MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
      for (int i = 0; i < 10; i++) {
        addEdits(log, hri, htd, 1, mvcc, scopes);
      }
    } finally {
      log.close();
    }
  }

  /**
   * Test case for https://issues.apache.org/jira/browse/HBASE-16721
   */
  @Test
  public void testUnflushedSeqIdTracking() throws IOException, InterruptedException {
    final String name = this.name.getMethodName();
    final byte[] b = Bytes.toBytes("b");

    final AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    final CountDownLatch holdAppend = new CountDownLatch(1);
    final CountDownLatch flushFinished = new CountDownLatch(1);
    final CountDownLatch putFinished = new CountDownLatch(1);

    try (FSHLog log =
        new FSHLog(FS, FSUtils.getRootDir(CONF), name, HConstants.HREGION_OLDLOGDIR_NAME, CONF,
            null, true, null, null)) {
      log.init();
      log.registerWALActionsListener(new WALActionsListener() {
        @Override
        public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit)
            throws IOException {
          if (startHoldingForAppend.get()) {
            try {
              holdAppend.await();
            } catch (InterruptedException e) {
              LOG.error(e.toString(), e);
            }
          }
        }
      });

      // open a new region which uses this WAL
      TableDescriptor htd =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(this.name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(b)).build();
      RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
      ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null);
      final HRegion region = TEST_UTIL.createLocalHRegion(hri, htd, log);
      ExecutorService exec = Executors.newFixedThreadPool(2);

      // do a regular write first because of memstore size calculation.
      region.put(new Put(b).addColumn(b, b,b));

      startHoldingForAppend.set(true);
      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            region.put(new Put(b).addColumn(b, b,b));
            putFinished.countDown();
          } catch (IOException e) {
            LOG.error(e.toString(), e);
          }
        }
      });

      // give the put a chance to start
      Threads.sleep(3000);

      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            HRegion.FlushResult flushResult = region.flush(true);
            LOG.info("Flush result:" +  flushResult.getResult());
            LOG.info("Flush succeeded:" +  flushResult.isFlushSucceeded());
            flushFinished.countDown();
          } catch (IOException e) {
            LOG.error(e.toString(), e);
          }
        }
      });

      // give the flush a chance to start. Flush should have got the region lock, and
      // should have been waiting on the mvcc complete after this.
      Threads.sleep(3000);

      // let the append to WAL go through now that the flush already started
      holdAppend.countDown();
      putFinished.await();
      flushFinished.await();

      // check whether flush went through
      assertEquals("Region did not flush?", 1, region.getStoreFileList(new byte[][]{b}).size());

      // now check the region's unflushed seqIds.
      long seqId = log.getEarliestMemStoreSeqNum(hri.getEncodedNameAsBytes());
      assertEquals("Found seqId for the region which is already flushed",
          HConstants.NO_SEQNUM, seqId);

      region.close();
    }
  }
}
