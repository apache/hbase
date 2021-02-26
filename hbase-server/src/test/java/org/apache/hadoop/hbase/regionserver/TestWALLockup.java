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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.DamagedWALException;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
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

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Testing for lock up of FSHLog.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestWALLockup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALLockup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALLockup.class);

  @Rule
  public TestName name = new TestName();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

  HRegion region = null;
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration CONF ;
  private String dir;

  // Test names
  protected TableName tableName;

  @Before
  public void setup() throws IOException {
    CONF = TEST_UTIL.getConfiguration();
    // Disable block cache.
    CONF.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    dir = TEST_UTIL.getDataTestDir("TestHRegion").toString();
    tableName = TableName.valueOf(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  private String getName() {
    return name.getMethodName();
  }

  // A WAL that we can have throw exceptions when a flag is set.
  private static final class DodgyFSLog extends FSHLog {
    // Set this when want the WAL to start throwing exceptions.
    volatile boolean throwException = false;

    // Latch to hold up processing until after another operation has had time to run.
    CountDownLatch latch = new CountDownLatch(1);

    public DodgyFSLog(FileSystem fs, Path root, String logDir, Configuration conf)
        throws IOException {
      super(fs, root, logDir, conf);
    }

    @Override
    protected void afterCreatingZigZagLatch() {
      // If throwException set, then append will throw an exception causing the WAL to be
      // rolled. We'll come in here. Hold up processing until a sync can get in before
      // the zigzag has time to complete its setup and get its own sync in. This is what causes
      // the lock up we've seen in production.
      if (throwException) {
        try {
          LOG.info("LATCHED");
          // So, timing can have it that the test can run and the bad flush below happens
          // before we get here. In this case, we'll be stuck waiting on this latch but there
          // is nothing in the WAL pipeline to get us to the below beforeWaitOnSafePoint...
          // because all WALs have rolled. In this case, just give up on test.
          if (!this.latch.await(5, TimeUnit.SECONDS)) {
            LOG.warn("GIVE UP! Failed waiting on latch...Test is ABORTED!");
          }
        } catch (InterruptedException e) {
        }
      }
    }

    @Override
    protected void beforeWaitOnSafePoint() {
      if (throwException) {
        LOG.info("COUNTDOWN");
        // Don't countdown latch until someone waiting on it otherwise, the above
        // afterCreatingZigZagLatch will get to the latch and no one will ever free it and we'll
        // be stuck; test won't go down
        while (this.latch.getCount() <= 0)
          Threads.sleep(1);
        this.latch.countDown();
      }
    }

    @Override
    protected Writer createWriterInstance(Path path) throws IOException {
      final Writer w = super.createWriterInstance(path);
      return new Writer() {
        @Override
        public void close() throws IOException {
          w.close();
        }

        @Override
        public void sync(boolean forceSync) throws IOException {
          if (throwException) {
            throw new IOException("FAKE! Failed to replace a bad datanode...SYNC");
          }
          w.sync(forceSync);
        }

        @Override
        public void append(Entry entry) throws IOException {
          if (throwException) {
            throw new IOException("FAKE! Failed to replace a bad datanode...APPEND");
          }
          w.append(entry);
        }

        @Override
        public long getLength() {
          return w.getLength();
        }

        @Override
        public long getSyncedLength() {
          return w.getSyncedLength();
        }
      };
    }
  }

  /**
   * Reproduce locking up that happens when we get an inopportune sync during setup for
   * zigzaglatch wait. See HBASE-14317. If below is broken, we will see this test timeout because
   * it is locked up.
   * <p>First I need to set up some mocks for Server and RegionServerServices. I also need to
   * set up a dodgy WAL that will throw an exception when we go to append to it.
   */
  @Test
  public void testLockupWhenSyncInMiddleOfZigZagSetup() throws IOException {
    // Mocked up server and regionserver services. Needed below.
    RegionServerServices services = Mockito.mock(RegionServerServices.class);
    Mockito.when(services.getConfiguration()).thenReturn(CONF);
    Mockito.when(services.isStopped()).thenReturn(false);
    Mockito.when(services.isAborted()).thenReturn(false);

    // OK. Now I have my mocked up Server & RegionServerServices and dodgy WAL, go ahead with test.
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    DodgyFSLog dodgyWAL = new DodgyFSLog(fs, rootDir, getName(), CONF);
    dodgyWAL.init();
    Path originalWAL = dodgyWAL.getCurrentFileName();
    // I need a log roller running.
    LogRoller logRoller = new LogRoller(services);
    logRoller.addWAL(dodgyWAL);
    // There is no 'stop' once a logRoller is running.. it just dies.
    logRoller.start();
    // Now get a region and start adding in edits.
    final HRegion region = initHRegion(tableName, null, null, CONF, dodgyWAL);
    byte [] bytes = Bytes.toBytes(getName());
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(
        Bytes.BYTES_COMPARATOR);
    scopes.put(COLUMN_FAMILY_BYTES, 0);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    try {
      // First get something into memstore. Make a Put and then pull the Cell out of it. Will
      // manage append and sync carefully in below to manufacture hang. We keep adding same
      // edit. WAL subsystem doesn't care.
      Put put = new Put(bytes);
      put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);
      WALKeyImpl key = new WALKeyImpl(region.getRegionInfo().getEncodedNameAsBytes(),
        TableName.META_TABLE_NAME, System.currentTimeMillis(), mvcc, scopes);
      WALEdit edit = new WALEdit();
      CellScanner CellScanner = put.cellScanner();
      assertTrue(CellScanner.advance());
      edit.add(CellScanner.current());
      // Put something in memstore and out in the WAL. Do a big number of appends so we push
      // out other side of the ringbuffer. If small numbers, stuff doesn't make it to WAL
      for (int i = 0; i < 1000; i++) {
        region.put(put);
      }
      // Set it so we start throwing exceptions.
      LOG.info("SET throwing of exception on append");
      dodgyWAL.throwException = true;
      // This append provokes a WAL roll request
      dodgyWAL.appendData(region.getRegionInfo(), key, edit);
      boolean exception = false;
      try {
        dodgyWAL.sync(false);
      } catch (Exception e) {
        exception = true;
      }
      assertTrue("Did not get sync exception", exception);

      // Get a memstore flush going too so we have same hung profile as up in the issue over
      // in HBASE-14317. Flush hangs trying to get sequenceid because the ringbuffer is held up
      // by the zigzaglatch waiting on syncs to come home.
      Thread t = new Thread ("Flusher") {
        @Override
        public void run() {
          try {
            if (region.getMemStoreDataSize() <= 0) {
              throw new IOException("memstore size=" + region.getMemStoreDataSize());
            }
            region.flush(false);
          } catch (IOException e) {
            // Can fail trying to flush in middle of a roll. Not a failure. Will succeed later
            // when roll completes.
            LOG.info("In flush", e);
          }
          LOG.info("Exiting");
        };
      };
      t.setDaemon(true);
      t.start();
      // Wait until
      while (dodgyWAL.latch.getCount() > 0) {
        Threads.sleep(1);
      }
      // Now assert I got a new WAL file put in place even though loads of errors above.
      assertTrue(originalWAL != dodgyWAL.getCurrentFileName());
      // Can I append to it?
      dodgyWAL.throwException = false;
      try {
        region.put(put);
      } catch (Exception e) {
        LOG.info("In the put", e);
      }
    } finally {
      // To stop logRoller, its server has to say it is stopped.
      Mockito.when(services.isStopped()).thenReturn(true);
      Closeables.close(logRoller, true);
      try {
        if (region != null) {
          region.close();
        }
        if (dodgyWAL != null) {
          dodgyWAL.close();
        }
      } catch (Exception e) {
        LOG.info("On way out", e);
      }
    }
  }

  /**
   *
   * If below is broken, we will see this test timeout because RingBufferEventHandler was stuck in
   * attainSafePoint. Everyone will wait for sync to finish forever. See HBASE-14317.
   */
  @Test
  public void testRingBufferEventHandlerStuckWhenSyncFailed()
    throws IOException, InterruptedException {

    // A WAL that we can have throw exceptions and slow FSHLog.replaceWriter down
    class DodgyFSLog extends FSHLog {

      private volatile boolean zigZagCreated = false;

      public DodgyFSLog(FileSystem fs, Path root, String logDir, Configuration conf)
        throws IOException {
        super(fs, root, logDir, conf);
      }

      @Override
      protected void afterCreatingZigZagLatch() {
        zigZagCreated = true;
        // Sleep a while to wait for RingBufferEventHandler to get stuck first.
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ignore) {
        }
      }

      @Override
      protected long getSequenceOnRingBuffer() {
        return super.getSequenceOnRingBuffer();
      }

      protected void publishSyncOnRingBufferAndBlock(long sequence) {
        try {
          super.blockOnSync(super.publishSyncOnRingBuffer(sequence, false));
          Assert.fail("Expect an IOException here.");
        } catch (IOException ignore) {
          // Here, we will get an IOException.
        }
      }

      @Override
      protected Writer createWriterInstance(Path path) throws IOException {
        final Writer w = super.createWriterInstance(path);
        return new Writer() {
          @Override
          public void close() throws IOException {
            w.close();
          }

          @Override
          public void sync(boolean forceSync) throws IOException {
            throw new IOException("FAKE! Failed to replace a bad datanode...SYNC");
          }

          @Override
          public void append(Entry entry) throws IOException {
            w.append(entry);
          }

          @Override
          public long getLength() {
            return w.getLength();
          }

          @Override
          public long getSyncedLength() {
            return w.getSyncedLength();
          }
        };
      }
    }

    // Mocked up server and regionserver services. Needed below.
    RegionServerServices services = Mockito.mock(RegionServerServices.class);
    Mockito.when(services.getConfiguration()).thenReturn(CONF);
    Mockito.when(services.isStopped()).thenReturn(false);
    Mockito.when(services.isAborted()).thenReturn(false);

    // OK. Now I have my mocked up Server & RegionServerServices and dodgy WAL, go ahead with test.
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    final DodgyFSLog dodgyWAL = new DodgyFSLog(fs, rootDir, getName(), CONF);
    dodgyWAL.init();
    // I need a log roller running.
    LogRoller logRoller = new LogRoller(services);
    logRoller.addWAL(dodgyWAL);
    // There is no 'stop' once a logRoller is running.. it just dies.
    logRoller.start();

    try {
      final long seqForSync = dodgyWAL.getSequenceOnRingBuffer();

      // This call provokes a WAL roll, and we will get a new RingBufferEventHandler.ZigZagLatch
      // in LogRoller.
      // After creating ZigZagLatch, RingBufferEventHandler would get stuck due to sync event,
      // as long as HBASE-14317 hasn't be fixed.
      LOG.info("Trigger log roll for creating a ZigZagLatch.");
      logRoller.requestRollAll();

      while (!dodgyWAL.zigZagCreated) {
        Thread.sleep(10);
      }

      // Send a sync event for RingBufferEventHandler,
      // and it gets blocked in RingBufferEventHandler.attainSafePoint
      LOG.info("Send sync for RingBufferEventHandler");
      Thread syncThread = new Thread() {
        @Override
        public void run() {
          dodgyWAL.publishSyncOnRingBufferAndBlock(seqForSync);
        }
      };
      // Sync in another thread to avoid reset SyncFuture again.
      syncThread.start();
      syncThread.join();

      try {
        LOG.info("Call sync for testing whether RingBufferEventHandler is hanging.");
        dodgyWAL.sync(false); // Should not get a hang here, otherwise we will see timeout in this test.
        Assert.fail("Expect an IOException here.");
      } catch (IOException ignore) {
      }

    } finally {
      // To stop logRoller, its server has to say it is stopped.
      Mockito.when(services.isStopped()).thenReturn(true);
      if (logRoller != null) {
        logRoller.close();
      }
      if (dodgyWAL != null) {
        dodgyWAL.close();
      }
    }
  }


  static class DummyServer implements Server {
    private Configuration conf;
    private String serverName;
    private boolean isAborted = false;

    public DummyServer(Configuration conf, String serverName) {
      this.conf = conf;
      this.serverName = serverName;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(this.serverName);
    }

    @Override
    public void abort(String why, Throwable e) {
      LOG.info("Aborting " + serverName);
      this.isAborted = true;
    }

    @Override
    public boolean isAborted() {
      return this.isAborted;
    }

    @Override
    public void stop(String why) {
      this.isAborted = true;
    }

    @Override
    public boolean isStopped() {
      return this.isAborted;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }
  }

  static class DummyWALActionsListener implements WALActionsListener {

    @Override
    public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit)
        throws IOException {
      if (logKey.getTableName().getNameAsString().equalsIgnoreCase("sleep")) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if (logKey.getTableName().getNameAsString()
          .equalsIgnoreCase("DamagedWALException")) {
        throw new DamagedWALException("Failed appending");
      }
    }

  }

  /**
   * @return A region on which you must call {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)}
   *         when done.
   */
  private static HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      Configuration conf, WAL wal) throws IOException {
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, conf, false,
      Durability.SYNC_WAL, wal, COLUMN_FAMILY_BYTES);
  }
}
