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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.DamagedWALException;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * Testing for lock up of WAL subsystem.
 * Copied from TestHRegion.
 */
@Category({MediumTests.class})
public class TestWALLockup {
  private static final Log LOG = LogFactory.getLog(TestWALLockup.class);
  @Rule public TestName name = new TestName();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

  HRegion region = null;
  // Do not run unit tests in parallel (? Why not?  It don't work?  Why not?  St.Ack)
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration CONF ;
  private String dir;

  // Test names
  protected TableName tableName;

  @Before
  public void setup() throws IOException {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
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

  String getName() {
    return name.getMethodName();
  }

  /**
   * Reproduce locking up that happens when we get an inopportune sync during setup for
   * zigzaglatch wait. See HBASE-14317. If below is broken, we will see this test timeout because
   * it is locked up.
   * <p>First I need to set up some mocks for Server and RegionServerServices. I also need to
   * set up a dodgy WAL that will throw an exception when we go to append to it.
   */
  @Test (timeout=20000)
  public void testLockupWhenSyncInMiddleOfZigZagSetup() throws IOException {
    // A WAL that we can have throw exceptions when a flag is set.
    class DodgyFSLog extends FSHLog {
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
            // TODO Auto-generated catch block
            e.printStackTrace();
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
          while (this.latch.getCount() <= 0) Threads.sleep(1);
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
          public void sync() throws IOException {
            if (throwException) {
              throw new IOException("FAKE! Failed to replace a bad datanode...SYNC");
            }
            w.sync();
          }

          @Override
          public void append(Entry entry) throws IOException {
            if (throwException) {
              throw new IOException("FAKE! Failed to replace a bad datanode...APPEND");
            }
            w.append(entry);
          }

          @Override
          public long getLength() throws IOException {
            return w.getLength();
          }
        };
      }
    }

    // Mocked up server and regionserver services. Needed below.
    Server server = Mockito.mock(Server.class);
    Mockito.when(server.getConfiguration()).thenReturn(CONF);
    Mockito.when(server.isStopped()).thenReturn(false);
    Mockito.when(server.isAborted()).thenReturn(false);
    RegionServerServices services = Mockito.mock(RegionServerServices.class);

    // OK. Now I have my mocked up Server & RegionServerServices and dodgy WAL, go ahead with test.
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    DodgyFSLog dodgyWAL = new DodgyFSLog(fs, rootDir, getName(), CONF);
    Path originalWAL = dodgyWAL.getCurrentFileName();
    // I need a log roller running.
    LogRoller logRoller = new LogRoller(server, services);
    logRoller.addWAL(dodgyWAL);
    // There is no 'stop' once a logRoller is running.. it just dies.
    logRoller.start();
    // Now get a region and start adding in edits.
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    final HRegion region = initHRegion(tableName, null, null, dodgyWAL);
    byte [] bytes = Bytes.toBytes(getName());
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
        Bytes.BYTES_COMPARATOR);
    scopes.put(COLUMN_FAMILY_BYTES, 0);
    try {
      // First get something into memstore. Make a Put and then pull the Cell out of it. Will
      // manage append and sync carefully in below to manufacture hang. We keep adding same
      // edit. WAL subsystem doesn't care.
      Put put = new Put(bytes);
      put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);
      WALKey key = new WALKey(region.getRegionInfo().getEncodedNameAsBytes(), htd.getTableName(),
          scopes);
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
      dodgyWAL.append(region.getRegionInfo(), key, edit, true);
      boolean exception = false;
      try {
        dodgyWAL.sync();
      } catch (Exception e) {
        exception = true;
      }
      assertTrue("Did not get sync exception", exception);

      // Get a memstore flush going too so we have same hung profile as up in the issue over
      // in HBASE-14317. Flush hangs trying to get sequenceid because the ringbuffer is held up
      // by the zigzaglatch waiting on syncs to come home.
      Thread t = new Thread ("Flusher") {
        public void run() {
          try {
            if (region.getMemstoreSize() <= 0) {
              throw new IOException("memstore size=" + region.getMemstoreSize());
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
      while (dodgyWAL.latch.getCount() > 0) Threads.sleep(1);
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
      Mockito.when(server.isStopped()).thenReturn(true);
      if (logRoller != null) logRoller.close();
      try {
        if (region != null) region.close();
        if (dodgyWAL != null) dodgyWAL.close();
      } catch (Exception e) {
        LOG.info("On way out", e);
      }
    }
  }

  /**
   * Reproduce locking up that happens when there's no further syncs after
   * append fails, and causing an isolated sync then infinite wait. See
   * HBASE-16960. If below is broken, we will see this test timeout because it
   * is locked up.
   * <p/>
   * Steps for reproduce:<br/>
   * 1. Trigger server abort through dodgyWAL1<br/>
   * 2. Add a {@link DummyWALActionsListener} to dodgyWAL2 to cause ringbuffer
   * event handler thread sleep for a while thus keeping {@code endOfBatch}
   * false<br/>
   * 3. Publish a sync then an append which will throw exception, check whether
   * the sync could return
   */
  @Test(timeout = 20000)
  public void testLockup16960() throws IOException {
    // A WAL that we can have throw exceptions when a flag is set.
    class DodgyFSLog extends FSHLog {
      // Set this when want the WAL to start throwing exceptions.
      volatile boolean throwException = false;

      public DodgyFSLog(FileSystem fs, Path root, String logDir,
          Configuration conf) throws IOException {
        super(fs, root, logDir, conf);
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
          public void sync() throws IOException {
            if (throwException) {
              throw new IOException(
                  "FAKE! Failed to replace a bad datanode...SYNC");
            }
            w.sync();
          }

          @Override
          public void append(Entry entry) throws IOException {
            if (throwException) {
              throw new IOException(
                  "FAKE! Failed to replace a bad datanode...APPEND");
            }
            w.append(entry);
          }

          @Override
          public long getLength() throws IOException {
            return w.getLength();
          }
        };
      }

      @Override
      protected long doReplaceWriter(Path oldPath, Path newPath,
          Writer nextWriter) throws IOException {
        if (throwException) {
          throw new FailedLogCloseException("oldPath=" + oldPath + ", newPath="
              + newPath);
        }
        long oldFileLen = 0L;
        oldFileLen = super.doReplaceWriter(oldPath, newPath, nextWriter);
        return oldFileLen;
      }
    }

    // Mocked up server and regionserver services. Needed below.
    Server server = new DummyServer(CONF, ServerName.valueOf(
        "hostname1.example.org", 1234, 1L).toString());
    RegionServerServices services = Mockito.mock(RegionServerServices.class);

    CONF.setLong("hbase.regionserver.hlog.sync.timeout", 10000);

    // OK. Now I have my mocked up Server & RegionServerServices and dodgy WAL,
    // go ahead with test.
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    DodgyFSLog dodgyWAL1 = new DodgyFSLog(fs, rootDir, getName(), CONF);

    Path rootDir2 = new Path(dir + getName() + "2");
    final DodgyFSLog dodgyWAL2 = new DodgyFSLog(fs, rootDir2, getName() + "2",
        CONF);
    // Add a listener to force ringbuffer event handler sleep for a while
    dodgyWAL2.registerWALActionsListener(new DummyWALActionsListener());

    // I need a log roller running.
    LogRoller logRoller = new LogRoller(server, services);
    logRoller.addWAL(dodgyWAL1);
    logRoller.addWAL(dodgyWAL2);
    // There is no 'stop' once a logRoller is running.. it just dies.
    logRoller.start();
    // Now get a region and start adding in edits.
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    final HRegion region = initHRegion(tableName, null, null, dodgyWAL1);
    byte[] bytes = Bytes.toBytes(getName());
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
        Bytes.BYTES_COMPARATOR);
    scopes.put(COLUMN_FAMILY_BYTES, 0);
    try {
      Put put = new Put(bytes);
      put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);
      WALKey key = new WALKey(region.getRegionInfo().getEncodedNameAsBytes(),
          htd.getTableName(), scopes);
      WALEdit edit = new WALEdit();
      CellScanner CellScanner = put.cellScanner();
      assertTrue(CellScanner.advance());
      edit.add(CellScanner.current());

      LOG.info("SET throwing of exception on append");
      dodgyWAL1.throwException = true;
      // This append provokes a WAL roll request
      dodgyWAL1.append(region.getRegionInfo(), key, edit, true);
      boolean exception = false;
      try {
        dodgyWAL1.sync();
      } catch (Exception e) {
        exception = true;
      }
      assertTrue("Did not get sync exception", exception);

      // LogRoller call dodgyWAL1.rollWriter get FailedLogCloseException and
      // cause server abort.
      try {
        // wait LogRoller exit.
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      final CountDownLatch latch = new CountDownLatch(1);

      // make RingBufferEventHandler sleep 1s, so the following sync
      // endOfBatch=false
      key = new WALKey(region.getRegionInfo().getEncodedNameAsBytes(),
          TableName.valueOf("sleep"), scopes);
      dodgyWAL2.append(region.getRegionInfo(), key, edit, true);

      Thread t = new Thread("Sync") {
        public void run() {
          try {
            dodgyWAL2.sync();
          } catch (IOException e) {
            LOG.info("In sync", e);
          }
          latch.countDown();
          LOG.info("Sync exiting");
        };
      };
      t.setDaemon(true);
      t.start();
      try {
        // make sure sync have published.
        Thread.sleep(100);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      // make append throw DamagedWALException
      key = new WALKey(region.getRegionInfo().getEncodedNameAsBytes(),
          TableName.valueOf("DamagedWALException"), scopes);
      dodgyWAL2.append(region.getRegionInfo(), key, edit, true);

      while (latch.getCount() > 0) {
        Threads.sleep(100);
      }
      assertTrue(server.isAborted());
    } finally {
      if (logRoller != null) {
        logRoller.close();
      }
      try {
        if (region != null) {
          region.close();
        }
        if (dodgyWAL1 != null) {
          dodgyWAL1.close();
        }
        if (dodgyWAL2 != null) {
          dodgyWAL2.close();
        }
      } catch (Exception e) {
        LOG.info("On way out", e);
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
    public ZooKeeperWatcher getZooKeeper() {
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
    public MetaTableLocator getMetaTableLocator() {
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

  }

  static class DummyWALActionsListener extends WALActionsListener.Base {

    @Override
    public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit)
        throws IOException {
      if (logKey.getTablename().getNameAsString().equalsIgnoreCase("sleep")) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if (logKey.getTablename().getNameAsString()
          .equalsIgnoreCase("DamagedWALException")) {
        throw new DamagedWALException("Failed appending");
      }
    }

  }

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  public static HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey, WAL wal)
  throws IOException {
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, false, Durability.SYNC_WAL,
      wal, COLUMN_FAMILY_BYTES);
  }
}
