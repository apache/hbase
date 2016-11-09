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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.exceptions.verification.WantedButNotInvoked;

/**
 * Testing sync/append failures.
 * Copied from TestHRegion.
 */
@Category({MediumTests.class})
public class TestFailedAppendAndSync {
  private static final Log LOG = LogFactory.getLog(TestFailedAppendAndSync.class);
  @Rule public TestName name = new TestName();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

  HRegion region = null;
  // Do not run unit tests in parallel (? Why not?  It don't work?  Why not?  St.Ack)
  private static HBaseTestingUtility TEST_UTIL;
  public static Configuration CONF ;
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
   * Reproduce locking up that happens when we get an exceptions appending and syncing.
   * See HBASE-14317.
   * First I need to set up some mocks for Server and RegionServerServices. I also need to
   * set up a dodgy WAL that will throw an exception when we go to append to it.
   */
  @Test (timeout=300000)
  public void testLockupAroundBadAssignSync() throws IOException {
    final AtomicLong rolls = new AtomicLong(0);
    // Dodgy WAL. Will throw exceptions when flags set.
    class DodgyFSLog extends FSHLog {
      volatile boolean throwSyncException = false;
      volatile boolean throwAppendException = false;

      public DodgyFSLog(FileSystem fs, Path root, String logDir, Configuration conf)
      throws IOException {
        super(fs, root, logDir, conf);
      }

      @Override
      public byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
        byte [][] regions = super.rollWriter(force);
        rolls.getAndIncrement();
        return regions;
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
              if (throwSyncException) {
                throw new IOException("FAKE! Failed to replace a bad datanode...");
              }
              w.sync();
            }

            @Override
            public void append(Entry entry) throws IOException {
              if (throwAppendException) {
                throw new IOException("FAKE! Failed to replace a bad datanode...");
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

    // Make up mocked server and services.
    Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(CONF);
    when(server.isStopped()).thenReturn(false);
    when(server.isAborted()).thenReturn(false);
    RegionServerServices services = mock(RegionServerServices.class);
    // OK. Now I have my mocked up Server and RegionServerServices and my dodgy WAL, go ahead with
    // the test.
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    DodgyFSLog dodgyWAL = new DodgyFSLog(fs, rootDir, getName(), CONF);
    LogRoller logRoller = new LogRoller(server, services);
    logRoller.addWAL(dodgyWAL);
    logRoller.start();

    boolean threwOnSync = false;
    boolean threwOnAppend = false;
    boolean threwOnBoth = false;

    HRegion region = initHRegion(tableName, null, null, dodgyWAL);
    try {
      // Get some random bytes.
      byte[] value = Bytes.toBytes(getName());
      try {
        // First get something into memstore
        Put put = new Put(value);
        put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), value);
        region.put(put);
      } catch (IOException ioe) {
        fail();
      }
      long rollsCount = rolls.get();
      try {
        dodgyWAL.throwAppendException = true;
        dodgyWAL.throwSyncException = false;
        Put put = new Put(value);
        put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("3"), value);
        region.put(put);
      } catch (IOException ioe) {
        threwOnAppend = true;
      }
      while (rollsCount == rolls.get()) Threads.sleep(100);
      rollsCount = rolls.get();

      // When we get to here.. we should be ok. A new WAL has been put in place. There were no
      // appends to sync. We should be able to continue.

      try {
        dodgyWAL.throwAppendException = true;
        dodgyWAL.throwSyncException = true;
        Put put = new Put(value);
        put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("4"), value);
        region.put(put);
      } catch (IOException ioe) {
        threwOnBoth = true;
      }
      while (rollsCount == rolls.get()) Threads.sleep(100);

      // Again, all should be good. New WAL and no outstanding unsync'd edits so we should be able
      // to just continue.

      // So, should be no abort at this stage. Verify.
      Mockito.verify(server, Mockito.atLeast(0)).
        abort(Mockito.anyString(), (Throwable)Mockito.anyObject());
      try {
        dodgyWAL.throwAppendException = false;
        dodgyWAL.throwSyncException = true;
        Put put = new Put(value);
        put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("2"), value);
        region.put(put);
      } catch (IOException ioe) {
        threwOnSync = true;
      }
      // An append in the WAL but the sync failed is a server abort condition. That is our
      // current semantic. Verify. It takes a while for abort to be called. Just hang here till it
      // happens. If it don't we'll timeout the whole test. That is fine.
      while (true) {
        try {
          Mockito.verify(server, Mockito.atLeast(1)).
            abort(Mockito.anyString(), (Throwable)Mockito.anyObject());
          break;
        } catch (WantedButNotInvoked t) {
          Threads.sleep(1);
        }
      }
    } finally {
      // To stop logRoller, its server has to say it is stopped.
      Mockito.when(server.isStopped()).thenReturn(true);
      if (logRoller != null) logRoller.close();
      if (region != null) {
        try {
          region.close(true);
        } catch (DroppedSnapshotException e) {
          LOG.info("On way out; expected!", e);
        }
      }
      if (dodgyWAL != null) dodgyWAL.close();
      assertTrue("The regionserver should have thrown an exception", threwOnBoth);
      assertTrue("The regionserver should have thrown an exception", threwOnAppend);
      assertTrue("The regionserver should have thrown an exception", threwOnSync);
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