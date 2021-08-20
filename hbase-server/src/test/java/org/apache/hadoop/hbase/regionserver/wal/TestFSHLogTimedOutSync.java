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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/*
  Testing RS abort in case if sync fails/times out.
 */
public class TestFSHLogTimedOutSync {
  private static final Log LOG = LogFactory.getLog(TestFSHLogTimedOutSync.class);

  @Rule public TestName name = new TestName();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
  private static final String COLUMN_QUALIFIER = "MyCQ";
  private static final byte [] COLUMN_QUALIFIER_BYTES = Bytes.toBytes(COLUMN_QUALIFIER);
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
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  // Test that RS aborts in case of put, append and increment when sync fails or times out.
  @Test(timeout=300000)
  public void testRSAbortWhenSyncTimedOut() throws IOException {
    // Dodgy WAL. Will throw exceptions when flags set.
    class DodgyFSLog extends FSHLog {
      volatile boolean throwSyncException = false;

      public DodgyFSLog(FileSystem fs, Path root, String logDir, Configuration conf)
        throws IOException {
        super(fs, root, logDir, conf);
      }

      @Override
      public void sync(long txid) throws IOException {
        super.sync(txid);
        if (throwSyncException) {
          throw new TimeoutIOException("Exception");
        }
      }

      @Override
      public void sync(long txid, boolean force) throws IOException {
        super.sync(txid, force);
        if (throwSyncException) {
          throw new TimeoutIOException("Exception");
        }
      }
    }

    // Make up mocked server and services.
    RegionServerServices services = mock(RegionServerServices.class);
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + getName());
    DodgyFSLog dodgyWAL = new DodgyFSLog(fs, rootDir, getName(), CONF);
    HRegion region = initHRegion(tableName, null, null, CONF, dodgyWAL);
    region.setRegionServerServices(services);
    // Get some random bytes.
    byte[] row = Bytes.toBytes(getName());
    byte[] value = Bytes.toBytes(getName());
    // Test Put operation
    try {
      dodgyWAL.throwSyncException = true;
      Put put = new Put(row);
      put.addColumn(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES, value);
      region.put(put);
      fail();
    } catch (IOException ioe) {
      assertTrue(ioe instanceof TimeoutIOException);
    }
    // Verify that RS aborts
    Mockito.verify(services, Mockito.times(1)).
      abort(Mockito.anyString(), Mockito.<Throwable>anyObject());

    // Test Append operation
    try {
      dodgyWAL.throwSyncException = true;
      Append a = new Append(row);
      a.setReturnResults(false);
      a.add(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES, value);
      region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);
      fail();
    } catch (IOException ioe) {
      assertTrue(ioe instanceof TimeoutIOException);
    }
    // Verify that RS aborts
    Mockito.verify(services, Mockito.times(2)).
      abort(Mockito.anyString(), Mockito.<Throwable>anyObject());

    // Test Increment operation
    try {
      dodgyWAL.throwSyncException = true;
      final Increment inc = new Increment(row);
      inc.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("qual2"), 1);
      region.increment(inc, HConstants.NO_NONCE, HConstants.NO_NONCE);
      fail();
    } catch (IOException ioe) {
      assertTrue(ioe instanceof TimeoutIOException);
    }
    // Verify that RS aborts
    Mockito.verify(services, Mockito.times(3)).
      abort(Mockito.anyString(), Mockito.<Throwable>anyObject());
  }

  String getName() {
    return name.getMethodName();
  }

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  public HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
    Configuration conf, WAL wal) throws IOException {
    return TEST_UTIL.createLocalHRegion(tableName.getName(), startKey, stopKey,
      getName(), conf, false, Durability.SYNC_WAL, wal, COLUMN_FAMILY_BYTES);
  }
}
