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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for WAL write durability - hflush vs hsync
 */
public abstract class WALDurabilityTestBase<T extends WAL> {

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf;
  private String dir;
  @Rule
  public TestName name = new TestName();

  // Test names
  protected TableName tableName;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    dir = TEST_UTIL.getDataTestDir("TestHRegion").toString();
    tableName = TableName.valueOf(name.getMethodName());
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.cleanupTestDir();
  }

  protected abstract T getWAL(FileSystem fs, Path root, String logDir, Configuration conf)
    throws IOException;

  protected abstract void resetSyncFlag(T wal);

  protected abstract Boolean getSyncFlag(T wal);

  protected abstract Boolean getWriterSyncFlag(T wal);

  @Test
  public void testWALDurability() throws IOException {
    byte[] bytes = Bytes.toBytes(getName());
    Put put = new Put(bytes);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);

    // global hbase.wal.hsync false, no override in put call - hflush
    conf.set(HRegion.WAL_HSYNC_CONF_KEY, "false");
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = new Path(dir + getName());
    T wal = getWAL(fs, rootDir, getName(), conf);
    HRegion region = initHRegion(tableName, null, null, conf, wal);
    try {
      resetSyncFlag(wal);
      assertNull(getSyncFlag(wal));
      assertNull(getWriterSyncFlag(wal));
      region.put(put);
      assertFalse(getSyncFlag(wal));
      assertFalse(getWriterSyncFlag(wal));

      // global hbase.wal.hsync false, durability set in put call - fsync
      put.setDurability(Durability.FSYNC_WAL);
      resetSyncFlag(wal);
      assertNull(getSyncFlag(wal));
      assertNull(getWriterSyncFlag(wal));
      region.put(put);
      assertTrue(getSyncFlag(wal));
      assertTrue(getWriterSyncFlag(wal));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }

    // global hbase.wal.hsync true, no override in put call
    conf.set(HRegion.WAL_HSYNC_CONF_KEY, "true");
    fs = FileSystem.get(conf);
    wal = getWAL(fs, rootDir, getName(), conf);
    region = initHRegion(tableName, null, null, conf, wal);

    try {
      resetSyncFlag(wal);
      assertNull(getSyncFlag(wal));
      assertNull(getWriterSyncFlag(wal));
      region.put(put);
      assertTrue(getSyncFlag(wal));
      assertTrue(getWriterSyncFlag(wal));

      // global hbase.wal.hsync true, durability set in put call - fsync
      put.setDurability(Durability.FSYNC_WAL);
      resetSyncFlag(wal);
      assertNull(getSyncFlag(wal));
      assertNull(getWriterSyncFlag(wal));
      region.put(put);
      assertTrue(getSyncFlag(wal));
      assertTrue(getWriterSyncFlag(wal));

      // global hbase.wal.hsync true, durability set in put call - sync
      put = new Put(bytes);
      put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);
      put.setDurability(Durability.SYNC_WAL);
      resetSyncFlag(wal);
      assertNull(getSyncFlag(wal));
      assertNull(getWriterSyncFlag(wal));
      region.put(put);
      assertFalse(getSyncFlag(wal));
      assertFalse(getWriterSyncFlag(wal));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  private String getName() {
    return name.getMethodName();
  }

  /**
   * @return A region on which you must call {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)}
   *         when done.
   */
  public static HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      Configuration conf, WAL wal) throws IOException {
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, conf, false,
      Durability.USE_DEFAULT, wal, COLUMN_FAMILY_BYTES);
  }
}
