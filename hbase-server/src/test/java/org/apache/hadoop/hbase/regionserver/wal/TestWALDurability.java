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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLABImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests for WAL write durability - hflush vs hsync
 */
@Category({ MediumTests.class })
public class TestWALDurability {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALDurability.class);

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
  public void setup() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    dir = TEST_UTIL.getDataTestDir("TestHRegion").toString();
    tableName = TableName.valueOf(name.getMethodName());
  }

  @Test
  public void testWALDurability() throws IOException {
    class CustomFSLog extends FSHLog {
      private Boolean syncFlag;

      public CustomFSLog(FileSystem fs, Path root, String logDir, Configuration conf)
          throws IOException {
        super(fs, root, logDir, conf);
      }

      @Override
      public void sync(boolean forceSync) throws IOException {
        syncFlag = forceSync;
        super.sync(forceSync);
      }

      @Override
      public void sync(long txid, boolean forceSync) throws IOException {
        syncFlag = forceSync;
        super.sync(txid, forceSync);
      }

      private void resetSyncFlag() {
        this.syncFlag = null;
      }

    }
    // global hbase.wal.hsync false, no override in put call - hflush
    conf.set(HRegion.WAL_HSYNC_CONF_KEY, "false");
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = new Path(dir + getName());
    CustomFSLog customFSLog = new CustomFSLog(fs, rootDir, getName(), conf);
    customFSLog.init();
    HRegion region = initHRegion(tableName, null, null, customFSLog);
    byte[] bytes = Bytes.toBytes(getName());
    Put put = new Put(bytes);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);

    customFSLog.resetSyncFlag();
    assertNull(customFSLog.syncFlag);
    region.put(put);
    assertEquals(customFSLog.syncFlag, false);

    // global hbase.wal.hsync true, no override in put call
    conf.set(HRegion.WAL_HSYNC_CONF_KEY, "true");
    fs = FileSystem.get(conf);
    customFSLog = new CustomFSLog(fs, rootDir, getName(), conf);
    customFSLog.init();
    region = initHRegion(tableName, null, null, customFSLog);

    customFSLog.resetSyncFlag();
    assertNull(customFSLog.syncFlag);
    region.put(put);
    assertEquals(customFSLog.syncFlag, true);

    // global hbase.wal.hsync true, durability set in put call - fsync
    put.setDurability(Durability.FSYNC_WAL);
    customFSLog.resetSyncFlag();
    assertNull(customFSLog.syncFlag);
    region.put(put);
    assertEquals(customFSLog.syncFlag, true);

    // global hbase.wal.hsync true, durability set in put call - sync
    put = new Put(bytes);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), bytes);
    put.setDurability(Durability.SYNC_WAL);
    customFSLog.resetSyncFlag();
    assertNull(customFSLog.syncFlag);
    region.put(put);
    assertEquals(customFSLog.syncFlag, false);

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  private String getName() {
    return name.getMethodName();
  }

  /**
   * @return A region on which you must call {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)}
   *         when done.
   */
  public static HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey, WAL wal)
      throws IOException {
    ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null);
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, false, Durability.USE_DEFAULT,
      wal, COLUMN_FAMILY_BYTES);
  }
}
