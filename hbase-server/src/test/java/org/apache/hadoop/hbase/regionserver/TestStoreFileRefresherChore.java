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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestStoreFileRefresherChore {

  private HBaseTestingUtility TEST_UTIL;
  private Path testDir;

  @Before
  public void setUp() throws IOException {
    TEST_UTIL = new HBaseTestingUtility();
    testDir = TEST_UTIL.getDataTestDir("TestStoreFileRefresherChore");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), testDir);
  }

  private HTableDescriptor getTableDesc(TableName tableName, byte[]... families) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // Set default to be three versions.
      hcd.setMaxVersions(Integer.MAX_VALUE);
      htd.addFamily(hcd);
    }
    return htd;
  }

  static class FailingHRegionFileSystem extends HRegionFileSystem {
    boolean fail = false;
    FailingHRegionFileSystem(Configuration conf, FileSystem fs, Path tableDir, HRegionInfo regionInfo) {
      super(conf, fs, tableDir, regionInfo);
    }

    @Override
    public Collection<StoreFileInfo> getStoreFiles(String familyName) throws IOException {
      if (fail) {
        throw new IOException("simulating FS failure");
      }
      return super.getStoreFiles(familyName);
    }
  }

  private HRegion initHRegion(HTableDescriptor htd, byte[] startKey, byte[] stopKey, int replicaId) throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    Path tableDir = FSUtils.getTableDir(testDir, htd.getTableName());

    HRegionInfo info = new HRegionInfo(htd.getTableName(), startKey, stopKey, false, 0, replicaId);

    HRegionFileSystem fs = new FailingHRegionFileSystem(conf, tableDir.getFileSystem(conf), tableDir, info);
    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, tableDir);
    final WALFactory wals = new WALFactory(walConf, null, "log_" + replicaId);
    HRegion region = new HRegion(fs, wals.getWAL(info.getEncodedNameAsBytes()), conf, htd, null);

    region.initialize();

    return region;
  }

  private void putData(HRegion region, int startRow, int numRows, byte[] qf, byte[]... families) throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      put.setDurability(Durability.SKIP_WAL);
      for (byte[] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private void verifyData(HRegion newReg, int startRow, int numRows, byte[] qf, byte[]... families)
      throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      byte[] row = Bytes.toBytes("" + i);
      Get get = new Get(row);
      for (byte[] family : families) {
        get.addColumn(family, qf);
      }
      Result result = newReg.get(get);
      Cell[] raw = result.rawCells();
      assertEquals(families.length, result.size());
      for (int j = 0; j < families.length; j++) {
        assertTrue(CellUtil.matchingRow(raw[j], row));
        assertTrue(CellUtil.matchingFamily(raw[j], families[j]));
        assertTrue(CellUtil.matchingQualifier(raw[j], qf));
      }
    }
  }

  static class StaleStorefileRefresherChore extends StorefileRefresherChore {
    boolean isStale = false;
    public StaleStorefileRefresherChore(int period, HRegionServer regionServer,
        Stoppable stoppable) {
      super(period, false, regionServer, stoppable);
    }
    @Override
    protected boolean isRegionStale(String encodedName, long time) {
      return isStale;
    }
  }

  @Test (timeout = 60000)
  public void testIsStale() throws IOException {
    int period = 0;
    byte[][] families = new byte[][] {Bytes.toBytes("cf")};
    byte[] qf = Bytes.toBytes("cq");

    HRegionServer regionServer = mock(HRegionServer.class);
    List<HRegion> regions = new ArrayList<HRegion>();
    when(regionServer.getOnlineRegionsLocalContext()).thenReturn(regions);
    when(regionServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());

    HTableDescriptor htd = getTableDesc(TableName.valueOf("testIsStale"), families);
    HRegion primary = initHRegion(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, 0);
    HRegion replica1 = initHRegion(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, 1);
    regions.add(primary);
    regions.add(replica1);

    StaleStorefileRefresherChore chore = new StaleStorefileRefresherChore(period, regionServer, new StoppableImplementation());

    // write some data to primary and flush
    putData(primary, 0, 100, qf, families);
    primary.flushcache();
    verifyData(primary, 0, 100, qf, families);

    try {
      verifyData(replica1, 0, 100, qf, families);
      Assert.fail("should have failed");
    } catch(AssertionError ex) {
      // expected
    }
    chore.chore();
    verifyData(replica1, 0, 100, qf, families);

    // simulate an fs failure where we cannot refresh the store files for the replica
    ((FailingHRegionFileSystem)replica1.getRegionFileSystem()).fail = true;

    // write some more data to primary and flush
    putData(primary, 100, 100, qf, families);
    primary.flushcache();
    verifyData(primary, 0, 200, qf, families);

    chore.chore(); // should not throw ex, but we cannot refresh the store files

    verifyData(replica1, 0, 100, qf, families);
    try {
      verifyData(replica1, 100, 100, qf, families);
      Assert.fail("should have failed");
    } catch(AssertionError ex) {
      // expected
    }

    chore.isStale = true;
    chore.chore(); //now after this, we cannot read back any value
    try {
      verifyData(replica1, 0, 100, qf, families);
      Assert.fail("should have failed with IOException");
    } catch(IOException ex) {
      // expected
    }
  }
}
