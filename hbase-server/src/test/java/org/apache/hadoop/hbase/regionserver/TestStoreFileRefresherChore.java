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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, MediumTests.class})
public class TestStoreFileRefresherChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStoreFileRefresherChore.class);

  private HBaseTestingUtility TEST_UTIL;
  private Path testDir;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws IOException {
    TEST_UTIL = new HBaseTestingUtility();
    testDir = TEST_UTIL.getDataTestDir("TestStoreFileRefresherChore");
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), testDir);
  }

  private TableDescriptor getTableDesc(TableName tableName, int regionReplication,
      byte[]... families) {
    TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(regionReplication);
    Arrays.stream(families).map(family -> ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMaxVersions(Integer.MAX_VALUE).build()).forEachOrdered(builder::setColumnFamily);
    return builder.build();
  }

  static class FailingHRegionFileSystem extends HRegionFileSystem {
    boolean fail = false;

    FailingHRegionFileSystem(Configuration conf, FileSystem fs, Path tableDir,
        RegionInfo regionInfo) {
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

  private HRegion initHRegion(TableDescriptor htd, byte[] startKey, byte[] stopKey, int replicaId)
      throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    Path tableDir = CommonFSUtils.getTableDir(testDir, htd.getTableName());

    RegionInfo info = RegionInfoBuilder.newBuilder(htd.getTableName()).setStartKey(startKey)
        .setEndKey(stopKey).setRegionId(0L).setReplicaId(replicaId).build();
    HRegionFileSystem fs =
        new FailingHRegionFileSystem(conf, tableDir.getFileSystem(conf), tableDir, info);
    final Configuration walConf = new Configuration(conf);
    CommonFSUtils.setRootDir(walConf, tableDir);
    final WALFactory wals = new WALFactory(walConf, "log_" + replicaId);
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    HRegion region =
        new HRegion(fs, wals.getWAL(info),
            conf, htd, null);

    region.initialize();

    return region;
  }

  private void putData(Region region, int startRow, int numRows, byte[] qf, byte[]... families)
      throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      put.setDurability(Durability.SKIP_WAL);
      for (byte[] family : families) {
        put.addColumn(family, qf, null);
      }
      region.put(put);
    }
  }

  private void verifyDataExpectFail(Region newReg, int startRow, int numRows, byte[] qf,
      byte[]... families) throws IOException {
    boolean threw = false;
    try {
      verifyData(newReg, startRow, numRows, qf, families);
    } catch (AssertionError e) {
      threw = true;
    }
    if (!threw) {
      fail("Expected data verification to fail");
    }
  }

  private void verifyData(Region newReg, int startRow, int numRows, byte[] qf, byte[]... families)
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
        assertTrue(CellUtil.matchingRows(raw[j], row));
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

  @Test
  public void testIsStale() throws IOException {
    int period = 0;
    byte[][] families = new byte[][] {Bytes.toBytes("cf")};
    byte[] qf = Bytes.toBytes("cq");

    HRegionServer regionServer = mock(HRegionServer.class);
    List<HRegion> regions = new ArrayList<>();
    when(regionServer.getOnlineRegionsLocalContext()).thenReturn(regions);
    when(regionServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());

    TableDescriptor htd = getTableDesc(TableName.valueOf(name.getMethodName()), 2, families);
    HRegion primary = initHRegion(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, 0);
    HRegion replica1 = initHRegion(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, 1);
    regions.add(primary);
    regions.add(replica1);

    StaleStorefileRefresherChore chore = new StaleStorefileRefresherChore(period, regionServer,
      new StoppableImplementation());

    // write some data to primary and flush
    putData(primary, 0, 100, qf, families);
    primary.flush(true);
    verifyData(primary, 0, 100, qf, families);

    verifyDataExpectFail(replica1, 0, 100, qf, families);
    chore.chore();
    verifyData(replica1, 0, 100, qf, families);

    // simulate an fs failure where we cannot refresh the store files for the replica
    ((FailingHRegionFileSystem)replica1.getRegionFileSystem()).fail = true;

    // write some more data to primary and flush
    putData(primary, 100, 100, qf, families);
    primary.flush(true);
    verifyData(primary, 0, 200, qf, families);

    chore.chore(); // should not throw ex, but we cannot refresh the store files

    verifyData(replica1, 0, 100, qf, families);
    verifyDataExpectFail(replica1, 100, 100, qf, families);

    chore.isStale = true;
    chore.chore(); //now after this, we cannot read back any value
    try {
      verifyData(replica1, 0, 100, qf, families);
      fail("should have failed with IOException");
    } catch(IOException ex) {
      // expected
    }
  }
}
