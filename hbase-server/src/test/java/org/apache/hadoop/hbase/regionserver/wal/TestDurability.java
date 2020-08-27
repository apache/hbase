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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for WAL write durability
 */
@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestDurability {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDurability.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static FileSystem FS;
  private static MiniDFSCluster CLUSTER;
  private static Configuration CONF;
  private static Path DIR;

  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] ROW = Bytes.toBytes("row");
  private static byte[] COL = Bytes.toBytes("col");

  @Parameter
  public String walProvider;

  @Rule
  public TestName name = new TestName();

  @Parameters(name = "{index}: provider={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { "defaultProvider" }, new Object[] { "asyncfs" });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(1);

    CLUSTER = TEST_UTIL.getDFSCluster();
    FS = CLUSTER.getFileSystem();
    DIR = TEST_UTIL.getDataTestDirOnTestFS("TestDurability");
    CommonFSUtils.setRootDir(CONF, DIR);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    CONF.set(WALFactory.WAL_PROVIDER, walProvider);
  }

  @After
  public void tearDown() throws IOException {
    FS.delete(DIR, true);
  }

  @Test
  public void testDurability() throws Exception {
    WALFactory wals = new WALFactory(CONF,
        ServerName.valueOf("TestDurability", 16010, System.currentTimeMillis()).toString());
    HRegion region = createHRegion(wals, Durability.USE_DEFAULT);
    WAL wal = region.getWAL();
    HRegion deferredRegion = createHRegion(region.getTableDescriptor(), region.getRegionInfo(),
      "deferredRegion", wal, Durability.ASYNC_WAL);

    region.put(newPut(null));
    verifyWALCount(wals, wal, 1);

    // a put through the deferred table does not write to the wal immediately,
    // but maybe has been successfully sync-ed by the underlying AsyncWriter +
    // AsyncFlusher thread
    deferredRegion.put(newPut(null));
    // but will after we sync the wal
    wal.sync();
    verifyWALCount(wals, wal, 2);

    // a put through a deferred table will be sync with the put sync'ed put
    deferredRegion.put(newPut(null));
    wal.sync();
    verifyWALCount(wals, wal, 3);
    region.put(newPut(null));
    verifyWALCount(wals, wal, 4);

    // a put through a deferred table will be sync with the put sync'ed put
    deferredRegion.put(newPut(Durability.USE_DEFAULT));
    wal.sync();
    verifyWALCount(wals, wal, 5);
    region.put(newPut(Durability.USE_DEFAULT));
    verifyWALCount(wals, wal, 6);

    // SKIP_WAL never writes to the wal
    region.put(newPut(Durability.SKIP_WAL));
    deferredRegion.put(newPut(Durability.SKIP_WAL));
    verifyWALCount(wals, wal, 6);
    wal.sync();
    verifyWALCount(wals, wal, 6);

    // Async overrides sync table default
    region.put(newPut(Durability.ASYNC_WAL));
    deferredRegion.put(newPut(Durability.ASYNC_WAL));
    wal.sync();
    verifyWALCount(wals, wal, 8);

    // sync overrides async table default
    region.put(newPut(Durability.SYNC_WAL));
    deferredRegion.put(newPut(Durability.SYNC_WAL));
    verifyWALCount(wals, wal, 10);

    // fsync behaves like sync
    region.put(newPut(Durability.FSYNC_WAL));
    deferredRegion.put(newPut(Durability.FSYNC_WAL));
    verifyWALCount(wals, wal, 12);
  }

  @Test
  public void testIncrement() throws Exception {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");

    // Setting up region
    WALFactory wals = new WALFactory(CONF,
        ServerName.valueOf("TestIncrement", 16010, System.currentTimeMillis()).toString());
    HRegion region = createHRegion(wals, Durability.USE_DEFAULT);
    WAL wal = region.getWAL();

    // col1: amount = 0, 1 write back to WAL
    Increment inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 0);
    Result res = region.increment(inc1);
    assertEquals(1, res.size());
    assertEquals(0, Bytes.toLong(res.getValue(FAMILY, col1)));
    verifyWALCount(wals, wal, 1);

    // col1: amount = 1, 1 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 1);
    res = region.increment(inc1);
    assertEquals(1, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    verifyWALCount(wals, wal, 2);

    // col1: amount = 0, 1 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 0);
    res = region.increment(inc1);
    assertEquals(1, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    verifyWALCount(wals, wal, 3);
    // col1: amount = 0, col2: amount = 0, col3: amount = 0
    // 1 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 0);
    inc1.addColumn(FAMILY, col2, 0);
    inc1.addColumn(FAMILY, col3, 0);
    res = region.increment(inc1);
    assertEquals(3, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    assertEquals(0, Bytes.toLong(res.getValue(FAMILY, col2)));
    assertEquals(0, Bytes.toLong(res.getValue(FAMILY, col3)));
    verifyWALCount(wals, wal, 4);

    // col1: amount = 5, col2: amount = 4, col3: amount = 3
    // 1 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 5);
    inc1.addColumn(FAMILY, col2, 4);
    inc1.addColumn(FAMILY, col3, 3);
    res = region.increment(inc1);
    assertEquals(3, res.size());
    assertEquals(6, Bytes.toLong(res.getValue(FAMILY, col1)));
    assertEquals(4, Bytes.toLong(res.getValue(FAMILY, col2)));
    assertEquals(3, Bytes.toLong(res.getValue(FAMILY, col3)));
    verifyWALCount(wals, wal, 5);
  }

  /**
   * Test when returnResults set to false in increment it should not return the result instead it
   * resturn null.
   */
  @Test
  public void testIncrementWithReturnResultsSetToFalse() throws Exception {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] col1 = Bytes.toBytes("col1");

    // Setting up region
    WALFactory wals = new WALFactory(CONF,
        ServerName
            .valueOf("testIncrementWithReturnResultsSetToFalse", 16010, System.currentTimeMillis())
            .toString());
    HRegion region = createHRegion(wals, Durability.USE_DEFAULT);

    Increment inc1 = new Increment(row1);
    inc1.setReturnResults(false);
    inc1.addColumn(FAMILY, col1, 1);
    Result res = region.increment(inc1);
    assertTrue(res.isEmpty());
  }

  private Put newPut(Durability durability) {
    Put p = new Put(ROW);
    p.addColumn(FAMILY, COL, COL);
    if (durability != null) {
      p.setDurability(durability);
    }
    return p;
  }

  private void verifyWALCount(WALFactory wals, WAL log, int expected) throws Exception {
    Path walPath = AbstractFSWALProvider.getCurrentFileName(log);
    WAL.Reader reader = wals.createReader(FS, walPath);
    int count = 0;
    WAL.Entry entry = new WAL.Entry();
    while (reader.next(entry) != null) {
      count++;
    }
    reader.close();
    assertEquals(expected, count);
  }

  // lifted from TestAtomicOperation
  private HRegion createHRegion(WALFactory wals, Durability durability) throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName().replaceAll("[^A-Za-z0-9-_]", "_"));
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    Path path = new Path(DIR, tableName.getNameAsString());
    if (FS.exists(path)) {
      if (!FS.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return HRegion.createHRegion(info, path, CONF, htd, wals.getWAL(info));
  }

  private HRegion createHRegion(TableDescriptor td, RegionInfo info, String dir, WAL wal,
      Durability durability) throws IOException {
    Path path = new Path(DIR, dir);
    if (FS.exists(path)) {
      if (!FS.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return HRegion.createHRegion(info, path, CONF, td, wal);
  }
}
