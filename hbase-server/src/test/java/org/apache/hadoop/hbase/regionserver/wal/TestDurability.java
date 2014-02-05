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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for HLog write durability
 */
@Category(MediumTests.class)
public class TestDurability {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static FileSystem FS;
  private static MiniDFSCluster CLUSTER;
  private static Configuration CONF;
  private static Path DIR;

  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] ROW = Bytes.toBytes("row");
  private static byte[] COL = Bytes.toBytes("col");


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(1);

    CLUSTER = TEST_UTIL.getDFSCluster();
    FS = CLUSTER.getFileSystem();
    DIR = TEST_UTIL.getDataTestDirOnTestFS("TestDurability");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDurability() throws Exception {
    HLog wal = HLogFactory.createHLog(FS, DIR, "hlogdir",
        "hlogdir_archive", CONF);
    byte[] tableName = Bytes.toBytes("TestDurability");
    HRegion region = createHRegion(tableName, "region", wal, false);
    HRegion deferredRegion = createHRegion(tableName, "deferredRegion", wal, true);

    region.put(newPut(null));
    verifyHLogCount(wal, 1);

    // a put through the deferred table does not write to the wal immdiately,
    // but maybe has been successfully sync-ed by the underlying AsyncWriter +
    // AsyncFlusher thread
    deferredRegion.put(newPut(null));
    // but will after we sync the wal
    wal.sync();
    verifyHLogCount(wal, 2);

    // a put through a deferred table will be sync with the put sync'ed put
    deferredRegion.put(newPut(null));
    wal.sync();
    verifyHLogCount(wal, 3);
    region.put(newPut(null));
    verifyHLogCount(wal, 4);

    // a put through a deferred table will be sync with the put sync'ed put
    deferredRegion.put(newPut(Durability.USE_DEFAULT));
    wal.sync();
    verifyHLogCount(wal, 5);
    region.put(newPut(Durability.USE_DEFAULT));
    verifyHLogCount(wal, 6);

    // SKIP_WAL never writes to the wal
    region.put(newPut(Durability.SKIP_WAL));
    deferredRegion.put(newPut(Durability.SKIP_WAL));
    verifyHLogCount(wal, 6);
    wal.sync();
    verifyHLogCount(wal, 6);

    // async overrides sync table default
    region.put(newPut(Durability.ASYNC_WAL));
    deferredRegion.put(newPut(Durability.ASYNC_WAL));
    wal.sync();
    verifyHLogCount(wal, 8);

    // sync overrides async table default
    region.put(newPut(Durability.SYNC_WAL));
    deferredRegion.put(newPut(Durability.SYNC_WAL));
    verifyHLogCount(wal, 10);

    // fsync behaves like sync
    region.put(newPut(Durability.FSYNC_WAL));
    deferredRegion.put(newPut(Durability.FSYNC_WAL));
    verifyHLogCount(wal, 12);
  }

  @Test
  public void testIncrement() throws Exception {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");

    // Setting up region
    HLog wal = HLogFactory.createHLog(FS, DIR, "myhlogdir",
        "myhlogdir_archive", CONF);
    byte[] tableName = Bytes.toBytes("TestIncrement");
    HRegion region = createHRegion(tableName, "increment", wal, false);

    // col1: amount = 1, 1 write back to WAL
    Increment inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 1);
    Result res = region.increment(inc1);
    assertEquals(1, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    verifyHLogCount(wal, 1);

    // col1: amount = 0, 0 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 0);
    res = region.increment(inc1);
    assertEquals(1, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    verifyHLogCount(wal, 1);

    // col1: amount = 0, col2: amount = 0, col3: amount = 0
    // 0 write back to WAL
    inc1 = new Increment(row1);
    inc1.addColumn(FAMILY, col1, 0);
    inc1.addColumn(FAMILY, col2, 0);
    inc1.addColumn(FAMILY, col3, 0);
    res = region.increment(inc1);
    assertEquals(3, res.size());
    assertEquals(1, Bytes.toLong(res.getValue(FAMILY, col1)));
    assertEquals(0, Bytes.toLong(res.getValue(FAMILY, col2)));
    assertEquals(0, Bytes.toLong(res.getValue(FAMILY, col3)));
    verifyHLogCount(wal, 1);

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
    verifyHLogCount(wal, 2);
  }

  private Put newPut(Durability durability) {
    Put p = new Put(ROW);
    p.add(FAMILY, COL, COL);
    if (durability != null) {
      p.setDurability(durability);
    }
    return p;
  }

  private void verifyHLogCount(HLog log, int expected) throws Exception {
    Path walPath = ((FSHLog) log).computeFilename();
    HLog.Reader reader = HLogFactory.createReader(FS, walPath, CONF);
    int count = 0;
    HLog.Entry entry = new HLog.Entry();
    while (reader.next(entry) != null) count++;
    reader.close();
    assertEquals(expected, count);
  }

  // lifted from TestAtomicOperation
  private HRegion createHRegion (byte [] tableName, String callingMethod,
      HLog log, boolean isAsyncLogFlush)
    throws IOException {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
      htd.setDeferredLogFlush(isAsyncLogFlush);
      HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
      HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
      Path path = new Path(DIR + callingMethod);
      if (FS.exists(path)) {
        if (!FS.delete(path, true)) {
          throw new IOException("Failed delete of " + path);
        }
      }
      return HRegion.createHRegion(info, path, CONF, htd, log);
    }

}
