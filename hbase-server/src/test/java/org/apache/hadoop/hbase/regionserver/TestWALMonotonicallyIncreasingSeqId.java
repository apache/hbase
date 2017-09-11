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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Test for HBASE-17471
 * MVCCPreAssign is added by HBASE-16698, but pre-assign mvcc is only used in put/delete
 * path. Other write paths like increment/append still assign mvcc in ringbuffer's consumer
 * thread. If put and increment are used parallel. Then seqid in WAL may not increase monotonically
 * Disorder in wals will lead to data loss.
 * This case use two thread to put and increment at the same time in a single region.
 * Then check the seqid in WAL. If seqid is wal is not monotonically increasing, this case will fail
 *
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestWALMonotonicallyIncreasingSeqId {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Path testDir = TEST_UTIL.getDataTestDir("TestWALMonotonicallyIncreasingSeqId");
  private WALFactory wals;
  private FileSystem fileSystem;
  private Configuration walConf;

  public static final String KEY_SEED = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private static final int KEY_SEED_LEN = KEY_SEED.length();

  private static final char[] KEY_SEED_CHARS = KEY_SEED.toCharArray();

  @Rule
  public TestName name = new TestName();

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

  private Region initHRegion(HTableDescriptor htd, byte[] startKey, byte[] stopKey, int replicaId)
      throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.hregion.mvcc.preassign", false);
    Path tableDir = FSUtils.getTableDir(testDir, htd.getTableName());

    HRegionInfo info = new HRegionInfo(htd.getTableName(), startKey, stopKey, false, 0, replicaId);
    fileSystem =  tableDir.getFileSystem(conf);
    HRegionFileSystem fs = new HRegionFileSystem(conf, fileSystem, tableDir, info);
    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, tableDir);
    this.walConf = walConf;
    wals = new WALFactory(walConf, null, "log_" + replicaId);
    ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null);
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.getDefaultRootDirPath(), conf, htd,
        wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace()));
    return region;
  }

  CountDownLatch latch = new CountDownLatch(1);
  public class PutThread extends Thread {
    HRegion region;
    public PutThread(HRegion region) {
      this.region = region;
    }

    @Override
    public void run() {
      try {
        for(int i = 0; i < 100; i++) {
          byte[] row = Bytes.toBytes("putRow" + i);
          Put put = new Put(row);
          put.addColumn("cf".getBytes(), Bytes.toBytes(0), Bytes.toBytes(""));
          //put.setDurability(Durability.ASYNC_WAL);
          latch.await();
          region.batchMutate(new Mutation[]{put});
          Thread.sleep(10);
        }


      } catch (Throwable t) {
        LOG.warn("Error happend when Increment: ", t);
      }

    }
  }

  public class IncThread extends Thread {
    HRegion region;
    public IncThread(HRegion region) {
      this.region = region;
    }
    @Override
    public void run() {
      try {
        for(int i = 0; i < 100; i++) {
          byte[] row = Bytes.toBytes("incrementRow" + i);
          Increment inc = new Increment(row);
          inc.addColumn("cf".getBytes(), Bytes.toBytes(0), 1);
          //inc.setDurability(Durability.ASYNC_WAL);
          region.increment(inc);
          latch.countDown();
          Thread.sleep(10);
        }


      } catch (Throwable t) {
        LOG.warn("Error happend when Put: ", t);
      }

    }
  }

  @Test
  public void TestWALMonotonicallyIncreasingSeqId() throws Exception {
    byte[][] families = new byte[][] {Bytes.toBytes("cf")};
    byte[] qf = Bytes.toBytes("cq");
    HTableDescriptor htd = getTableDesc(TableName.valueOf(name.getMethodName()), families);
    HRegion region = (HRegion)initHRegion(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, 0);
    List<Thread> putThreads = new ArrayList<>();
    for(int i = 0; i < 1; i++) {
      putThreads.add(new PutThread(region));
    }
    IncThread incThread = new IncThread(region);
    for(int i = 0; i < 1; i++) {
      putThreads.get(i).start();
    }
    incThread.start();
    incThread.join();

    Path logPath = ((FSHLog) region.getWAL()).getCurrentFileName();
    region.getWAL().rollWriter();
    Thread.sleep(10);
    Path hbaseDir = new Path(walConf.get(HConstants.HBASE_DIR));
    Path oldWalsDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    WAL.Reader reader = null;
    try {
      reader = wals.createReader(fileSystem, logPath);
    } catch (Throwable t) {
      reader = wals.createReader(fileSystem, new Path(oldWalsDir, logPath.getName()));

    }
    WAL.Entry e;
    try {
      long currentMaxSeqid = 0;
      while ((e = reader.next()) != null) {
        if (!WALEdit.isMetaEditFamily(e.getEdit().getCells().get(0))) {
          long currentSeqid = e.getKey().getSequenceId();
          if(currentSeqid > currentMaxSeqid) {
            currentMaxSeqid = currentSeqid;
          } else {
            Assert.fail("Current max Seqid is " + currentMaxSeqid
                + ", but the next seqid in wal is smaller:" + currentSeqid);
          }
        }
      }
    } finally {
      if(reader != null) {
        reader.close();
      }
      if(region != null) {
        region.close();
      }
    }
  }


}
