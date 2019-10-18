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
package org.apache.hadoop.hbase.mob;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MobFileCleanerChore;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
    Reproduction for MOB data loss

 1. Settings: Region Size 200 MB,  Flush threshold 800 KB.
 2. Insert 10 Million records
 3. MOB Compaction and Archiver
      a) Trigger MOB Compaction (every 2 minutes)
      b) Trigger major compaction (every 2 minutes)
      c) Trigger archive cleaner (every 3 minutes)
 4. Validate MOB data after complete data load.

 */
@SuppressWarnings("deprecation")
@Category(LargeTests.class)
public class TestMobCompaction {
  private static final Logger LOG = LoggerFactory.getLogger(TestMobCompaction.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobCompaction.class);
  @Rule
  public TestName testName = new TestName();

  private HBaseTestingUtility HTU;

  private final static String famStr = "f1";
  private final static byte[] fam = Bytes.toBytes(famStr);
  private final static byte[] qualifier = Bytes.toBytes("q1");
  private final static long mobLen = 10;
  private final static byte[] mobVal = Bytes
      .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  private Configuration conf;
  private HTableDescriptor hdt;
  private HColumnDescriptor hcd;
  private Admin admin;
  private long count = 500000;
  private double failureProb = 0.1;
  private Table table = null;
  private MobFileCleanerChore chore = new MobFileCleanerChore();

  private static volatile boolean run = true;

  public TestMobCompaction() {

  }

  public void init(Configuration conf, long numRows) throws IOException {
    this.conf = conf;
    this.count = numRows;
    printConf();
    hdt = createTableDescriptor("testMobCompactTable");
    Connection conn = ConnectionFactory.createConnection(this.conf);
    this.admin = conn.getAdmin();
    this.hcd = new HColumnDescriptor(fam);
    this.hcd.setMobEnabled(true);
    this.hcd.setMobThreshold(mobLen);
    this.hcd.setMaxVersions(1);
    this.hdt.addFamily(hcd);
    if (admin.tableExists(hdt.getTableName())) {
      admin.disableTable(hdt.getTableName());
      admin.deleteTable(hdt.getTableName());
    }
    admin.createTable(hdt);
    table = conn.getTable(hdt.getTableName());
  }

  private void printConf() {
    LOG.info("To run stress test, please change HBase configuration as following:");
    LOG.info("hfile.format.version=3");
    LOG.info("hbase.master.hfilecleaner.ttl=0");
    LOG.info("hbase.hregion.max.filesize=200000000");
    LOG.info("hbase.client.retries.number=100");
    LOG.info("hbase.hregion.memstore.flush.size=800000");
    LOG.info("hbase.hstore.blockingStoreFiles=150");
    LOG.info("hbase.hstore.compaction.throughput.lower.bound=50000000");
    LOG.info("hbase.hstore.compaction.throughput.higher.bound=100000000");
    LOG.info("hbase.master.mob.cleaner.period=0");
    LOG.info("hbase.mob.default.compactor=org.apache.hadoop.hbase.mob.FaultyMobStoreCompactor");
    LOG.warn("injected.fault.probability=x, where x is between 0. and 1.");

  }

  private HTableDescriptor createTableDescriptor(final String name, final int minVersions,
      final int versions, final int ttl, KeepDeletedCells keepDeleted) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    return htd;
  }

  private HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, HColumnDescriptor.DEFAULT_MIN_VERSIONS, 1,
      HConstants.FOREVER, HColumnDescriptor.DEFAULT_KEEP_DELETED);
  }

  @Before
  public void setUp() throws Exception {
    HTU = new HBaseTestingUtility();
    hdt = HTU.createTableDescriptor("testMobCompactTable");
    conf = HTU.getConfiguration();

    initConf();

    // HTU.getConfiguration().setInt("hbase.mob.compaction.chore.period", 0);
    HTU.startMiniCluster();
    admin = HTU.getAdmin();

    hcd = new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobLen);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    table = HTU.createTable(hdt, null);
  }

  private void initConf() {

    conf.setInt("hfile.format.version", 3);
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    conf.setInt("hbase.client.retries.number", 100);
    conf.setInt("hbase.hregion.max.filesize", 200000000);
    conf.setInt("hbase.hregion.memstore.flush.size", 800000);
    conf.setInt("hbase.hstore.blockingStoreFiles", 150);
    conf.setInt("hbase.hstore.compaction.throughput.lower.bound", 52428800);
    conf.setInt("hbase.hstore.compaction.throughput.higher.bound", 2 * 52428800);
    conf.setDouble("injected.fault.probability", failureProb);
    conf.set(MobStoreEngine.DEFAULT_MOB_COMPACTOR_CLASS_KEY,
      FaultyMobStoreCompactor.class.getName());

  }

  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  class MajorCompaction implements Runnable {

    @Override
    public void run() {
      while (run) {
        try {
          admin.majorCompact(hdt.getTableName(), fam);
          Thread.sleep(120000);
        } catch (Exception e) {
          LOG.error("MOB Stress Test FAILED", e);
          System.exit(-1);
        }
      }
    }
  }

  class CleanMobAndArchive implements Runnable {

    @Override
    public void run() {
      while (run) {
        try {
          LOG.info("MOB cleanup chore started ...");
          chore.cleanupObsoleteMobFiles(conf, table.getName());
          LOG.info("MOB cleanup chore finished");

          Thread.sleep(130000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  class WriteData implements Runnable {

    private long rows = -1;

    public WriteData(long rows) {
      this.rows = rows;
    }

    @Override
    public void run() {
      try {

        // BufferedMutator bm = admin.getConnection().getBufferedMutator(table.getName());
        // Put Operation
        for (int i = 0; i < rows; i++) {
          Put p = new Put(Bytes.toBytes(i));
          p.addColumn(fam, qualifier, mobVal);
          table.put(p);

          // bm.mutate(p);
          if (i % 10000 == 0) {
            LOG.info("LOADED=" + i);
            try {
              Thread.sleep(500);
            } catch (InterruptedException ee) {

            }
          }
          if (i % 100000 == 0) {
            printStats(i);
          }
        }
        // bm.flush();
        admin.flush(table.getName());
        run = false;
      } catch (Exception e) {
        LOG.error("MOB Stress Test FAILED", e);
        System.exit(-1);
      }
    }
  }

  @Test
  public void testMobCompaction() throws InterruptedException, IOException {

    try {

      Thread writeData = new Thread(new WriteData(count));
      writeData.start();

      Thread majorcompact = new Thread(new MajorCompaction());
      majorcompact.start();

      Thread cleaner = new Thread(new CleanMobAndArchive());
      cleaner.start();

      while (run) {
        Thread.sleep(1000);
      }

      getNumberOfMobFiles(conf, table.getName(), new String(fam));
      LOG.info("Waiting for write thread to finish ...");
      writeData.join();
      // Cleanup again
      chore.cleanupObsoleteMobFiles(conf, table.getName());

      if (HTU != null) {
        LOG.info("Archive cleaner started ...");
        // Call archive cleaner again
        HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
        LOG.info("Archive cleaner finished");
      }

      scanTable();

    } finally {

      admin.disableTable(hdt.getTableName());
      admin.deleteTable(hdt.getTableName());
    }
    LOG.info("MOB Stress Test finished OK");
    printStats(count);

  }
  
  private  long getNumberOfMobFiles(Configuration conf, TableName tableName, String family)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dir = MobUtils.getMobFamilyPath(conf, tableName, family);
    FileStatus[] stat = fs.listStatus(dir);
    for (FileStatus st : stat) {
      LOG.debug("MOB Directory content: {}", st.getPath());
    }
    LOG.debug("MOB Directory content total files: {}", stat.length);

    return stat.length;
  }

  public void printStats(long loaded) {
    LOG.info("MOB Stress Test: loaded=" + loaded + " compactions="
        + FaultyMobStoreCompactor.totalCompactions.get() + " major="
        + FaultyMobStoreCompactor.totalMajorCompactions.get() + " mob="
        + FaultyMobStoreCompactor.mobCounter.get() + " injected failures="
        + FaultyMobStoreCompactor.totalFailures.get());
  }

  private void scanTable() {
    try {

      Result result;
      ResultScanner scanner = table.getScanner(fam);
      int counter = 0;
      while ((result = scanner.next()) != null) {
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
        if (counter % 10000 == 0) {
          LOG.info("GET=" + counter);
        }
        counter++;
      }
      assertEquals(count, counter);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("MOB Stress Test FAILED");
      if (HTU != null) {
        assertTrue(false);
      } else {
        System.exit(-1);
      }
    }
  }
}
