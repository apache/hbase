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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MobFileCleanerChore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Mob file compaction base test.
  * 1. Enables batch mode for regular MOB compaction, 
  *    Sets batch size to 7 regions. (Optional)
  * 2. Disables periodic MOB compactions, sets minimum age to archive to 10 sec   
  * 3. Creates MOB table with 20 regions
  * 4. Loads MOB data (randomized keys, 1000 rows), flushes data.
  * 5. Repeats 4. two more times
  * 6. Verifies that we have 20 *3 = 60 mob files (equals to number of regions x 3)
  * 7. Runs major MOB compaction.
  * 8. Verifies that number of MOB files in a mob directory is 20 x4 = 80
  * 9. Waits for a period of time larger than minimum age to archive 
  * 10. Runs Mob cleaner chore
  * 11 Verifies that number of MOB files in a mob directory is 20.
  * 12 Runs scanner and checks all 3 * 1000 rows.
 */
@SuppressWarnings("deprecation")
public abstract class TestMobCompactionBase {
  private static final Logger LOG = 
      LoggerFactory.getLogger(TestMobCompactionBase.class);

  protected HBaseTestingUtility HTU;

  protected final static String famStr = "f1";
  protected final static byte[] fam = Bytes.toBytes(famStr);
  protected final static byte[] qualifier = Bytes.toBytes("q1");
  protected final static long mobLen = 10;
  protected final static byte[] mobVal = Bytes
      .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  protected Configuration conf;
  protected HTableDescriptor hdt;
  private HColumnDescriptor hcd;
  protected Admin admin;
  protected Table table = null;
  protected long minAgeToArchive = 10000;
  protected int numRegions = 20;
  protected int rows = 1000;
  
  protected MobFileCleanerChore cleanerChore;

  public TestMobCompactionBase() {
  }

 
  @Before
  public void setUp() throws Exception {
    HTU = new HBaseTestingUtility();
    hdt = HTU.createTableDescriptor(getClass().getName());
    conf = HTU.getConfiguration();

    initConf();

    HTU.startMiniCluster();
    admin = HTU.getAdmin();
    cleanerChore = new MobFileCleanerChore();
    hcd = new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobLen);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    byte[][] splitKeys = generateSplitKeys();
    table = HTU.createTable(hdt, splitKeys);

  }

  private byte[][] generateSplitKeys() {
    RegionSplitter.UniformSplit splitAlgo = new RegionSplitter.UniformSplit();
    return splitAlgo.split(numRegions);
  }


  protected void initConf() {

    conf.setInt("hfile.format.version", 3);
    // Disable automatic MOB compaction
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    // Disable automatic MOB file cleaner chore
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    // Set minimum age to archive to 10 sec
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, minAgeToArchive);
    // Set compacted file discharger interval to a half minAgeToArchive
    conf.setLong("hbase.hfile.compaction.discharger.interval", minAgeToArchive/2);
    //conf.setInt(MobConstants.MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE, 7);
  }

  private void loadData(int num) {
    
    Random r = new Random();
    try {
      LOG.info("Started loading {} rows", num);
      for (int i = 0; i < num; i++) {
        byte[] key = new byte[32];
        r.nextBytes(key);
        Put p = new Put(key);
        p.addColumn(fam, qualifier, mobVal);
        table.put(p);
      }
      admin.flush(table.getName());
      LOG.info("Finished loading {} rows", num);
    } catch (Exception e) {
      LOG.error("MOB file compaction chore test FAILED", e);
      assertTrue(false);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  
  public void baseTestMobFileCompaction() throws InterruptedException, IOException {

    try {

      // Load and flush data 3 times
      loadData(rows);
      loadData(rows);
      loadData(rows);
      long num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
      assertEquals(numRegions * 3, num);
      // Major MOB compact
      mobCompact(admin, hdt, hcd);
      // wait until compaction is complete
      while (admin.getCompactionState(hdt.getTableName()) != CompactionState.NONE) {
        Thread.sleep(100);
      }
      
      num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
      assertEquals(numRegions * 4, num);
      // We have guarantee, that compcated file discharger will run during this pause
      // because it has interval less than this wait time
      LOG.info("Waiting for {}ms", minAgeToArchive + 1000);
      
      Thread.sleep(minAgeToArchive + 1000);
      LOG.info("Cleaning up MOB files");
      // Cleanup again
      cleanerChore.cleanupObsoleteMobFiles(conf, table.getName());

      num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
      assertEquals(numRegions, num);
      
      long scanned = scanTable();
      assertEquals(3 * rows, scanned);

    } finally {

      admin.disableTable(hdt.getTableName());
      admin.deleteTable(hdt.getTableName());
    }
 
  }
  
  protected abstract void mobCompact(Admin admin2, HTableDescriptor hdt2, HColumnDescriptor hcd2) 
      throws IOException, InterruptedException;


  protected  long getNumberOfMobFiles(Configuration conf, TableName tableName, String family)
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

 
  protected long scanTable() {
    try {

      Result result;
      ResultScanner scanner = table.getScanner(fam);
      long counter = 0;
      while ((result = scanner.next()) != null) {
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
        counter++;
      }
      return counter;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("MOB file compaction test FAILED");
      if (HTU != null) {
        assertTrue(false);
      } else {
        System.exit(-1);
      }
    }
    return 0;
  }
}
