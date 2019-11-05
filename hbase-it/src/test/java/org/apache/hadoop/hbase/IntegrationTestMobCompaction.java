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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MobFileCleanerChore;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.mob.FaultyMobStoreCompactor;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobStoreEngine;
import org.apache.hadoop.hbase.mob.MobUtils;

import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An integration test to detect regressions in HBASE-22749. Test creates 
 * MOB-enabled table, and runs in parallel, the following tasks: loads data,
 * runs MOB compactions, runs MOB cleaning chore. The failure injections into MOB 
 * compaction cycle is implemented via specific sub-class of DefaultMobStoreCompactor -
 * FaultyMobStoreCompactor. The probability of failure is controlled by command-line
 * argument 'failprob'.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-22749">HBASE-22749</a>
 */
@SuppressWarnings("deprecation")

@Category(IntegrationTests.class)
public class IntegrationTestMobCompaction extends IntegrationTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestMobCompaction.class);
  
  protected static final String REGIONSERVER_COUNT_KEY = "servers";
  protected static final String ROWS_COUNT_KEY = "rows";
  protected static final String FAILURE_PROB_KEY = "failprob";
  
  protected static final int DEFAULT_REGIONSERVER_COUNT = 3;
  protected static final int DEFAULT_ROWS_COUNT = 5000000; 
  protected static final double DEFAULT_FAILURE_PROB = 0.1;
  
  protected static int regionServerCount = DEFAULT_REGIONSERVER_COUNT;
  protected static long rowsToLoad = DEFAULT_ROWS_COUNT;
  protected static double failureProb = DEFAULT_FAILURE_PROB;
  
  protected static String famStr = "f1";
  protected static byte[] fam = Bytes.toBytes(famStr);
  protected static byte[] qualifier = Bytes.toBytes("q1");
  protected static long mobLen = 10;
  protected static byte[] mobVal = Bytes
      .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  private static Configuration conf;
  private static HTableDescriptor hdt;
  private static HColumnDescriptor hcd;
  private static Admin admin;
  private static Table table = null;
  private static MobFileCleanerChore chore;

  private static volatile boolean run = true;
  
  @Override
  @Before
  public void setUp() throws Exception {
    util = getTestingUtil(getConf());
    conf = util.getConfiguration();
    // Initialize with test-specific configuration values
    initConf(conf);
    regionServerCount =
        conf.getInt(REGIONSERVER_COUNT_KEY, DEFAULT_REGIONSERVER_COUNT);
    LOG.info("Initializing cluster with {} region servers.", regionServerCount);
    util.initializeCluster(regionServerCount);
    admin = util.getAdmin();

    createTestTable();

    LOG.info("Cluster initialized and ready");
  }

  private void createTestTable() throws IOException {
    // Create test table
    hdt = util.createTableDescriptor("testMobCompactTable");
    hcd = new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobLen);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    table = util.createTable(hdt, null);
  }
  
  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    if(util.isDistributedCluster()) {
      deleteTablesIfAny();
      // TODO
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  @Override
  public void setUpMonkey() throws Exception {
    // Sorry, no Monkey
  }

  private void deleteTablesIfAny() throws IOException {
    if (table != null) {  
      util.deleteTableIfAny(table.getName());
    }
  }
 
 

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    LOG.debug("Initializing/checking cluster has {} servers",regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.debug("Done initializing/checking cluster");
  }

  /**
   *
   * @return status of CLI execution
   */
  @Override
  public int runTestFromCommandLine() throws Exception {
    testMobCompaction();
    return 0;
  }

  @Override
  public TableName getTablename() {
    // That is only valid when Monkey is CALM (no monkey)
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    // That is only valid when Monkey is CALM (no monkey)
    return null;
  }

  @Override
  protected void addOptions() {
    addOptWithArg(REGIONSERVER_COUNT_KEY,
      "Total number of region servers. Default: '" + DEFAULT_REGIONSERVER_COUNT + "'");
    addOptWithArg(ROWS_COUNT_KEY,
      "Total number of data rows to load. Default: '" + DEFAULT_ROWS_COUNT + "'");
    addOptWithArg(FAILURE_PROB_KEY,
      "Probability of a failure of a region MOB compaction request. Default: '" 
    + DEFAULT_FAILURE_PROB + "'");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    
    regionServerCount =
        Integer.parseInt(cmd.getOptionValue(REGIONSERVER_COUNT_KEY,
          Integer.toString(DEFAULT_REGIONSERVER_COUNT)));
    rowsToLoad =
        Long.parseLong(cmd.getOptionValue(ROWS_COUNT_KEY,
          Long.toString(DEFAULT_ROWS_COUNT)));
    failureProb = Double.parseDouble(cmd.getOptionValue(FAILURE_PROB_KEY,
      Double.toString(DEFAULT_FAILURE_PROB)));
    
    LOG.info(MoreObjects.toStringHelper("Parsed Options")
      .add(REGIONSERVER_COUNT_KEY, regionServerCount)
      .add(ROWS_COUNT_KEY, rowsToLoad)
      .add(FAILURE_PROB_KEY, failureProb)
      .toString());
  }

  private static void initConf(Configuration conf) {

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
    conf.setBoolean("hbase.table.sanity.checks", false);
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, 20000);

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
          if (chore == null) {
            chore = new MobFileCleanerChore();
          }
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

      Thread writeData = new Thread(new WriteData(rowsToLoad));
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

      if (util != null) {
        LOG.info("Archive cleaner started ...");
        // Call archive cleaner again
        util.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
        LOG.info("Archive cleaner finished");
      }

      scanTable();

    } finally {

      admin.disableTable(hdt.getTableName());
      admin.deleteTable(hdt.getTableName());
    }
    LOG.info("MOB Stress Test finished OK");
    printStats(rowsToLoad);

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
      assertEquals(rowsToLoad, counter);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("MOB Stress Test FAILED");
      if (util != null) {
        assertTrue(false);
      } else {
        System.exit(-1);
      }
    }
  }  
    
  /**
   *
   * @param args argument list
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    initConf(conf);
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestMobCompaction(), args);
    System.exit(status);
  }
}
