/*
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
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduction for MOB data loss 1. Settings: Region Size 200 MB, Flush threshold 800 KB. 2. Insert
 * 10 Million records 3. MOB Compaction and Archiver a) Trigger MOB Compaction (every 2 minutes) b)
 * Trigger major compaction (every 2 minutes) c) Trigger archive cleaner (every 3 minutes) 4.
 * Validate MOB data after complete data load. This class is used by MobStressTool only. This is not
 * a unit test
 */
public class MobStressToolRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MobStressToolRunner.class);

  private HBaseTestingUtil HTU;

  private final static String famStr = "f1";
  private final static byte[] fam = Bytes.toBytes(famStr);
  private final static byte[] qualifier = Bytes.toBytes("q1");
  private final static long mobLen = 10;
  private final static byte[] mobVal = Bytes
    .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  private Configuration conf;
  private TableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor familyDescriptor;
  private Admin admin;
  private long count = 500000;
  private double failureProb = 0.1;
  private Table table = null;

  private static volatile boolean run = true;

  public MobStressToolRunner() {
  }

  public void init(Configuration conf, long numRows) throws IOException {
    this.conf = conf;
    this.count = numRows;
    initConf();
    printConf();
    Connection conn = ConnectionFactory.createConnection(this.conf);
    this.admin = conn.getAdmin();
    this.familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(fam).setMobEnabled(true)
      .setMobThreshold(mobLen).setMaxVersions(1).build();
    this.tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("testMobCompactTable"))
        .setColumnFamily(familyDescriptor).build();
    if (admin.tableExists(tableDescriptor.getTableName())) {
      admin.disableTable(tableDescriptor.getTableName());
      admin.deleteTable(tableDescriptor.getTableName());
    }
    admin.createTable(tableDescriptor);
    table = conn.getTable(tableDescriptor.getTableName());
  }

  private void printConf() {
    LOG.info("Please ensure the following HBase configuration is set:");
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
    LOG.warn("hbase.mob.compaction.fault.probability=x, where x is between 0. and 1.");

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
    conf.setDouble("hbase.mob.compaction.fault.probability", failureProb);
    // conf.set(MobStoreEngine.DEFAULT_MOB_COMPACTOR_CLASS_KEY,
    // FaultyMobStoreCompactor.class.getName());
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, 120000);
    conf.set(MobConstants.MOB_COMPACTION_TYPE_KEY, MobConstants.OPTIMIZED_MOB_COMPACTION_TYPE);
    conf.setLong(MobConstants.MOB_COMPACTION_MAX_FILE_SIZE_KEY, 1000000);

  }

  class MajorCompaction implements Runnable {

    @Override
    public void run() {
      while (run) {
        try {
          admin.majorCompact(tableDescriptor.getTableName(), fam);
          Thread.sleep(120000);
        } catch (Exception e) {
          LOG.error("MOB Stress Test FAILED", e);
          ExitHandler.getInstance().exit(-1);
        }
      }
    }
  }

  class CleanMobAndArchive implements Runnable {

    @Override
    public void run() {
      while (run) {
        try {
          LOG.info("MOB cleanup started ...");
          MobFileCleanupUtil.cleanupObsoleteMobFiles(conf, table.getName(), admin);
          LOG.info("MOB cleanup finished");

          Thread.sleep(130000);
        } catch (Exception e) {
          LOG.error("CleanMobAndArchive", e);
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

        // Put Operation
        for (int i = 0; i < rows; i++) {
          byte[] key = Bytes.toBytes(i);
          Put p = new Put(key);
          p.addColumn(fam, qualifier, Bytes.add(key, mobVal));
          table.put(p);
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
        admin.flush(table.getName());
        run = false;
      } catch (Exception e) {
        LOG.error("MOB Stress Test FAILED", e);
        ExitHandler.getInstance().exit(-1);
      }
    }
  }

  public void runStressTest() throws InterruptedException, IOException {

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
      MobFileCleanupUtil.cleanupObsoleteMobFiles(conf, table.getName(), admin);
      getNumberOfMobFiles(conf, table.getName(), new String(fam));

      if (HTU != null) {
        LOG.info("Archive cleaner started ...");
        // Call archive cleaner again
        HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
        LOG.info("Archive cleaner finished");
      }

      scanTable();

    } finally {

      admin.disableTable(tableDescriptor.getTableName());
      admin.deleteTable(tableDescriptor.getTableName());
    }
    LOG.info("MOB Stress Test finished OK");
    printStats(count);

  }

  private long getNumberOfMobFiles(Configuration conf, TableName tableName, String family)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dir = MobUtils.getMobFamilyPath(conf, tableName, family);
    FileStatus[] stat = fs.listStatus(dir);
    long size = 0;
    for (FileStatus st : stat) {
      LOG.debug("MOB Directory content: {} len={}", st.getPath(), st.getLen());
      size += st.getLen();
    }
    LOG.debug("MOB Directory content total files: {}, total size={}", stat.length, size);

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
        byte[] key = result.getRow();
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), Bytes.add(key, mobVal)));
        if (counter % 10000 == 0) {
          LOG.info("GET=" + counter + " key=" + Bytes.toInt(key));
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
        ExitHandler.getInstance().exit(-1);
      }
    }
  }
}
