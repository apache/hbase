/*
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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupHandler.BACKUPSTATUS;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.regionserver.LogRollRegionServerProcedureManager;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * This class is only a base for other integration-level backup tests.
 * Do not add tests here.
 * TestBackupSmallTests is where tests that don't require bring machines up/down should go
 * All other tests should have their own classes and extend this one
 */
public class TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestBackupBase.class);

  protected static Configuration conf1;
  protected static Configuration conf2;

  protected static HBaseTestingUtility TEST_UTIL;
  protected static HBaseTestingUtility TEST_UTIL2;

  protected static TableName table1;
  protected static TableName table2;
  protected static TableName table3;
  protected static TableName table4;

  protected static String table1_restore = "table1_restore";
  protected static String table2_restore = "table2_restore";
  protected static String table3_restore = "table3_restore";
  protected static String table4_restore = "table4_restore";

  protected static final int NB_ROWS_IN_BATCH = 100;
  protected static final byte[] qualName = Bytes.toBytes("q1");
  protected static final byte[] famName = Bytes.toBytes("f");

  protected static String BACKUP_ROOT_DIR = "/backupUT";
  protected static String BACKUP_REMOTE_ROOT_DIR = "/backupUT";

  protected static final String BACKUP_ZNODE = "/backup/hbase";
  protected static final String BACKUP_SUCCEED_NODE = "complete";
  protected static final String BACKUP_FAILED_NODE = "failed";


  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set("hbase.procedure.regionserver.classes",
      LogRollRegionServerProcedureManager.class.getName());
    TEST_UTIL.getConfiguration().set("hbase.procedure.master.classes",
      LogRollMasterProcedureManager.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    TEST_UTIL.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = TEST_UTIL.getZkCluster();

    conf1 = TEST_UTIL.getConfiguration();
    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    TEST_UTIL2 = new HBaseTestingUtility(conf2);
    TEST_UTIL2.setZkCluster(miniZK);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL2.startMiniCluster();
    conf1 = TEST_UTIL.getConfiguration();

    TEST_UTIL.startMiniMapReduceCluster();
    BACKUP_ROOT_DIR = TEST_UTIL.getConfiguration().get("fs.defaultFS") + "/backupUT";
    LOG.info("ROOTDIR " + BACKUP_ROOT_DIR);
    BACKUP_REMOTE_ROOT_DIR = TEST_UTIL2.getConfiguration().get("fs.defaultFS") + "/backupUT";
    LOG.info("REMOTE ROOTDIR " + BACKUP_REMOTE_ROOT_DIR);

    BackupClient.setConf(conf1);
    RestoreClient.setConf(conf1);
    createTables();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
    //zkw1.close();
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  protected static void loadTable(HTable table) throws Exception {

    Put p; // 100 + 1 row to t1_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      table.put(p);
    }
  }

  protected static void createTables() throws Exception {

    long tid = System.currentTimeMillis();
    table1 = TableName.valueOf("test-" + tid);
    HBaseAdmin ha = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor desc = new HTableDescriptor(table1);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    desc.addFamily(fam);
    ha.createTable(desc);
    Connection conn = ConnectionFactory.createConnection(conf1);    
    HTable table = (HTable) conn.getTable(table1);
    loadTable(table);
    table.close();
    table2 = TableName.valueOf("test-" + tid + 1);
    desc = new HTableDescriptor(table2);
    desc.addFamily(fam);
    ha.createTable(desc);
    table = (HTable) conn.getTable(table2);
    loadTable(table);
    table.close();
    table3 = TableName.valueOf("test-" + tid + 2);
    table = TEST_UTIL.createTable(table3, famName);
    table.close();
    table4 = TableName.valueOf("test-" + tid + 3);
    table = TEST_UTIL.createTable(table4, famName);
    table.close();
    ha.close();
    conn.close();
  }

  protected boolean checkSucceeded(String backupId) throws IOException
  {
    BackupContext status = getBackupContext(backupId);
    if(status == null) return false;
    return status.getFlag() == BACKUPSTATUS.COMPLETE;
  }

  protected boolean checkFailed(String backupId) throws IOException
  {
    BackupContext status = getBackupContext(backupId);
    if(status == null) return false;
    return status.getFlag() == BACKUPSTATUS.FAILED;
  }

  private BackupContext getBackupContext(String backupId) throws IOException
  {
    Configuration conf = BackupClient.getConf();
    BackupSystemTable table = BackupSystemTable.getTable(conf);
    BackupContext status =  table.readBackupStatus(backupId);
    return status;
  }
}

