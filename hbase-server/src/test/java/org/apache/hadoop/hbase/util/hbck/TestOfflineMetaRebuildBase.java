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
package org.apache.hadoop.hbase.util.hbck;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This builds a table, removes info from meta, and then rebuilds meta.
 */
@Category(MediumTests.class)
public class TestOfflineMetaRebuildBase extends OfflineMetaRebuildTestCore {
  private static final Log LOG = LogFactory.getLog(TestOfflineMetaRebuildBase.class);

  @Test(timeout = 120000)
  public void testMetaRebuild() throws Exception {
    wipeOutMeta();

    // is meta really messed up?
    assertEquals(1, scanMeta());
    assertErrors(doFsck(conf, false),
        new ERROR_CODE[] {
            ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            ERROR_CODE.NOT_IN_META_OR_DEPLOYED});
    // Note, would like to check # of tables, but this takes a while to time
    // out.

    // shutdown the minicluster
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniZKCluster();

    // rebuild meta table from scratch
    HBaseFsck fsck = new HBaseFsck(conf);
    assertTrue(fsck.rebuildMeta(false));
    assertTrue("HBCK meta recovery WAL directory exist.", validateHBCKMetaRecoveryWALDir());

    // bring up the minicluster
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.restartHBaseCluster(3);
    validateMetaAndUserTableRows(1, 5);
  }

  @Test(timeout = 300000)
  public void testHMasterStartupOnMetaRebuild() throws Exception {
    // shutdown the minicluster
    TEST_UTIL.shutdownMiniHBaseCluster();

    // Assign meta in master and restart HBase cluster
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:meta");
    // Set namespace initialization timeout
    TEST_UTIL.getConfiguration().set("hbase.master.namespace.init.timeout", "150000");
    TEST_UTIL.restartHBaseCluster(3);
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();

    try {
      // Create namespace
      TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create("ns1").build());
      TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create("ns2").build());
      // Create tables
      TEST_UTIL.createTable(TableName.valueOf("ns1:testHMasterStartupOnMetaRebuild"),
        Bytes.toBytes("cf1"));
      TEST_UTIL.createTable(TableName.valueOf("ns2:testHMasterStartupOnMetaRebuild"),
        Bytes.toBytes("cf1"));

      // Flush meta
      TEST_UTIL.flush(TableName.META_TABLE_NAME);

      // HMaster graceful shutdown
      TEST_UTIL.getHBaseCluster().getMaster().shutdown();

      // Kill region servers
      List<RegionServerThread> regionServerThreads =
          TEST_UTIL.getHBaseCluster().getRegionServerThreads();
      for (RegionServerThread regionServerThread : regionServerThreads) {
        TEST_UTIL.getHBaseCluster()
            .killRegionServer(regionServerThread.getRegionServer().getServerName());
      }

      // rebuild meta table from scratch
      HBaseFsck fsck = new HBaseFsck(conf);
      assertTrue(fsck.rebuildMeta(false));

      // bring up the minicluster
      TEST_UTIL.restartHBaseCluster(3);
      validateMetaAndUserTableRows(3, 7);
    } finally {
      // Remove table and namesapce
      TEST_UTIL.deleteTable("ns1:testHMasterStartupOnMetaRebuild");
      TEST_UTIL.deleteTable("ns2:testHMasterStartupOnMetaRebuild");
      TEST_UTIL.getHBaseAdmin().deleteNamespace("ns1");
      TEST_UTIL.getHBaseAdmin().deleteNamespace("ns2");
    }
  }

  /*
   * Validate meta table region count and user table rows.
   */
  private void validateMetaAndUserTableRows(int totalTableCount, int totalRegionCount)
      throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Admin admin = connection.getAdmin();
      admin.enableTable(table);
      LOG.info("Waiting for no more RIT");
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      LOG.info("No more RIT in ZK, now doing final test verification");

      // everything is good again.
      assertEquals(totalRegionCount, scanMeta());
      HTableDescriptor[] htbls = admin.listTables();
      LOG.info("Tables present after restart: " + Arrays.toString(htbls));
      assertEquals(totalTableCount, htbls.length);
    }

    assertErrors(doFsck(conf, false), new ERROR_CODE[] {});
    LOG.info("Table " + table + " has " + tableRowCount(conf, table) + " entries.");
    assertEquals(16, tableRowCount(conf, table));
  }

  /**
   * Validate whether Meta recovery empty WAL directory is removed.
   * @return True if directory is removed otherwise false.
   */
  private boolean validateHBCKMetaRecoveryWALDir() throws IOException {
    Path rootdir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path walLogDir = new Path(rootdir, HConstants.HREGION_LOGDIR_NAME);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    FileStatus[] walFiles = FSUtils.listStatus(fs, walLogDir, null);
    assertNotNull(walFiles);
    for (FileStatus fsStat : walFiles) {
      if (fsStat.isDirectory() && fsStat.getPath().getName().startsWith("hregion-")) {
        return false;
      }
    }
    return true;
  }
}
