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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.HBaseTestingUtility;
/**
 * This builds a table, removes info from meta, and then rebuilds meta.
 */
@Category(MediumTests.class)
public class TestOfflineMetaRebuildBase extends OfflineMetaRebuildTestCore {

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
    HConnectionManager.deleteConnection(conf);

    // rebuild meta table from scratch
    HBaseFsck fsck = new HBaseFsck(conf);
    assertTrue(fsck.rebuildMeta(false));

    // bring up the minicluster
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.restartHBaseCluster(3);
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    
    LOG.info("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    LOG.info("No more RIT in ZK, now doing final test verification");

    // everything is good again.
    assertEquals(5, scanMeta());
    HTableDescriptor[] htbls = TEST_UTIL.getHBaseAdmin().listTables();
    LOG.info("Tables present after restart: " + Arrays.toString(htbls));

    assertEquals(1, htbls.length);
    assertErrors(doFsck(conf, false), new ERROR_CODE[] {});
    LOG.info("Table " + table + " has " + tableRowCount(conf, table)
        + " entries.");
    assertEquals(16, tableRowCount(conf, table));
  }


}

