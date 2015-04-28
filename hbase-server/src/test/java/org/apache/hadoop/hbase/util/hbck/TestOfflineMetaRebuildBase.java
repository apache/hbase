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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
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

    // bring up the minicluster
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.restartHBaseCluster(3);
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Admin admin = connection.getAdmin();
      admin.enableTable(table);
      LOG.info("Waiting for no more RIT");
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      LOG.info("No more RIT in ZK, now doing final test verification");

      // everything is good again.
      assertEquals(5, scanMeta());
      HTableDescriptor[] htbls = admin.listTables();
      LOG.info("Tables present after restart: " + Arrays.toString(htbls));
      assertEquals(1, htbls.length);
    }

    assertErrors(doFsck(conf, false), new ERROR_CODE[] {});
    LOG.info("Table " + table + " has " + tableRowCount(conf, table) + " entries.");
    assertEquals(16, tableRowCount(conf, table));
  }
}
