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

package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the case where a meta region is opened in one regionserver and closed, there should not
 * be any WALs left over.
 */
@Category({MediumTests.class})
public class TestMetaWALsAreClosed {
  protected static final Log LOG = LogFactory.getLog(TestMetaWALsAreClosed.class);

  protected static final int NUM_RS = 2;

  protected static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();


  protected final Configuration conf = TEST_UTIL.getConfiguration();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1, NUM_RS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private boolean isHostingMeta(FileSystem fs, Path wals, ServerName serverName)
      throws IOException {
    for (FileStatus status : fs.listStatus(wals)) {
      LOG.info(status.getPath());
      if (DefaultWALProvider.isMetaFile(status.getPath())) {
        return true; // only 1 meta region for now
      }
    }
    return false;
  }

  private void moveMetaRegionAndWait(final ServerName target) throws Exception {
    try (final Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        final Admin admin = conn.getAdmin();
        final RegionLocator rl = conn.getRegionLocator(TableName.META_TABLE_NAME)) {

      LOG.info("Disabling balancer");
      admin.setBalancerRunning(false, true);

      LOG.info("Moving meta region");
      admin.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
          Bytes.toBytes(target.toString()));

      LOG.info("Waiting for meta region to move");
      // wait for the move of meta region
      TEST_UTIL.waitFor(30000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return target.equals(
              rl.getRegionLocation(HConstants.EMPTY_START_ROW, true).getServerName());
        }
      });
    }
  }

  @Test (timeout = 60000)
  public void testMetaWALsAreClosed() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();

    FileSystem fs = TEST_UTIL.getTestFileSystem();

    // find the region server hosting the meta table now.
    ServerName metaServerName = null;
    ServerName otherServerName = null;
    for (RegionServerThread rs : cluster.getRegionServerThreads()) {
      ServerName serverName = rs.getRegionServer().getServerName();

      Path wals = new Path(FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
        DefaultWALProvider.getWALDirectoryName(serverName.toString()));

      if (isHostingMeta(fs, wals, serverName)) {
        metaServerName = serverName; // only 1 meta region for now
      } else {
        otherServerName = serverName;
      }
    }

    LOG.info(metaServerName);
    LOG.info(otherServerName);
    assertNotNull(metaServerName);
    assertNotNull(otherServerName);

    moveMetaRegionAndWait(otherServerName);

    LOG.info("Checking that old meta server does not have WALs for meta");
    // the server that used to host meta now should not have any WAL files for the meta region now
    Path wals = new Path(FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
      DefaultWALProvider.getWALDirectoryName(metaServerName.toString()));
    for (FileStatus status : fs.listStatus(wals)) {
      LOG.info(status.getPath());
      assertFalse(DefaultWALProvider.isMetaFile(status.getPath()));
    }

    // assign the meta server back
    moveMetaRegionAndWait(metaServerName);

    // do some basic operations to ensure that nothing is failing
    HTableDescriptor htd = TEST_UTIL.createTableDescriptor("foo");
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table table = conn.getTable(htd.getTableName())) {

      TEST_UTIL.loadNumericRows(table, TEST_UTIL.fam1, 0, 100);
      TEST_UTIL.verifyNumericRows(table, TEST_UTIL.fam1, 0, 100, 0);
    }
  }
}
