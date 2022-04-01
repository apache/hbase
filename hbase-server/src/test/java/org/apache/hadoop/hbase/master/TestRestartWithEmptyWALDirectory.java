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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Simulate the scenario described in HBASE-26245, where we clean the WAL directory and try to start
 * the cluster.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestRestartWithEmptyWALDirectory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestartWithEmptyWALDirectory.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("test");

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static byte[] QUALIFIER = Bytes.toBytes("qualifier");

  @BeforeClass
  public static void setUp() throws Exception {
    // in the test we shutdown the only master and after restarting its port will be changed, so the
    // default rpc region server can not work
    UTIL.getConfiguration().set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, FAMILY).close();
    UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRestart() throws IOException, InterruptedException {
    byte[] row = Bytes.toBytes(0);
    try (Table table = UTIL.getConnection().getTable(NAME)) {
      table.put(new Put(row).addColumn(FAMILY, QUALIFIER, row));
    }
    // flush all in memory data
    UTIL.flush(TableName.META_TABLE_NAME);
    UTIL.flush(NAME);

    // stop master first, so when stopping region server, we will not schedule a SCP.
    UTIL.getMiniHBaseCluster().stopMaster(0).join();
    UTIL.getMiniHBaseCluster().stopRegionServer(0).join();

    // let's cleanup the WAL directory
    UTIL.getTestFileSystem().delete(new Path(CommonFSUtils.getWALRootDir(UTIL.getConfiguration()),
      HConstants.HREGION_LOGDIR_NAME), true);

    // restart the cluster
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.waitTableAvailable(NAME);

    // the start up should succeed and the data should be persist
    try (Table table = UTIL.getConnection().getTable(NAME)) {
      assertArrayEquals(row, table.get(new Get(row)).getValue(FAMILY, QUALIFIER));
    }
  }
}
