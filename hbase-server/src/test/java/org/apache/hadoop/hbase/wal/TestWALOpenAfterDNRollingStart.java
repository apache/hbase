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
package org.apache.hadoop.hbase.wal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MediumTests.class)
public class TestWALOpenAfterDNRollingStart {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static long DataNodeRestartInterval;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Sleep time before restart next dn, we need to wait the current dn to finish start up
    DataNodeRestartInterval = 15000;
    // interval of checking low replication. The sleep time must smaller than DataNodeRestartInterval
    // so a low replication case will be detected and the wal will be rolled
    long checkLowReplicationInterval = 10000;
    //don't let hdfs client to choose a new replica when dn down
    TEST_UTIL.getConfiguration().setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable",
        false);
    TEST_UTIL.getConfiguration().setLong("hbase.regionserver.hlog.check.lowreplication.interval",
        checkLowReplicationInterval);
    TEST_UTIL.startMiniDFSCluster(3);
    TEST_UTIL.startMiniCluster(1);

  }

  /**
   * see HBASE-18132
   * This is a test case of failing open a wal(for replication for example) after all datanode
   * restarted (rolling upgrade, for example).
   * Before this patch, low replication detection is only used when syncing wal.
   * But if the wal haven't had any entry whiten, it will never know all the replica of the wal
   * is broken(because of dn restarting). And this wal can never be open
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void test() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    FSHLog hlog = (FSHLog)server.getWAL(null);
    Path currentFile = hlog.getCurrentFileName();
    //restart every dn to simulate a dn rolling upgrade
    for(int i = 0; i < TEST_UTIL.getDFSCluster().getDataNodes().size(); i++) {
      //This is NOT a bug, when restart dn in miniDFSCluster, it will remove the stopped dn from
      //the dn list and then add to the tail of this list, we need to always restart the first one
      //to simulate rolling upgrade of every dn.
      TEST_UTIL.getDFSCluster().restartDataNode(0);
      //sleep enough time so log roller can detect the pipeline break and roll log
      Thread.sleep(DataNodeRestartInterval);
    }

    //if the log is not rolled, then we can never open this wal forever.
    WAL.Reader reader = WALFactory
        .createReader(TEST_UTIL.getTestFileSystem(), currentFile, TEST_UTIL.getConfiguration());
  }


}
