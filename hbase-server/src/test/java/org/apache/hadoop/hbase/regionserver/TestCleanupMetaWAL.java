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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.SPLITTING_EXT;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestCleanupMetaWAL {
  private static final Logger LOG = LoggerFactory.getLogger(TestCleanupMetaWAL.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCleanupMetaWAL.class);

  @BeforeClass
  public static void before() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testCleanupMetaWAL() throws Exception {
    TEST_UTIL.createTable(TableName.valueOf("test"), "cf");
    HRegionServer serverWithMeta = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(TEST_UTIL.getMiniHBaseCluster().getServerWithMeta());
    RegionInfo metaInfo = TEST_UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME).get(0);
    TEST_UTIL.getAdmin().move(metaInfo.getEncodedNameAsBytes());
    LOG.info("KILL");
    TEST_UTIL.getMiniHBaseCluster().killRegionServer(serverWithMeta.getServerName());
    LOG.info("WAIT");
    TEST_UTIL.waitFor(30000, () ->
        TEST_UTIL.getMiniHBaseCluster().getMaster().getProcedures().stream()
            .filter(p -> p instanceof ServerCrashProcedure && p.isFinished()).count() > 0);
    LOG.info("DONE WAITING");
    MasterFileSystem fs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path walPath = new Path(fs.getWALRootDir(), HConstants.HREGION_LOGDIR_NAME);
    for (FileStatus status : CommonFSUtils.listStatus(fs.getFileSystem(), walPath)) {
      if (status.getPath().toString().contains(SPLITTING_EXT)) {
        fail("Should not have splitting wal dir here:" + status);
      }
    }
  }
}
