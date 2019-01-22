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

import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;

@Category(MediumTests.class)
public class TestCleanupMetaWAL {
  private static final Logger LOG = LoggerFactory.getLogger(TestCleanupMetaWAL.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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
    TEST_UTIL.getHBaseAdmin()
        .move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(), null);
    TEST_UTIL.getMiniHBaseCluster().killRegionServer(serverWithMeta.getServerName());
    int count = 0;
    boolean scpFinished = false;
    while(count < 25 && !scpFinished) {
      List<ProcedureInfo> procs = TEST_UTIL.getMiniHBaseCluster().getMaster().listProcedures();
      for(ProcedureInfo pi : procs) {
        if(pi.getProcName().startsWith("ServerCrashProcedure") && pi.getProcState() ==
            ProcedureProtos.ProcedureState.FINISHED){
          LOG.info("SCP is finished: " + pi.getProcName());
          scpFinished = true;
          break;
        }
      }
      Thread.sleep(1000);
      count++;
    }

    MasterFileSystem fs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path walPath = new Path(fs.getWALRootDir(), HConstants.HREGION_LOGDIR_NAME);
    for (FileStatus status : FSUtils.listStatus(fs.getFileSystem(), walPath)) {
      if (status.getPath().toString().contains(DefaultWALProvider.SPLITTING_EXT)) {
        fail("Should not have splitting wal dir here:" + status);
      }
    }
  }
}