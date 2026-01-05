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
package org.apache.hadoop.hbase.master;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Testcase for HBASE-29797, where the lazy initialized WALProvider may recreate the WAL directory
 * and cause our fencing way loses efficacy.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestWALFencing {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMoveMeta() throws Exception {
    HRegionServer metaRs = UTIL.getRSForFirstRegionInTable(TableName.META_TABLE_NAME);
    HRegionServer otherRs = UTIL.getOtherRegionServer(metaRs);
    // do fencing here, i.e, kill otherRs
    Path splittingDir = UTIL.getMiniHBaseCluster().getMaster().getMasterWalManager()
      .getLogDirs(Collections.singleton(otherRs.getServerName())).get(0);
    for (FileStatus walFile : otherRs.getWALFileSystem().listStatus(splittingDir)) {
      RecoverLeaseFSUtils.recoverFileLease(otherRs.getWALFileSystem(), walFile.getPath(),
        otherRs.getConfiguration());
    }
    // move meta region to otherRs, which should fail and crash otherRs, and then master will try to
    // assign meta region to another rs
    RegionInfo metaRegionInfo = metaRs.getRegions().stream().map(Region::getRegionInfo)
      .filter(RegionInfo::isMetaRegion).findAny().get();
    UTIL.getAdmin().move(metaRegionInfo.getRegionName(), otherRs.getServerName());
    // make sure that meta region is not on otherRs
    await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(6))
      .until(() -> otherRs.getRegions(TableName.META_TABLE_NAME).isEmpty());
  }
}
