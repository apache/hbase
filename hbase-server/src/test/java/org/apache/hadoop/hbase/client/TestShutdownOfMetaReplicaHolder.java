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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
public class TestShutdownOfMetaReplicaHolder extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestShutdownOfMetaReplicaHolder.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestShutdownOfMetaReplicaHolder.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testShutdownOfReplicaHolder() throws Exception {
    // checks that the when the server holding meta replica is shut down, the meta replica
    // can be recovered
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      RegionLocator locator = conn.getRegionLocator(TableName.META_TABLE_NAME)) {
      HRegionLocation hrl = locator.getRegionLocations(HConstants.EMPTY_START_ROW, true).get(1);
      ServerName oldServer = hrl.getServerName();
      TEST_UTIL.getHBaseClusterInterface().killRegionServer(oldServer);
      LOG.debug("Waiting for the replica {} to come up", hrl.getRegion());
      TEST_UTIL.waitFor(30000, () -> {
        HRegionLocation loc = locator.getRegionLocations(HConstants.EMPTY_START_ROW, true).get(1);
        return loc != null && !loc.getServerName().equals(oldServer);
      });
      LOG.debug("Replica {} is online on {}, old server is {}", hrl.getRegion(),
        locator.getRegionLocations(HConstants.EMPTY_START_ROW, true).get(1).getServerName(),
        oldServer);
    }
  }
}
