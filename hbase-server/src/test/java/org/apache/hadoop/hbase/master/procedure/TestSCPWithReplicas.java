/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestSCPWithReplicas extends TestSCP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSCPWithReplicas.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestSCPWithReplicas.class);

  @Override
  protected void startMiniCluster() throws Exception {
    // Start a cluster with 4 nodes because we have 3 replicas.
    // So on a crash of a server still we can ensure that the
    // replicas are distributed.
    this.util.startMiniCluster(4);
  }

  @Override
  protected Table createTable(final TableName tableName) throws IOException {
    final Table t = this.util.createTable(tableName, HBaseTestingUtility.COLUMNS,
      HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE, 3);
    return t;
  }

  @Override
  protected void assertReplicaDistributed(final Table t) {
    // Assert all data came back.
    List<RegionInfo> regionInfos = new ArrayList<>();
    for (RegionServerThread rs : this.util.getMiniHBaseCluster().getRegionServerThreads()) {
      regionInfos.clear();
      for (Region r : rs.getRegionServer().getRegions(t.getName())) {
        LOG.info("The region is " + r.getRegionInfo() + " the location is "
            + rs.getRegionServer().getServerName());
        if (contains(regionInfos, r.getRegionInfo())) {
          LOG.error("Am exiting");
          fail("Crashed replica regions should not be assigned to same region server");
        } else {
          regionInfos.add(r.getRegionInfo());
        }
      }
    }
  }

  private boolean contains(List<RegionInfo> regionInfos, RegionInfo regionInfo) {
    for (RegionInfo info : regionInfos) {
      if (RegionReplicaUtil.isReplicasForSameRegion(info, regionInfo)) {
        return true;
      }
    }
    return false;
  }
}
