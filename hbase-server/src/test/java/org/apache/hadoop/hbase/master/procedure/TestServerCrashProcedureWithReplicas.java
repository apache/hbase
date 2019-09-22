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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestServerCrashProcedureWithReplicas extends TestServerCrashProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerCrashProcedureWithReplicas.class);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestServerCrashProcedureWithReplicas.class);

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

  private boolean contains(List<RegionInfo> regionInfos, RegionInfo regionInfo) {
    for (RegionInfo info : regionInfos) {
      if (RegionReplicaUtil.isReplicasForSameRegion(info, regionInfo)) {
        return true;
      }
    }
    return false;
  }
}
