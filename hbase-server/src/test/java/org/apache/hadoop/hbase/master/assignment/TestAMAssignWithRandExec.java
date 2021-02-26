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
package org.apache.hadoop.hbase.master.assignment;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestAMAssignWithRandExec extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAMAssignWithRandExec.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAMAssignWithRandExec.class);

  @Test
  public void testAssignWithRandExec() throws Exception {
    TableName tableName = TableName.valueOf("testAssignWithRandExec");
    RegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new RandRsExecutor());
    // Loop a bunch of times so we hit various combos of exceptions.
    for (int i = 0; i < 10; i++) {
      LOG.info("ROUND=" + i);
      TransitRegionStateProcedure proc = createAssignProcedure(hri);
      waitOnFuture(submitProcedure(proc));
    }
  }
}
