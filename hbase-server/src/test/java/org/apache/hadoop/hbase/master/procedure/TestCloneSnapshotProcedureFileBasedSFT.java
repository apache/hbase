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
package org.apache.hadoop.hbase.master.procedure;

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.TRACKER_IMPL;
import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.Trackers.FILE;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestCloneSnapshotProcedureFileBasedSFT extends TestCloneSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloneSnapshotProcedureFileBasedSFT.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().set(TRACKER_IMPL, FILE.name());
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(1);
  }
}
