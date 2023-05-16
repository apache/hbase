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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestEditsStuckBehindDroppedTable extends ReplicationDroppedTablesTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEditsStuckBehindDroppedTable.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupClusters(false);
  }

  @Test
  public void testEditsStuckBehindDroppedTable() throws Exception {
    // Sanity check Make sure by default edits for dropped tables stall the replication queue, even
    // when the table(s) in question have been deleted on both ends.
    testEditsBehindDroppedTable("test_dropped");
  }
}
