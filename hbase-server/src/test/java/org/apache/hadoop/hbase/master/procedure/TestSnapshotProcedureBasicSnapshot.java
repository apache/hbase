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

package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureBasicSnapshot extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureBasicSnapshot.class);

  @Test
  public void testSimpleSnapshotTable() throws Exception {
    TEST_UTIL.getAdmin().snapshot(snapshot);
    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test(expected = org.apache.hadoop.hbase.snapshot.SnapshotCreationException.class)
  public void testClientTakingTwoSnapshotOnSameTable() throws Exception {
    Thread first = new Thread("first-client") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshot);
        } catch (IOException e) {
          LOG.error("first client failed taking snapshot", e);
          fail("first client failed taking snapshot");
        }
      }
    };
    first.start();
    Thread.sleep(1000);
    // we don't allow different snapshot with same name
    SnapshotDescription snapshotWithSameName =
      new SnapshotDescription(SNAPSHOT_NAME, TABLE_NAME, SnapshotType.SKIPFLUSH);
    TEST_UTIL.getAdmin().snapshot(snapshotWithSameName);
  }

  @Test(expected = org.apache.hadoop.hbase.snapshot.SnapshotCreationException.class)
  public void testClientTakeSameSnapshotTwice() throws IOException, InterruptedException {
    Thread first = new Thread("first-client") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshot);
        } catch (IOException e) {
          LOG.error("first client failed taking snapshot", e);
          fail("first client failed taking snapshot");
        }
      }
    };
    first.start();
    Thread.sleep(1000);
    TEST_UTIL.getAdmin().snapshot(snapshot);
  }
}
