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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionState {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionState.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testSerializeDeserialize() {
    final TableName tableName = TableName.valueOf("testtb");
    for (RegionState.State state: RegionState.State.values()) {
      testSerializeDeserialize(tableName, state);
    }
  }

  private void testSerializeDeserialize(final TableName tableName, final RegionState.State state) {
    RegionState state1 = RegionState.createForTesting(new HRegionInfo(tableName), state);
    ClusterStatusProtos.RegionState protobuf1 = state1.convert();
    RegionState state2 = RegionState.convert(protobuf1);
    ClusterStatusProtos.RegionState protobuf2 = state1.convert();
    assertEquals("RegionState does not match " + state, state1, state2);
    assertEquals("Protobuf does not match " + state, protobuf1, protobuf2);
  }
}
