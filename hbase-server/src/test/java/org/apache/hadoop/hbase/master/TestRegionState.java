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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestRegionState {
  private String testMethodName;

  @BeforeEach
  public void setTestMethod(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().get().getName();
  }

  @Test
  public void testSerializeDeserialize() {
    final TableName tableName = TableName.valueOf("testtb");
    for (RegionState.State state : RegionState.State.values()) {
      testSerializeDeserialize(tableName, state);
    }
  }

  private void testSerializeDeserialize(final TableName tableName, final RegionState.State state) {
    RegionState state1 =
      RegionState.createForTesting(RegionInfoBuilder.newBuilder(tableName).build(), state);
    ClusterStatusProtos.RegionState protobuf1 = state1.convert();
    RegionState state2 = RegionState.convert(protobuf1);
    ClusterStatusProtos.RegionState protobuf2 = state1.convert();
    assertEquals(state1, state2, "RegionState does not match " + state);
    assertEquals(protobuf1, protobuf2, "Protobuf does not match " + state);
  }
}
