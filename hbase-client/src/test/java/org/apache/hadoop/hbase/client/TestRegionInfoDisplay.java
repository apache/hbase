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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestRegionInfoDisplay {

  @Test
  public void testRegionDetailsForDisplay(TestInfo testInfo) throws IOException {
    String name = testInfo.getTestMethod().get().getName();
    byte[] startKey = new byte[] { 0x01, 0x01, 0x02, 0x03 };
    byte[] endKey = new byte[] { 0x01, 0x01, 0x02, 0x04 };
    Configuration conf = new Configuration();
    conf.setBoolean("hbase.display.keys", false);
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.valueOf(name)).setStartKey(startKey)
      .setEndKey(endKey).build();
    checkEquality(ri, conf);
    // check HRIs with non-default replicaId
    ri =
      RegionInfoBuilder.newBuilder(TableName.valueOf(name)).setStartKey(startKey).setEndKey(endKey)
        .setSplit(false).setRegionId(EnvironmentEdgeManager.currentTime()).setReplicaId(1).build();
    checkEquality(ri, conf);
    assertArrayEquals(RegionInfoDisplay.HIDDEN_END_KEY,
      RegionInfoDisplay.getEndKeyForDisplay(ri, conf));
    assertArrayEquals(RegionInfoDisplay.HIDDEN_START_KEY,
      RegionInfoDisplay.getStartKeyForDisplay(ri, conf));

    RegionState state = RegionState.createForTesting(ri, RegionState.State.OPEN);
    String descriptiveNameForDisplay =
      RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf);
    String originalDescriptive = state.toDescriptiveString();
    checkDescriptiveNameEquality(descriptiveNameForDisplay, originalDescriptive, startKey);

    conf.setBoolean("hbase.display.keys", true);
    assertArrayEquals(endKey, RegionInfoDisplay.getEndKeyForDisplay(ri, conf));
    assertArrayEquals(startKey, RegionInfoDisplay.getStartKeyForDisplay(ri, conf));
    assertEquals(originalDescriptive,
      RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf));
  }

  private void checkDescriptiveNameEquality(String descriptiveNameForDisplay, String origDesc,
    byte[] startKey) {
    // except for the "hidden-start-key" substring everything else should exactly match
    String firstPart = descriptiveNameForDisplay.substring(0, descriptiveNameForDisplay
      .indexOf(new String(RegionInfoDisplay.HIDDEN_START_KEY, StandardCharsets.UTF_8)));
    String secondPart = descriptiveNameForDisplay.substring(descriptiveNameForDisplay
      .indexOf(new String(RegionInfoDisplay.HIDDEN_START_KEY, StandardCharsets.UTF_8))
      + RegionInfoDisplay.HIDDEN_START_KEY.length);
    String firstPartOrig = origDesc.substring(0, origDesc.indexOf(Bytes.toStringBinary(startKey)));
    String secondPartOrig = origDesc.substring(
      origDesc.indexOf(Bytes.toStringBinary(startKey)) + Bytes.toStringBinary(startKey).length());
    assertTrue(firstPart.equals(firstPartOrig));
    assertTrue(secondPart.equals(secondPartOrig));
  }

  private void checkEquality(RegionInfo ri, Configuration conf) throws IOException {
    byte[] modifiedRegionName = RegionInfoDisplay.getRegionNameForDisplay(ri, conf);
    System.out.println(Bytes.toString(modifiedRegionName) + " " + ri.toString());
    byte[][] modifiedRegionNameParts = RegionInfo.parseRegionName(modifiedRegionName);
    byte[][] regionNameParts = RegionInfo.parseRegionName(ri.getRegionName());

    // same number of parts
    assert (modifiedRegionNameParts.length == regionNameParts.length);
    for (int i = 0; i < regionNameParts.length; i++) {
      // all parts should match except for [1] where in the modified one,
      // we should have "hidden_start_key"
      if (i != 1) {
        System.out.println("" + i + " " + Bytes.toString(regionNameParts[i]) + " "
          + Bytes.toString(modifiedRegionNameParts[i]));
        assertArrayEquals(regionNameParts[i], modifiedRegionNameParts[i]);
      } else {
        System.out.println("" + i + " " + Bytes.toString(regionNameParts[i]) + " "
          + Bytes.toString(modifiedRegionNameParts[i]));
        assertNotEquals(regionNameParts[i], modifiedRegionNameParts[i]);
        assertArrayEquals(modifiedRegionNameParts[1],
          RegionInfoDisplay.getStartKeyForDisplay(ri, conf));
      }
    }
  }
}
