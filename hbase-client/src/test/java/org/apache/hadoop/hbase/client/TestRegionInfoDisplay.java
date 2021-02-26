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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionInfoDisplay {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionInfoDisplay.class);

  @Rule public TestName name = new TestName();

  @Test
  public void testRegionDetailsForDisplay() throws IOException {
    byte[] startKey = new byte[] {0x01, 0x01, 0x02, 0x03};
    byte[] endKey = new byte[] {0x01, 0x01, 0x02, 0x04};
    Configuration conf = new Configuration();
    conf.setBoolean("hbase.display.keys", false);
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setStartKey(startKey).setEndKey(endKey).build();
    checkEquality(ri, conf);
    // check HRIs with non-default replicaId
    ri = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
    .setStartKey(startKey)
    .setEndKey(endKey)
    .setSplit(false)
    .setRegionId(System.currentTimeMillis())
    .setReplicaId(1).build();
    checkEquality(ri, conf);
    Assert.assertArrayEquals(RegionInfoDisplay.HIDDEN_END_KEY,
        RegionInfoDisplay.getEndKeyForDisplay(ri, conf));
    Assert.assertArrayEquals(RegionInfoDisplay.HIDDEN_START_KEY,
        RegionInfoDisplay.getStartKeyForDisplay(ri, conf));

    RegionState state = RegionState.createForTesting(convert(ri), RegionState.State.OPEN);
    String descriptiveNameForDisplay =
        RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf);
    String originalDescriptive = state.toDescriptiveString();
    checkDescriptiveNameEquality(descriptiveNameForDisplay, originalDescriptive, startKey);

    conf.setBoolean("hbase.display.keys", true);
    Assert.assertArrayEquals(endKey, RegionInfoDisplay.getEndKeyForDisplay(ri, conf));
    Assert.assertArrayEquals(startKey, RegionInfoDisplay.getStartKeyForDisplay(ri, conf));
    Assert.assertEquals(originalDescriptive,
        RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf));
  }

  private void checkDescriptiveNameEquality(String descriptiveNameForDisplay, String origDesc,
      byte[] startKey) {
    // except for the "hidden-start-key" substring everything else should exactly match
    String firstPart = descriptiveNameForDisplay.substring(0,
        descriptiveNameForDisplay.indexOf(
        new String(RegionInfoDisplay.HIDDEN_START_KEY, StandardCharsets.UTF_8)));
    String secondPart = descriptiveNameForDisplay.substring(
        descriptiveNameForDisplay.indexOf(
        new String(RegionInfoDisplay.HIDDEN_START_KEY, StandardCharsets.UTF_8)) +
            RegionInfoDisplay.HIDDEN_START_KEY.length);
    String firstPartOrig = origDesc.substring(0, origDesc.indexOf(Bytes.toStringBinary(startKey)));
    String secondPartOrig = origDesc.substring(
        origDesc.indexOf(Bytes.toStringBinary(startKey)) +
            Bytes.toStringBinary(startKey).length());
    assert(firstPart.equals(firstPartOrig));
    assert(secondPart.equals(secondPartOrig));
  }

  private void checkEquality(RegionInfo ri, Configuration conf) throws IOException {
    byte[] modifiedRegionName = RegionInfoDisplay.getRegionNameForDisplay(ri, conf);
    System.out.println(Bytes.toString(modifiedRegionName) + " " + ri.toString());
    byte[][] modifiedRegionNameParts = RegionInfo.parseRegionName(modifiedRegionName);
    byte[][] regionNameParts = RegionInfo.parseRegionName(ri.getRegionName());

    //same number of parts
    assert(modifiedRegionNameParts.length == regionNameParts.length);
    for (int i = 0; i < regionNameParts.length; i++) {
      // all parts should match except for [1] where in the modified one,
      // we should have "hidden_start_key"
      if (i != 1) {
        System.out.println("" + i + " " + Bytes.toString(regionNameParts[i]) + " " +
          Bytes.toString(modifiedRegionNameParts[i]));
        Assert.assertArrayEquals(regionNameParts[i], modifiedRegionNameParts[i]);
      } else {
        System.out.println("" + i + " " + Bytes.toString(regionNameParts[i]) + " " +
          Bytes.toString(modifiedRegionNameParts[i]));
        Assert.assertNotEquals(regionNameParts[i], modifiedRegionNameParts[i]);
        Assert.assertArrayEquals(modifiedRegionNameParts[1],
          RegionInfoDisplay.getStartKeyForDisplay(ri, conf));
      }
    }
  }

  private HRegionInfo convert(RegionInfo ri) {
    HRegionInfo hri =new HRegionInfo(ri.getTable(), ri.getStartKey(), ri.getEndKey(),
        ri.isSplit(), ri.getRegionId());
    hri.setOffline(ri.isOffline());
    return hri;
  }
}
