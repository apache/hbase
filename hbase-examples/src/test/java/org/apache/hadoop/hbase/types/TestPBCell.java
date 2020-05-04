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
package org.apache.hadoop.hbase.types;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.example.protobuf.generated.CellMessage;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

@Category({ SmallTests.class, MiscTests.class })
public class TestPBCell {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestPBCell.class);

  private static final PBCell CODEC = new PBCell();

  /**
   * Basic test to verify utility methods in {@link PBType} and delegation to protobuf works.
   */
  @Test
  public void testRoundTrip() {
    CellMessage.Cell cell =
      CellMessage.Cell.newBuilder().setRow(ByteString.copyFromUtf8("row")).build();
    PositionedByteRange pbr = new SimplePositionedByteRange(cell.getSerializedSize());
    pbr.setPosition(0);
    int encodedLength = CODEC.encode(pbr, cell);
    pbr.setPosition(0);
    CellMessage.Cell decoded = CODEC.decode(pbr);
    assertEquals(encodedLength, pbr.getPosition());
    assertEquals("row", decoded.getRow().toStringUtf8());
  }
}
