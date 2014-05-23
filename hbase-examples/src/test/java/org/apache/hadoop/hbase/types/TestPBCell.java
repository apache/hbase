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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Test;

public class TestPBCell {

  private static final PBCell CODEC = new PBCell();

  /**
   * Basic test to verify utility methods in {@link PBType} and delegation to protobuf works.
   */
  @Test
  public void testRoundTrip() {
    final Cell cell = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("fam"),
      Bytes.toBytes("qual"), Bytes.toBytes("val"));
    CellProtos.Cell c = ProtobufUtil.toCell(cell), decoded;
    PositionedByteRange pbr = new SimplePositionedByteRange(c.getSerializedSize());
    pbr.setPosition(0);
    int encodedLength = CODEC.encode(pbr, c);
    pbr.setPosition(0);
    decoded = CODEC.decode(pbr);
    assertEquals(encodedLength, pbr.getPosition());
    assertTrue(CellComparator.equals(cell, ProtobufUtil.toCell(decoded)));
  }
}
