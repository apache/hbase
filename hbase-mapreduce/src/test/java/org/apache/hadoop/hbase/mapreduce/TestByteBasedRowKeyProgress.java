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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MapReduceTests.TAG)
@Tag(SmallTests.TAG)
public class TestByteBasedRowKeyProgress {
  private static RowKeyProgress create(byte[] start, byte[] stop) {
    RowKeyProgress p = new ByteBasedRowKeyProgress();
    p.setStartStopRows(start, stop);
    return p;
  }

  @Test
  public void testNullCurrentRow() {
    assertEquals(0.0f, create(Bytes.toBytes("a"), Bytes.toBytes("z")).getProgress(null));
  }

  @Test
  public void testNullStopRow() {
    assertEquals(0.0f, create(Bytes.toBytes("a"), null).getProgress(Bytes.toBytes("m")));
  }

  @Test
  public void testEmptyStopRow() {
    assertEquals(0.0f, create(Bytes.toBytes("a"), new byte[0]).getProgress(Bytes.toBytes("m")));
  }

  @Test
  public void testMidpoint() {
    assertEquals(0.5f, create(new byte[] { 0x00 }, new byte[] { (byte) 0xFF })
      .getProgress(new byte[] { (byte) 0x80 }), 0.01f);
  }

  @Test
  public void testAtStart() {
    assertEquals(0.0f,
      create(Bytes.toBytes("aaa"), Bytes.toBytes("zzz")).getProgress(Bytes.toBytes("aaa")), 0.001f);
  }

  @Test
  public void testNearMid() {
    assertEquals(0.48f,
      create(Bytes.toBytes("aaa"), Bytes.toBytes("zzz")).getProgress(Bytes.toBytes("mmm")), 0.01f);
  }

  @Test
  public void testNearEnd() {
    assertEquals(1.0f,
      create(Bytes.toBytes("aaa"), Bytes.toBytes("zzz")).getProgress(Bytes.toBytes("zzy")), 0.01f);
  }

  @Test
  public void testEmptyStartRow() {
    assertEquals(0.5f,
      create(new byte[0], new byte[] { (byte) 0xFF }).getProgress(new byte[] { (byte) 0x80 }),
      0.01f);
  }

  @Test
  public void testNullStartRow() {
    assertEquals(0.5f,
      create(null, new byte[] { (byte) 0xFF }).getProgress(new byte[] { (byte) 0x80 }), 0.01f);
  }

  @Test
  public void testProgressNeverExceedsOne() {
    assertEquals(1.0f,
      create(Bytes.toBytes("aaa"), Bytes.toBytes("mmm")).getProgress(Bytes.toBytes("zzz")));
  }

  @Test
  public void testProgressNeverBelowZero() {
    assertEquals(0.0f,
      create(Bytes.toBytes("mmm"), Bytes.toBytes("zzz")).getProgress(Bytes.toBytes("aaa")));
  }
}
