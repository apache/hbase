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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MapReduceTests.TAG)
@Tag(SmallTests.TAG)
public class TestHexPrefixRowKeyProgress {
  private static RowKeyProgress create(byte[] start, byte[] stop) {
    HexPrefixRowKeyProgress p = new HexPrefixRowKeyProgress();
    p.setStartStopRows(start, stop);
    return p;
  }

  private static RowKeyProgress createWithPrefixLength(byte[] start, byte[] stop,
    int prefixLength) {
    Configuration conf = new Configuration(false);
    conf.setInt(HexPrefixRowKeyProgress.PREFIX_LENGTH_KEY, prefixLength);
    HexPrefixRowKeyProgress p = new HexPrefixRowKeyProgress();
    p.setConf(conf);
    p.setStartStopRows(start, stop);
    return p;
  }

  @Test
  public void testNullCurrentRow() {
    assertEquals(0.0f, create(Bytes.toBytes("00"), Bytes.toBytes("ff")).getProgress(null));
  }

  @Test
  public void testMidpoint() {
    assertEquals(0.5f,
      create(Bytes.toBytes("0000"), Bytes.toBytes("ffff")).getProgress(Bytes.toBytes("8000")),
      0.01f);
  }

  @Test
  public void testQuarterPoint() {
    assertEquals(0.25f,
      create(Bytes.toBytes("0000"), Bytes.toBytes("ffff")).getProgress(Bytes.toBytes("4000")),
      0.01f);
  }

  @Test
  public void testAcross9ToAGap() {
    RowKeyProgress p = createWithPrefixLength(Bytes.toBytes("00"), Bytes.toBytes("ff"), 2);
    float at0f = p.getProgress(Bytes.toBytes("0f"));
    float at10 = p.getProgress(Bytes.toBytes("10"));
    assertEquals(1.0f / 255, at10 - at0f, 0.001f);
  }

  @Test
  public void testProgressNeverExceedsOne() {
    assertEquals(1.0f,
      create(Bytes.toBytes("0000"), Bytes.toBytes("8000")).getProgress(Bytes.toBytes("ffff")));
  }

  @Test
  public void testProgressNeverBelowZero() {
    assertEquals(0.0f,
      create(Bytes.toBytes("8000"), Bytes.toBytes("ffff")).getProgress(Bytes.toBytes("0000")));
  }

  @Test
  public void testNonHexSuffixIgnored() {
    RowKeyProgress p = createWithPrefixLength(Bytes.toBytes("00"), Bytes.toBytes("ff"), 2);
    float progressA = p.getProgress(Bytes.toBytes("80:dataA"));
    float progressB = p.getProgress(Bytes.toBytes("80:dataZ"));
    assertEquals(progressA, progressB);
  }

  @Test
  public void testMonotonicWithMixedSuffix() {
    RowKeyProgress p = createWithPrefixLength(Bytes.toBytes("00"), Bytes.toBytes("ff"), 2);
    float at0f = p.getProgress(Bytes.toBytes("0f:zzz"));
    float at10 = p.getProgress(Bytes.toBytes("10:aaa"));
    assertTrue(at10 > at0f);
  }
}
