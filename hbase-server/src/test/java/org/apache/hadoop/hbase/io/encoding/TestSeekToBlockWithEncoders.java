/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({IOTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestSeekToBlockWithEncoders {
  static final byte[] HFILEBLOCK_DUMMY_HEADER = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
  private final boolean useOffheapData;

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  public TestSeekToBlockWithEncoders(boolean useOffheapData) {
    this.useOffheapData = useOffheapData;
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekToBlockWithNonMatchingSeekKey() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aab"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv3 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv3);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aad"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("bba"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aae"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    seekToTheKey(kv4, sampleKv, toSeek);
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekingToBlockWithBiggerNonLength1() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aab"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv3 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv3);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aad"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aaaad"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    seekToTheKey(kv1, sampleKv, toSeek);
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekingToBlockToANotAvailableKey() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aab"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv3 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv3);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aaae"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("bbbcd"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("bbbce"), Bytes.toBytes("f1"),
        Bytes.toBytes("q1"), Bytes.toBytes("val"));
    seekToTheKey(kv5, sampleKv, toSeek);
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekToBlockWithDecreasingCommonPrefix() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row10aaa"), Bytes.toBytes("f1"),
        Bytes.toBytes("q1"), Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("row10aaa"), Bytes.toBytes("f1"),
        Bytes.toBytes("q2"), Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv3 = new KeyValue(Bytes.toBytes("row10aaa"), Bytes.toBytes("f1"),
        Bytes.toBytes("q3"), Bytes.toBytes("val"));
    sampleKv.add(kv3);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("row11baa"), Bytes.toBytes("f1"),
        Bytes.toBytes("q1"), Bytes.toBytes("val"));
    sampleKv.add(kv4);
    Cell toSeek = CellUtil.createLastOnRow(kv3);
    seekToTheKey(kv3, sampleKv, toSeek);
  }

  @Test
  public void testSeekToBlockWithDiffQualifer() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aab"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    seekToTheKey(kv5, sampleKv, toSeek);
  }

  @Test
  public void testSeekToBlockWithDiffQualiferOnSameRow() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q3"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q4"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue kv6 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q5"),
        Bytes.toBytes("val"));
    sampleKv.add(kv6);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q5"),
        Bytes.toBytes("val"));
    seekToTheKey(kv6, sampleKv, toSeek);
  }

  @Test
  public void testSeekToBlockWithDiffQualiferOnSameRow1() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q3"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q4"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue kv6 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("z5"),
        Bytes.toBytes("val"));
    sampleKv.add(kv6);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q5"),
        Bytes.toBytes("val"));
    seekToTheKey(kv5, sampleKv, toSeek);
  }

  @Test
  public void testSeekToBlockWithDiffQualiferOnSameRowButDescendingInSize() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qual1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qual2"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qual3"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qual4"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue kv6 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qz"),
        Bytes.toBytes("val"));
    sampleKv.add(kv6);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qz"),
        Bytes.toBytes("val"));
    seekToTheKey(kv6, sampleKv, toSeek);
  }

  @Test
  public void testSeekToBlockWithDiffFamilyAndQualifer() throws IOException {
    List<KeyValue> sampleKv = new ArrayList<KeyValue>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("fam1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv1);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("aab"), Bytes.toBytes("fam1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv2);
    KeyValue kv4 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("fam1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    sampleKv.add(kv4);
    KeyValue kv5 = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("fam1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    sampleKv.add(kv5);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("fam2"),
        Bytes.toBytes("q2"), Bytes.toBytes("val"));
    seekToTheKey(kv5, sampleKv, toSeek);
  }

  private void seekToTheKey(KeyValue expected, List<KeyValue> kvs, Cell toSeek)
      throws IOException {
    // create all seekers
    List<DataBlockEncoder.EncodedSeeker> encodedSeekers = new ArrayList<DataBlockEncoder.EncodedSeeker>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      if (encoding.getEncoder() == null || encoding == DataBlockEncoding.PREFIX_TREE) {
        continue;
      }

      DataBlockEncoder encoder = encoding.getEncoder();
      HFileContext meta = new HFileContextBuilder().withHBaseCheckSum(false)
          .withIncludesMvcc(false).withIncludesTags(false)
          .withCompression(Compression.Algorithm.NONE).build();
      HFileBlockEncodingContext encodingContext = encoder.newDataBlockEncodingContext(encoding,
          HFILEBLOCK_DUMMY_HEADER, meta);
      ByteBuffer encodedBuffer = TestDataBlockEncoders.encodeKeyValues(encoding, kvs,
          encodingContext, this.useOffheapData);
      DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(CellComparator.COMPARATOR,
          encoder.newDataBlockDecodingContext(meta));
      seeker.setCurrentBuffer(new SingleByteBuff(encodedBuffer));
      encodedSeekers.add(seeker);
    }
    // test it!
    // try a few random seeks
    checkSeekingConsistency(encodedSeekers, toSeek, expected);
  }

  private void checkSeekingConsistency(List<DataBlockEncoder.EncodedSeeker> encodedSeekers,
      Cell keyValue, KeyValue expected) {
    for (DataBlockEncoder.EncodedSeeker seeker : encodedSeekers) {
      seeker.seekToKeyInBlock(keyValue, false);
      Cell keyValue2 = seeker.getCell();
      assertEquals(expected, keyValue2);
      seeker.rewind();
    }
  }
}
