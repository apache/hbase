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
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.test.RedundantKVGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSeekToBlockWithEncoders {

  private static int ENCODED_DATA_OFFSET = HConstants.HFILEBLOCK_HEADER_SIZE
      + DataBlockEncoding.ID_SIZE;

  private HFileBlockEncodingContext getEncodingContext(Compression.Algorithm algo,
      DataBlockEncoding encoding) {
    DataBlockEncoder encoder = encoding.getEncoder();
    HFileContext meta = new HFileContextBuilder().withHBaseCheckSum(false).withIncludesMvcc(false)
        .withIncludesTags(false).withCompression(algo).build();
    if (encoder != null) {
      return encoder
          .newDataBlockEncodingContext(encoding, HConstants.HFILEBLOCK_DUMMY_HEADER, meta);
    } else {
      return new HFileBlockDefaultEncodingContext(encoding, HConstants.HFILEBLOCK_DUMMY_HEADER,
          meta);
    }
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aae"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    seekToTheKey(kv4, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"),
        Bytes.toBytes("val"));
    seekToTheKey(kv1, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("bbbce"), Bytes.toBytes("f1"),
        Bytes.toBytes("q1"), Bytes.toBytes("val"));
    seekToTheKey(kv5, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = KeyValue.createLastOnRow(kv3.getRowArray(), kv3.getRowOffset(),
        kv3.getRowLength(), null, 0, 0, null, 0, 0);
    seekToTheKey(kv3, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("f1"), Bytes.toBytes("q2"),
        Bytes.toBytes("val"));
    seekToTheKey(kv5, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q5"),
        Bytes.toBytes("val"));
    seekToTheKey(kv6, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q5"),
        Bytes.toBytes("val"));
    seekToTheKey(kv5, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("qz"),
        Bytes.toBytes("val"));
    seekToTheKey(kv6, originalBuffer, toSeek);
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
    ByteBuffer originalBuffer = RedundantKVGenerator.convertKvToByteBuffer(sampleKv, false);
    KeyValue toSeek = new KeyValue(Bytes.toBytes("aac"), Bytes.toBytes("fam2"),
        Bytes.toBytes("q2"), Bytes.toBytes("val"));
    seekToTheKey(kv5, originalBuffer, toSeek);
  }

  private void seekToTheKey(KeyValue expected, ByteBuffer originalBuffer, KeyValue toSeek)
      throws IOException {
    // create all seekers
    List<DataBlockEncoder.EncodedSeeker> encodedSeekers = 
        new ArrayList<DataBlockEncoder.EncodedSeeker>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      if (encoding.getEncoder() == null || encoding == DataBlockEncoding.PREFIX_TREE) {
        continue;
      }

      ByteBuffer encodedBuffer = ByteBuffer.wrap(encodeBytes(encoding, originalBuffer));
      DataBlockEncoder encoder = encoding.getEncoder();
      HFileContext meta = new HFileContextBuilder().withHBaseCheckSum(false)
          .withIncludesMvcc(false).withIncludesTags(false)
          .withCompression(Compression.Algorithm.NONE).build();
      DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
          encoder.newDataBlockDecodingContext(meta));
      seeker.setCurrentBuffer(encodedBuffer);
      encodedSeekers.add(seeker);
    }
    // test it!
    // try a few random seeks
    checkSeekingConsistency(encodedSeekers, toSeek, expected);
  }

  private void checkSeekingConsistency(List<DataBlockEncoder.EncodedSeeker> encodedSeekers,
      KeyValue keyValue, KeyValue expected) {
    for (DataBlockEncoder.EncodedSeeker seeker : encodedSeekers) {
      seeker.seekToKeyInBlock(
          new KeyValue.KeyOnlyKeyValue(keyValue.getBuffer(), keyValue.getKeyOffset(), keyValue
              .getKeyLength()), false);
      KeyValue keyValue2 = seeker.getKeyValue();
      assertEquals(expected, keyValue2);
      seeker.rewind();
    }
  }

  private byte[] encodeBytes(DataBlockEncoding encoding, ByteBuffer dataset) throws IOException {
    DataBlockEncoder encoder = encoding.getEncoder();
    HFileBlockEncodingContext encodingCtx = getEncodingContext(Compression.Algorithm.NONE, encoding);

    encoder.encodeKeyValues(dataset, encodingCtx);

    byte[] encodedBytesWithHeader = encodingCtx.getUncompressedBytesWithHeader();
    byte[] encodedData = new byte[encodedBytesWithHeader.length - ENCODED_DATA_OFFSET];
    System.arraycopy(encodedBytesWithHeader, ENCODED_DATA_OFFSET, encodedData, 0,
        encodedData.length);
    return encodedData;
  }
}
