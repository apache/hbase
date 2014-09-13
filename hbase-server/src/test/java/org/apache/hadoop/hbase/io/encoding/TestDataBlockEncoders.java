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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.Writer.BufferGrabbingByteArrayOutputStream;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.test.RedundantKVGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test all of the data block encoding algorithms for correctness. Most of the
 * class generate data which will test different branches in code.
 */
@Category({IOTests.class, LargeTests.class})
@RunWith(Parameterized.class)
public class TestDataBlockEncoders {

  private static int NUMBER_OF_KV = 10000;
  private static int NUM_RANDOM_SEEKS = 10000;

  private static int ENCODED_DATA_OFFSET = HConstants.HFILEBLOCK_HEADER_SIZE
      + DataBlockEncoding.ID_SIZE;

  private RedundantKVGenerator generator = new RedundantKVGenerator();
  private Random randomizer = new Random(42l);

  private final boolean includesMemstoreTS;
  private final boolean includesTags;

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.MEMSTORETS_TAGS_PARAMETRIZED;
  }
  public TestDataBlockEncoders(boolean includesMemstoreTS, boolean includesTag) {
    this.includesMemstoreTS = includesMemstoreTS;
    this.includesTags = includesTag;
  }
  
  private HFileBlockEncodingContext getEncodingContext(Compression.Algorithm algo,
      DataBlockEncoding encoding) {
    DataBlockEncoder encoder = encoding.getEncoder();
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(false)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(includesTags)
                        .withCompression(algo).build();
    if (encoder != null) {
      return encoder.newDataBlockEncodingContext(encoding,
          HConstants.HFILEBLOCK_DUMMY_HEADER, meta);
    } else {
      return new HFileBlockDefaultEncodingContext(encoding,
          HConstants.HFILEBLOCK_DUMMY_HEADER, meta);
    }
  }

  /**
   * Test data block encoding of empty KeyValue.
   * 
   * @throws IOException
   *           On test failure.
   */
  @Test
  public void testEmptyKeyValues() throws IOException {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    if (!includesTags) {
      kvList.add(new KeyValue(row, family, qualifier, 0l, value));
      kvList.add(new KeyValue(row, family, qualifier, 0l, value));
    } else {
      byte[] metaValue1 = Bytes.toBytes("metaValue1");
      byte[] metaValue2 = Bytes.toBytes("metaValue2");
      kvList.add(new KeyValue(row, family, qualifier, 0l, value, new Tag[] { new Tag((byte) 1,
          metaValue1) }));
      kvList.add(new KeyValue(row, family, qualifier, 0l, value, new Tag[] { new Tag((byte) 1,
          metaValue2) }));
    }
    testEncodersOnDataset(kvList, includesMemstoreTS, includesTags);
  }

  /**
   * Test KeyValues with negative timestamp.
   * 
   * @throws IOException
   *           On test failure.
   */
  @Test
  public void testNegativeTimestamps() throws IOException {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    if (includesTags) {
      byte[] metaValue1 = Bytes.toBytes("metaValue1");
      byte[] metaValue2 = Bytes.toBytes("metaValue2");
      kvList.add(new KeyValue(row, family, qualifier, 0l, value, new Tag[] { new Tag((byte) 1,
          metaValue1) }));
      kvList.add(new KeyValue(row, family, qualifier, 0l, value, new Tag[] { new Tag((byte) 1,
          metaValue2) }));
    } else {
      kvList.add(new KeyValue(row, family, qualifier, -1l, Type.Put, value));
      kvList.add(new KeyValue(row, family, qualifier, -2l, Type.Put, value));
    }
    testEncodersOnDataset(kvList, includesMemstoreTS, includesTags);
  }


  /**
   * Test whether compression -> decompression gives the consistent results on
   * pseudorandom sample.
   * @throws IOException On test failure.
   */
  @Test
  public void testExecutionOnSample() throws IOException {
    List<KeyValue> kvList = generator.generateTestKeyValues(NUMBER_OF_KV, includesTags);
    testEncodersOnDataset(kvList, includesMemstoreTS, includesTags);
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekingOnSample() throws IOException {
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV, includesTags);

    // create all seekers
    List<DataBlockEncoder.EncodedSeeker> encodedSeekers = 
        new ArrayList<DataBlockEncoder.EncodedSeeker>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      DataBlockEncoder encoder = encoding.getEncoder();
      if (encoder == null) {
        continue;
      }
      ByteBuffer encodedBuffer = encodeKeyValues(encoding, sampleKv,
          getEncodingContext(Compression.Algorithm.NONE, encoding));
      HFileContext meta = new HFileContextBuilder()
                          .withHBaseCheckSum(false)
                          .withIncludesMvcc(includesMemstoreTS)
                          .withIncludesTags(includesTags)
                          .withCompression(Compression.Algorithm.NONE)
                          .build();
      DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
          encoder.newDataBlockDecodingContext(meta));
      seeker.setCurrentBuffer(encodedBuffer);
      encodedSeekers.add(seeker);
    }
    // test it!
    // try a few random seeks
    for (boolean seekBefore : new boolean[] { false, true }) {
      for (int i = 0; i < NUM_RANDOM_SEEKS; ++i) {
        int keyValueId;
        if (!seekBefore) {
          keyValueId = randomizer.nextInt(sampleKv.size());
        } else {
          keyValueId = randomizer.nextInt(sampleKv.size() - 1) + 1;
        }

        KeyValue keyValue = sampleKv.get(keyValueId);
        checkSeekingConsistency(encodedSeekers, seekBefore, keyValue);
      }
    }

    // check edge cases
    checkSeekingConsistency(encodedSeekers, false, sampleKv.get(0));
    for (boolean seekBefore : new boolean[] { false, true }) {
      checkSeekingConsistency(encodedSeekers, seekBefore, sampleKv.get(sampleKv.size() - 1));
      KeyValue midKv = sampleKv.get(sampleKv.size() / 2);
      KeyValue lastMidKv =KeyValueUtil.createLastOnRowCol(midKv);
      checkSeekingConsistency(encodedSeekers, seekBefore, lastMidKv);
    }
  }

  static ByteBuffer encodeKeyValues(DataBlockEncoding encoding, List<KeyValue> kvs,
      HFileBlockEncodingContext encodingContext) throws IOException {
    DataBlockEncoder encoder = encoding.getEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(HConstants.HFILEBLOCK_DUMMY_HEADER);
    DataOutputStream dos = new DataOutputStream(baos);
    encoder.startBlockEncoding(encodingContext, dos);
    for (KeyValue kv : kvs) {
      encoder.encode(kv, encodingContext, dos);
    }
    BufferGrabbingByteArrayOutputStream stream = new BufferGrabbingByteArrayOutputStream();
    baos.writeTo(stream);
    encoder.endBlockEncoding(encodingContext, dos, stream.getBuffer());
    byte[] encodedData = new byte[baos.size() - ENCODED_DATA_OFFSET];
    System.arraycopy(baos.toByteArray(), ENCODED_DATA_OFFSET, encodedData, 0, encodedData.length);
    return ByteBuffer.wrap(encodedData);
  }

  @Test
  public void testNextOnSample() throws IOException {
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV, includesTags);

    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      if (encoding.getEncoder() == null) {
        continue;
      }
      DataBlockEncoder encoder = encoding.getEncoder();
      ByteBuffer encodedBuffer = encodeKeyValues(encoding, sampleKv,
          getEncodingContext(Compression.Algorithm.NONE, encoding));
      HFileContext meta = new HFileContextBuilder()
                          .withHBaseCheckSum(false)
                          .withIncludesMvcc(includesMemstoreTS)
                          .withIncludesTags(includesTags)
                          .withCompression(Compression.Algorithm.NONE)
                          .build();
      DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
          encoder.newDataBlockDecodingContext(meta));
      seeker.setCurrentBuffer(encodedBuffer);
      int i = 0;
      do {
        KeyValue expectedKeyValue = sampleKv.get(i);
        ByteBuffer keyValue = seeker.getKeyValueBuffer();
        if (0 != Bytes.compareTo(keyValue.array(), keyValue.arrayOffset(), keyValue.limit(),
            expectedKeyValue.getBuffer(), expectedKeyValue.getOffset(),
            expectedKeyValue.getLength())) {

          int commonPrefix = 0;
          byte[] left = keyValue.array();
          byte[] right = expectedKeyValue.getBuffer();
          int leftOff = keyValue.arrayOffset();
          int rightOff = expectedKeyValue.getOffset();
          int length = Math.min(keyValue.limit(), expectedKeyValue.getLength());
          while (commonPrefix < length
              && left[commonPrefix + leftOff] == right[commonPrefix + rightOff]) {
            commonPrefix++;
          }

          fail(String.format("next() produces wrong results "
              + "encoder: %s i: %d commonPrefix: %d" + "\n expected %s\n actual      %s", encoder
              .toString(), i, commonPrefix, Bytes.toStringBinary(expectedKeyValue.getBuffer(),
              expectedKeyValue.getOffset(), expectedKeyValue.getLength()), Bytes
              .toStringBinary(keyValue)));
        }
        i++;
      } while (seeker.next());
    }
  }

  /**
   * Test whether the decompression of first key is implemented correctly.
   * @throws IOException
   */
  @Test
  public void testFirstKeyInBlockOnSample() throws IOException {
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV, includesTags);

    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      if (encoding.getEncoder() == null) {
        continue;
      }
      DataBlockEncoder encoder = encoding.getEncoder();
      ByteBuffer encodedBuffer = encodeKeyValues(encoding, sampleKv,
          getEncodingContext(Compression.Algorithm.NONE, encoding));
      ByteBuffer keyBuffer = encoder.getFirstKeyInBlock(encodedBuffer);
      KeyValue firstKv = sampleKv.get(0);
      if (0 != Bytes.compareTo(keyBuffer.array(), keyBuffer.arrayOffset(), keyBuffer.limit(),
          firstKv.getBuffer(), firstKv.getKeyOffset(), firstKv.getKeyLength())) {

        int commonPrefix = 0;
        int length = Math.min(keyBuffer.limit(), firstKv.getKeyLength());
        while (commonPrefix < length
            && keyBuffer.array()[keyBuffer.arrayOffset() + commonPrefix] == firstKv.getBuffer()[firstKv
                .getKeyOffset() + commonPrefix]) {
          commonPrefix++;
        }
        fail(String.format("Bug in '%s' commonPrefix %d", encoder.toString(), commonPrefix));
      }
    }
  }
  
  private void checkSeekingConsistency(List<DataBlockEncoder.EncodedSeeker> encodedSeekers,
      boolean seekBefore, KeyValue keyValue) {
    ByteBuffer expectedKeyValue = null;
    ByteBuffer expectedKey = null;
    ByteBuffer expectedValue = null;
    for (DataBlockEncoder.EncodedSeeker seeker : encodedSeekers) {
      seeker.seekToKeyInBlock(keyValue, seekBefore);
      seeker.rewind();

      ByteBuffer actualKeyValue = seeker.getKeyValueBuffer();
      ByteBuffer actualKey = seeker.getKeyDeepCopy();
      ByteBuffer actualValue = seeker.getValueShallowCopy();

      if (expectedKeyValue != null) {
        assertEquals(expectedKeyValue, actualKeyValue);
      } else {
        expectedKeyValue = actualKeyValue;
      }

      if (expectedKey != null) {
        assertEquals(expectedKey, actualKey);
      } else {
        expectedKey = actualKey;
      }

      if (expectedValue != null) {
        assertEquals(expectedValue, actualValue);
      } else {
        expectedValue = actualValue;
      }
    }
  }

  private void testEncodersOnDataset(List<KeyValue> kvList, boolean includesMemstoreTS,
      boolean includesTags) throws IOException {
    ByteBuffer unencodedDataBuf = RedundantKVGenerator.convertKvToByteBuffer(kvList,
        includesMemstoreTS);
    HFileContext fileContext = new HFileContextBuilder().withIncludesMvcc(includesMemstoreTS)
        .withIncludesTags(includesTags).build();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      DataBlockEncoder encoder = encoding.getEncoder();
      if (encoder == null) {
        continue;
      }
      HFileBlockEncodingContext encodingContext = new HFileBlockDefaultEncodingContext(encoding,
          HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(HConstants.HFILEBLOCK_DUMMY_HEADER);
      DataOutputStream dos = new DataOutputStream(baos);
      encoder.startBlockEncoding(encodingContext, dos);
      for (KeyValue kv : kvList) {
        encoder.encode(kv, encodingContext, dos);
      }
      BufferGrabbingByteArrayOutputStream stream = new BufferGrabbingByteArrayOutputStream();
      baos.writeTo(stream);
      encoder.endBlockEncoding(encodingContext, dos, stream.getBuffer());
      byte[] encodedData = baos.toByteArray();

      testAlgorithm(encodedData, unencodedDataBuf, encoder);
    }
  }
  
  @Test
  public void testZeroByte() throws IOException {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    byte[] row = Bytes.toBytes("abcd");
    byte[] family = new byte[] { 'f' };
    byte[] qualifier0 = new byte[] { 'b' };
    byte[] qualifier1 = new byte[] { 'c' };
    byte[] value0 = new byte[] { 'd' };
    byte[] value1 = new byte[] { 0x00 };
    if (includesTags) {
      kvList.add(new KeyValue(row, family, qualifier0, 0, value0, new Tag[] { new Tag((byte) 1,
          "value1") }));
      kvList.add(new KeyValue(row, family, qualifier1, 0, value1, new Tag[] { new Tag((byte) 1,
          "value1") }));
    } else {
      kvList.add(new KeyValue(row, family, qualifier0, 0, Type.Put, value0));
      kvList.add(new KeyValue(row, family, qualifier1, 0, Type.Put, value1));
    }
    testEncodersOnDataset(kvList, includesMemstoreTS, includesTags);
  }

  private void testAlgorithm(byte[] encodedData, ByteBuffer unencodedDataBuf,
      DataBlockEncoder encoder) throws IOException {
    // decode
    ByteArrayInputStream bais = new ByteArrayInputStream(encodedData, ENCODED_DATA_OFFSET,
        encodedData.length - ENCODED_DATA_OFFSET);
    DataInputStream dis = new DataInputStream(bais);
    ByteBuffer actualDataset;
    HFileContext meta = new HFileContextBuilder().withHBaseCheckSum(false)
        .withIncludesMvcc(includesMemstoreTS).withIncludesTags(includesTags)
        .withCompression(Compression.Algorithm.NONE).build();
    actualDataset = encoder.decodeKeyValues(dis, encoder.newDataBlockDecodingContext(meta));
    actualDataset.rewind();

    // this is because in case of prefix tree the decoded stream will not have
    // the
    // mvcc in it.
    assertEquals("Encoding -> decoding gives different results for " + encoder,
        Bytes.toStringBinary(unencodedDataBuf), Bytes.toStringBinary(actualDataset));
  }
}
