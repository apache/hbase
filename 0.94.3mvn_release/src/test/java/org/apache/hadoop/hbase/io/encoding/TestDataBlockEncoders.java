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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test all of the data block encoding algorithms for correctness.
 * Most of the class generate data which will test different branches in code.
 */
@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestDataBlockEncoders {
  static int NUMBER_OF_KV = 10000;
  static int NUM_RANDOM_SEEKS = 10000;

  private RedundantKVGenerator generator = new RedundantKVGenerator();
  private Random randomizer = new Random(42l);

  private final boolean includesMemstoreTS;

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  public TestDataBlockEncoders(boolean includesMemstoreTS) {
    this.includesMemstoreTS = includesMemstoreTS;
  }

  private void testAlgorithm(ByteBuffer dataset, DataBlockEncoder encoder)
      throws IOException {
    // encode
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(baos);
    encoder.compressKeyValues(dataOut, dataset, includesMemstoreTS);

    // decode
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    ByteBuffer actualDataset;
    actualDataset = encoder.uncompressKeyValues(dis, includesMemstoreTS);

    dataset.rewind();
    actualDataset.rewind();

    assertEquals("Encoding -> decoding gives different results for " + encoder,
        Bytes.toStringBinary(dataset), Bytes.toStringBinary(actualDataset));
  }

  /**
   * Test data block encoding of empty KeyValue.
   * @throws IOException On test failure.
   */
  @Test
  public void testEmptyKeyValues() throws IOException {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    kvList.add(new KeyValue(row, family, qualifier, 0l, Type.Put, value));
    kvList.add(new KeyValue(row, family, qualifier, 0l, Type.Put, value));
    testEncodersOnDataset(RedundantKVGenerator.convertKvToByteBuffer(kvList,
        includesMemstoreTS));
  }

  /**
   * Test KeyValues with negative timestamp.
   * @throws IOException On test failure.
   */
  @Test
  public void testNegativeTimestamps() throws IOException {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    kvList.add(new KeyValue(row, family, qualifier, -1l, Type.Put, value));
    kvList.add(new KeyValue(row, family, qualifier, -2l, Type.Put, value));
    testEncodersOnDataset(
        RedundantKVGenerator.convertKvToByteBuffer(kvList,
            includesMemstoreTS));
  }

  /**
   * Test whether compression -> decompression gives the consistent results on
   * pseudorandom sample.
   * @throws IOException On test failure.
   */
  @Test
  public void testExecutionOnSample() throws IOException {
    testEncodersOnDataset(
        RedundantKVGenerator.convertKvToByteBuffer(
            generator.generateTestKeyValues(NUMBER_OF_KV),
            includesMemstoreTS));
  }

  /**
   * Test seeking while file is encoded.
   */
  @Test
  public void testSeekingOnSample() throws IOException{
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV);
    ByteBuffer originalBuffer =
        RedundantKVGenerator.convertKvToByteBuffer(sampleKv,
            includesMemstoreTS);
    List<DataBlockEncoder> dataBlockEncoders =
        DataBlockEncoding.getAllEncoders();

    // create all seekers
    List<DataBlockEncoder.EncodedSeeker> encodedSeekers =
        new ArrayList<DataBlockEncoder.EncodedSeeker>();
    for (DataBlockEncoder encoder : dataBlockEncoders) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      encoder.compressKeyValues(dataOut, originalBuffer, includesMemstoreTS);
      ByteBuffer encodedBuffer = ByteBuffer.wrap(baos.toByteArray());
      DataBlockEncoder.EncodedSeeker seeker =
          encoder.createSeeker(KeyValue.KEY_COMPARATOR, includesMemstoreTS);
      seeker.setCurrentBuffer(encodedBuffer);
      encodedSeekers.add(seeker);
    }

    // test it!
    // try a few random seeks
    for (boolean seekBefore : new boolean[] {false, true}) {
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
    for (boolean seekBefore : new boolean[] {false, true}) {
      checkSeekingConsistency(encodedSeekers, seekBefore,
          sampleKv.get(sampleKv.size() - 1));
      KeyValue midKv = sampleKv.get(sampleKv.size() / 2);
      KeyValue lastMidKv = midKv.createLastOnRowCol();
      checkSeekingConsistency(encodedSeekers, seekBefore, lastMidKv);
    }
  }

  /**
   * Test iterating on encoded buffers.
   */
  @Test
  public void testNextOnSample() {
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV);
    ByteBuffer originalBuffer =
        RedundantKVGenerator.convertKvToByteBuffer(sampleKv,
            includesMemstoreTS);
    List<DataBlockEncoder> dataBlockEncoders =
        DataBlockEncoding.getAllEncoders();

    for (DataBlockEncoder encoder : dataBlockEncoders) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      try {
        encoder.compressKeyValues(dataOut, originalBuffer, includesMemstoreTS);
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Bug while encoding using '%s'", encoder.toString()), e);
      }

      ByteBuffer encodedBuffer = ByteBuffer.wrap(baos.toByteArray());
      DataBlockEncoder.EncodedSeeker seeker =
          encoder.createSeeker(KeyValue.KEY_COMPARATOR, includesMemstoreTS);
      seeker.setCurrentBuffer(encodedBuffer);
      int i = 0;
      do {
        KeyValue expectedKeyValue = sampleKv.get(i);
        ByteBuffer keyValue = seeker.getKeyValueBuffer();
        if (0 != Bytes.compareTo(
            keyValue.array(), keyValue.arrayOffset(), keyValue.limit(),
            expectedKeyValue.getBuffer(), expectedKeyValue.getOffset(),
            expectedKeyValue.getLength())) {

          int commonPrefix = 0;
          byte[] left = keyValue.array();
          byte[] right = expectedKeyValue.getBuffer();
          int leftOff = keyValue.arrayOffset();
          int rightOff = expectedKeyValue.getOffset();
          int length = Math.min(keyValue.limit(), expectedKeyValue.getLength());
          while (commonPrefix < length &&
              left[commonPrefix + leftOff] == right[commonPrefix + rightOff]) {
            commonPrefix++;
          }

          fail(String.format(
              "next() produces wrong results " +
              "encoder: %s i: %d commonPrefix: %d" +
              "\n expected %s\n actual      %s",
              encoder.toString(), i, commonPrefix,
              Bytes.toStringBinary(expectedKeyValue.getBuffer(),
                  expectedKeyValue.getOffset(), expectedKeyValue.getLength()),
              Bytes.toStringBinary(keyValue)));
        }
        i++;
      } while (seeker.next());
    }
  }

  /**
   * Test whether the decompression of first key is implemented correctly.
   */
  @Test
  public void testFirstKeyInBlockOnSample() {
    List<KeyValue> sampleKv = generator.generateTestKeyValues(NUMBER_OF_KV);
    ByteBuffer originalBuffer =
        RedundantKVGenerator.convertKvToByteBuffer(sampleKv,
            includesMemstoreTS);
    List<DataBlockEncoder> dataBlockEncoders =
        DataBlockEncoding.getAllEncoders();

    for (DataBlockEncoder encoder : dataBlockEncoders) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      try {
        encoder.compressKeyValues(dataOut, originalBuffer, includesMemstoreTS);
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Bug while encoding using '%s'", encoder.toString()), e);
      }

      ByteBuffer encodedBuffer = ByteBuffer.wrap(baos.toByteArray());
      ByteBuffer keyBuffer = encoder.getFirstKeyInBlock(encodedBuffer);
      KeyValue firstKv = sampleKv.get(0);
      if (0 != Bytes.compareTo(
          keyBuffer.array(), keyBuffer.arrayOffset(), keyBuffer.limit(),
          firstKv.getBuffer(), firstKv.getKeyOffset(),
          firstKv.getKeyLength())) {

        int commonPrefix = 0;
        int length = Math.min(keyBuffer.limit(), firstKv.getKeyLength());
        while (commonPrefix < length &&
            keyBuffer.array()[keyBuffer.arrayOffset() + commonPrefix] ==
            firstKv.getBuffer()[firstKv.getKeyOffset() + commonPrefix]) {
          commonPrefix++;
        }
        fail(String.format("Bug in '%s' commonPrefix %d",
            encoder.toString(), commonPrefix));
      }
    }
  }

  private void checkSeekingConsistency(
      List<DataBlockEncoder.EncodedSeeker> encodedSeekers, boolean seekBefore,
      KeyValue keyValue) {
    ByteBuffer expectedKeyValue = null;
    ByteBuffer expectedKey = null;
    ByteBuffer expectedValue = null;

    for (DataBlockEncoder.EncodedSeeker seeker : encodedSeekers) {
      seeker.seekToKeyInBlock(keyValue.getBuffer(),
          keyValue.getKeyOffset(), keyValue.getKeyLength(), seekBefore);
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

  private void testEncodersOnDataset(ByteBuffer onDataset)
      throws IOException{
    List<DataBlockEncoder> dataBlockEncoders =
        DataBlockEncoding.getAllEncoders();
    ByteBuffer dataset = ByteBuffer.allocate(onDataset.capacity());
    onDataset.rewind();
    dataset.put(onDataset);
    onDataset.rewind();
    dataset.flip();

    for (DataBlockEncoder encoder : dataBlockEncoders) {
      testAlgorithm(dataset, encoder);

      // ensure that dataset is unchanged
      dataset.rewind();
      assertEquals("Input of two methods is changed", onDataset, dataset);
    }
  }
}
