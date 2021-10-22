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
package org.apache.hadoop.hbase.io.compress;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RandomDistribution;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:innerassignment")
public class CompressionTestBase {

  protected static final Logger LOG = LoggerFactory.getLogger(CompressionTestBase.class);

  static final int LARGE_SIZE = 10 * 1024 * 1024;
  static final int VERY_LARGE_SIZE = 100 * 1024 * 1024;
  static final int BLOCK_SIZE = 4096;

  static final byte[] SMALL_INPUT;
  static {
    // 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597
    SMALL_INPUT = new byte[1+1+2+3+5+8+13+21+34+55+89+144+233+377+610+987+1597];
    int off = 0;
    Arrays.fill(SMALL_INPUT, off, (off+=1), (byte)'A');
    Arrays.fill(SMALL_INPUT, off, (off+=1), (byte)'B');
    Arrays.fill(SMALL_INPUT, off, (off+=2), (byte)'C');
    Arrays.fill(SMALL_INPUT, off, (off+=3), (byte)'D');
    Arrays.fill(SMALL_INPUT, off, (off+=5), (byte)'E');
    Arrays.fill(SMALL_INPUT, off, (off+=8), (byte)'F');
    Arrays.fill(SMALL_INPUT, off, (off+=13), (byte)'G');
    Arrays.fill(SMALL_INPUT, off, (off+=21), (byte)'H');
    Arrays.fill(SMALL_INPUT, off, (off+=34), (byte)'I');
    Arrays.fill(SMALL_INPUT, off, (off+=55), (byte)'J');
    Arrays.fill(SMALL_INPUT, off, (off+=89), (byte)'K');
    Arrays.fill(SMALL_INPUT, off, (off+=144), (byte)'L');
    Arrays.fill(SMALL_INPUT, off, (off+=233), (byte)'M');
    Arrays.fill(SMALL_INPUT, off, (off+=377), (byte)'N');
    Arrays.fill(SMALL_INPUT, off, (off+=610), (byte)'O');
    Arrays.fill(SMALL_INPUT, off, (off+=987), (byte)'P');
    Arrays.fill(SMALL_INPUT, off, (off+=1597), (byte)'Q');
  }

  protected void codecTest(final CompressionCodec codec, final byte[][] input)
      throws Exception {
    // We do this in Compression.java
    ((Configurable)codec).getConf().setInt("io.file.buffer.size", 32 * 1024);
    // Compress
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionOutputStream out = codec.createOutputStream(baos);
    int inLen = 0;
    long start = EnvironmentEdgeManager.currentTime();
    for (int i = 0; i < input.length; i++) {
      out.write(input[i]);
      inLen += input[i].length;
    }
    out.close();
    long end = EnvironmentEdgeManager.currentTime();
    final byte[] compressed = baos.toByteArray();
    LOG.info("{} compressed {} bytes to {} bytes in {} ms", codec.getClass().getSimpleName(),
      inLen, compressed.length, end - start);
    // Decompress
    final byte[] plain = new byte[inLen];
    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed));
    start = EnvironmentEdgeManager.currentTime();
    IOUtils.readFully(in, plain, 0, plain.length);
    in.close();
    end = EnvironmentEdgeManager.currentTime();
    LOG.info("{} decompressed {} bytes to {} bytes in {} ms", codec.getClass().getSimpleName(),
      compressed.length, plain.length, end - start);
    // Decompressed bytes should equal the original
    int offset = 0;
    for (int i = 0; i < input.length; i++) {
      assertTrue("Comparison failed at offset " + offset,
        Bytes.compareTo(plain, offset, input[i].length, input[i], 0, input[i].length) == 0);
      offset += input[i].length;
    }
  }

  /**
   * Test with one smallish input buffer
   */
  protected void codecSmallTest(final CompressionCodec codec) throws Exception {
    codecTest(codec, new byte[][] { SMALL_INPUT });
  }

  /**
   * Test with a large input (1MB) divided into blocks of 4KB.
   */
  protected void codecLargeTest(final CompressionCodec codec, final double sigma) throws Exception {
    RandomDistribution.DiscreteRNG zipf =
      new RandomDistribution.Zipf(new Random(), 0, Byte.MAX_VALUE, sigma);
    final byte[][] input = new byte[LARGE_SIZE/BLOCK_SIZE][BLOCK_SIZE];
    for (int i = 0; i < input.length; i++) {
      for (int j = 0; j < input[i].length; j++) {
        input[i][j] = (byte)zipf.nextInt();
      }
    }
    codecTest(codec, input);
  }

  /**
   * Test with a very large input (100MB) as a single input buffer.
   */
  protected void codecVeryLargeTest(final CompressionCodec codec, final double sigma) throws Exception {
    RandomDistribution.DiscreteRNG zipf =
        new RandomDistribution.Zipf(new Random(), 0, Byte.MAX_VALUE, sigma);
    final byte[][] input = new byte[1][VERY_LARGE_SIZE];
    for (int i = 0; i < VERY_LARGE_SIZE; i++) {
      input[0][i] = (byte)zipf.nextInt();
    }
    codecTest(codec, input);
  }

}
