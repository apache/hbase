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
package org.apache.hadoop.hbase.io.compress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Round trip through {@link Compression.Algorithm#SNAPPY} using its default codec, Hadoop's
 * {@link SnappyCodec}. Hadoop's SnappyCompressor sets the output buffer limit to 0 before calling
 * snappy-java, which some snappy-java releases (1.1.10.9, 1.1.10.10) reject. See HBASE-30538.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestHadoopSnappyCodec {

  @Test
  public void testRoundTrip() throws Exception {
    // Hadoop's SnappyCodec only uses snappy-java since 3.3.1 (HADOOP-17125). Earlier versions
    // need the native hadoop library.
    assumeTrue(
      VersionInfo.compareVersion(org.apache.hadoop.util.VersionInfo.getVersion(), "3.3.1") >= 0,
      "Hadoop SnappyCodec does not use snappy-java before 3.3.1");
    Compression.Algorithm algo = Compression.Algorithm.SNAPPY;
    byte[] data = new byte[256 * 1024];
    Random rand = new Random(0x30538);
    for (int i = 0; i < data.length; i++) {
      // Compressible but not trivial
      data[i] = (byte) ('a' + rand.nextInt(8));
    }

    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    Compressor compressor = algo.getCompressor();
    assertEquals(SnappyCompressor.class, compressor.getClass());
    try (OutputStream os = algo.createCompressionStream(compressed, compressor, 0)) {
      os.write(data);
    } finally {
      algo.returnCompressor(compressor);
    }

    byte[] result = new byte[data.length];
    Decompressor decompressor = algo.getDecompressor();
    try (InputStream is = algo.createDecompressionStream(
      new ByteArrayInputStream(compressed.toByteArray()), decompressor, 0)) {
      IOUtils.readFully(is, result, 0, result.length);
    } finally {
      algo.returnDecompressor(decompressor);
    }
    assertArrayEquals(data, result);
  }
}
