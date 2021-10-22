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
package org.apache.hadoop.hbase.io.compress.zstd;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.CompressionTestBase;
import org.apache.hadoop.hbase.io.compress.DictionaryCache;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.RandomDistribution;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZstdCodec extends CompressionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZstdCodec.class);

  @Test
  public void testZstdCodecSmall() throws Exception {
    codecSmallTest(new ZstdCodec());
  }

  @Test
  public void testZstdCodecLarge() throws Exception {
    codecLargeTest(new ZstdCodec(), 1.1); // poor compressability
    codecLargeTest(new ZstdCodec(),   2);
    codecLargeTest(new ZstdCodec(),  10); // very high compressability
  }

  @Test
  public void testZstdCodecVeryLarge() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // ZStandard levels range from 1 to 22.
    // Level 22 might take up to a minute to complete. 3 is the Hadoop default, and will be fast.
    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, 3);
    ZstdCodec codec = new ZstdCodec();
    codec.setConf(conf);
    codecVeryLargeTest(codec, 3); // like text
  }

  @Test
  public void testZstdCodecWithDictionary() throws Exception {
    final int maxSize = 1024 * 1024;
    Configuration conf = HBaseConfiguration.create();
    // Configure for dictionary available in test resources
    conf.setInt(DictionaryCache.DICTIONARY_MAX_SIZE_KEY, maxSize);
    final String dictionaryPath = DictionaryCache.RESOURCE_SCHEME + "zstd.test.dict";
    conf.set(ZstdCodec.ZSTD_DICTIONARY_KEY, dictionaryPath);
    // Load test data from test resources
    // This will throw an IOException if the test data cannot be loaded
    final byte[] testData = DictionaryCache.loadFromResource(conf,
      DictionaryCache.RESOURCE_SCHEME + "zstd.test.data", maxSize);
    assertNotNull("Failed to load test data", testData);
    // Run the test
    // This will throw an IOException of some kind if there is a problem loading or using the
    // dictionary.
    ZstdCodec codec = new ZstdCodec();
    codec.setConf(conf);
    codecTest(codec, new byte[][] { testData });
    // Assert that the dictionary was actually loaded
    assertTrue("Dictionary was not loaded by codec", DictionaryCache.contains(dictionaryPath));
  }

  // 
  // For generating the test data in src/test/resources/
  //

  public static void main(String args[]) throws IOException {
    // Write 1000 1k blocks for training to the specified file
    // Train with:
    //   zstd --train-fastcover=k=32,b=8 -B1024 -o <dictionary_file> <input_file>
    if (args.length < 1) {
      System.err.println("Usage: TestZstdCodec <outFile>");
      System.exit(-1);
    }
    final RandomDistribution.DiscreteRNG rng =
      new RandomDistribution.Zipf(new Random(), 0, Byte.MAX_VALUE, 2);
    final File outFile = new File(args[0]);
    final byte[] buffer = new byte[1024];
    System.out.println("Generating " + outFile);
    try (FileOutputStream os = new FileOutputStream(outFile)) {
      for (int i = 0; i < 1000; i++) {
        fill(rng, buffer);
        os.write(buffer);
      }
    }
    System.out.println("Done");
  }

}
