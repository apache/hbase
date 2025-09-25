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
import org.apache.hadoop.hbase.io.compress.CompressionTestBase;
import org.apache.hadoop.hbase.io.compress.DictionaryCache;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.util.RandomDistribution;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZstdDictionary extends CompressionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZstdDictionary.class);

  private static final String DICTIONARY_PATH = DictionaryCache.RESOURCE_SCHEME + "zstd.test.dict";
  // zstd.test.data compressed with zstd.test.dict at level 3 with a default buffer size of 262144
  // will produce a result of 359909 bytes
  private static final int EXPECTED_COMPRESSED_SIZE = 359909;

  private static byte[] TEST_DATA;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    TEST_DATA = DictionaryCache.loadFromResource(conf,
      DictionaryCache.RESOURCE_SCHEME + "zstd.test.data", /* maxSize */ 1024 * 1024);
    assertNotNull("Failed to load test data", TEST_DATA);
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, 3);
    conf.set(ZstdCodec.ZSTD_DICTIONARY_KEY, DICTIONARY_PATH);
    ZstdCodec codec = new ZstdCodec();
    codec.setConf(conf);
    codecTest(codec, new byte[][] { TEST_DATA }, EXPECTED_COMPRESSED_SIZE);
    // Assert that the dictionary was actually loaded
    assertTrue("Dictionary was not loaded by codec", DictionaryCache.contains(DICTIONARY_PATH));
  }

  //
  // For generating the test data in src/test/resources/
  //

  public static void main(String[] args) throws IOException {
    // Write 1000 1k blocks for training to the specified file
    // Train with:
    // zstd --train -B1024 -o <dictionary_file> <input_file>
    if (args.length < 1) {
      System.err.println("Usage: TestZstdCodec <outFile>");
      ExitHandler.getInstance().exit(-1);
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
