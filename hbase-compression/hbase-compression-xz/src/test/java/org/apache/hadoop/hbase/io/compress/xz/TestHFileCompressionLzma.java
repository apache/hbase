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
package org.apache.hadoop.hbase.io.compress.xz;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.HFileTestBase;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileCompressionLzma extends HFileTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileCompressionLzma.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set(Compression.LZMA_CODEC_CLASS_KEY, LzmaCodec.class.getCanonicalName());
    Compression.Algorithm.LZMA.reload(conf);
    HFileTestBase.setUpBeforeClass();
  }

  @Test
  public void test() throws Exception {
    Path path = new Path(TEST_UTIL.getDataTestDir(),
      HBaseTestingUtility.getRandomUUID().toString() + ".hfile");
    doTest(conf, path, Compression.Algorithm.LZMA);
  }

  @Test
  public void testReconfLevels() throws Exception {
    Path path_1 = new Path(TEST_UTIL.getDataTestDir(),
      HBaseTestingUtility.getRandomUUID().toString() + ".1.hfile");
    Path path_2 = new Path(TEST_UTIL.getDataTestDir(),
      HBaseTestingUtility.getRandomUUID().toString() + ".2.hfile");
    conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 1);
    doTest(conf, path_1, Compression.Algorithm.LZMA);
    long len_1 = FS.getFileStatus(path_1).getLen();
    conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 9);
    doTest(conf, path_2, Compression.Algorithm.LZMA);
    long len_2 = FS.getFileStatus(path_2).getLen();
    LOG.info("Level 1 len {}", len_1);
    LOG.info("Level 9 len {}", len_2);
    assertTrue("Reconfiguraton with LZMA_LEVEL_KEY did not seem to work", len_1 > len_2);
  }

}
