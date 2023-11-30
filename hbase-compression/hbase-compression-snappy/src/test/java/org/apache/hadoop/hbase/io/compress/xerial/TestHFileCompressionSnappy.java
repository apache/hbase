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
package org.apache.hadoop.hbase.io.compress.xerial;

import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.HFileTestBase;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileCompressionSnappy extends HFileTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileCompressionSnappy.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    assumeTrue(SnappyCodec.isLoaded());
    conf = TEST_UTIL.getConfiguration();
    conf.set(Compression.SNAPPY_CODEC_CLASS_KEY, SnappyCodec.class.getCanonicalName());
    Compression.Algorithm.SNAPPY.reload(conf);
    HFileTestBase.setUpBeforeClass();
  }

  @Test
  public void test() throws Exception {
    Path path =
      new Path(TEST_UTIL.getDataTestDir(), HBaseTestingUtil.getRandomUUID().toString() + ".hfile");
    doTest(conf, path, Compression.Algorithm.SNAPPY);
  }

}
