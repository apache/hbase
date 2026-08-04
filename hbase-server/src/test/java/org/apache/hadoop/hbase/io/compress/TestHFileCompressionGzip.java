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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileCompressionGzip extends HFileTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileCompressionGzip.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HFileTestBase.setUpBeforeClass();
  }

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    HFileTestBase.setUpBeforeClass();
  }

  @Test
  public void testWithStreamDecompression() throws Exception {
    conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", false);
    Compression.Algorithm.GZ.reload(conf);

    Path path =
      new Path(TEST_UTIL.getDataTestDir(), HBaseTestingUtil.getRandomUUID().toString() + ".hfile");
    doTest(conf, path, Compression.Algorithm.GZ);
  }

  @Test
  public void testWithByteBuffDecompression() throws Exception {
    conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", true);
    Compression.Algorithm.GZ.reload(conf);

    Path path =
      new Path(TEST_UTIL.getDataTestDir(), HBaseTestingUtil.getRandomUUID().toString() + ".hfile");
    doTest(conf, path, Compression.Algorithm.GZ);
  }

}
