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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.CompressionTestBase;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLzmaCodec extends CompressionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLzmaCodec.class);

  @Test
  public void testLzmaCodecSmall() throws Exception {
    codecSmallTest(new LzmaCodec());
  }

  @Test
  public void testLzmaCodecLarge() throws Exception {
    codecLargeTest(new LzmaCodec(), 1.1); // poor compressability
    codecLargeTest(new LzmaCodec(), 2);
    codecLargeTest(new LzmaCodec(), 10); // very high compressability
  }

  @Test
  public void testLzmaCodecVeryLarge() throws Exception {
    Configuration conf = new Configuration();
    // LZMA levels range from 1 to 9.
    // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
    conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 1);
    LzmaCodec codec = new LzmaCodec();
    codec.setConf(conf);
    codecVeryLargeTest(codec, 3); // like text
  }

}
