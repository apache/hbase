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
package org.apache.hadoop.hbase.io.compress.aircompressor;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.CompressionTestBase;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLz4Codec extends CompressionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLz4Codec.class);

  @Test
  public void testLz4CodecSmall() throws Exception {
    codecSmallTest(new Lz4Codec());
  }

  @Test
  public void testLz4CodecLarge() throws Exception {
    codecLargeTest(new Lz4Codec(), 1.1); // poor compressability, expansion with this codec
    codecLargeTest(new Lz4Codec(), 2);
    codecLargeTest(new Lz4Codec(), 10); // high compressability
  }

  @Test
  public void testLz4CodecVeryLarge() throws Exception {
    codecVeryLargeTest(new Lz4Codec(), 3); // like text
  }
}
