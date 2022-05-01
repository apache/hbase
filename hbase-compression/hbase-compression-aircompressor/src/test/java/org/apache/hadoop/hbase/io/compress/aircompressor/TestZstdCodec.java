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
    codecLargeTest(new ZstdCodec(), 1.1); // poor compressability, expansion with this codec
    codecLargeTest(new ZstdCodec(), 2);
    codecLargeTest(new ZstdCodec(), 10); // very high compressability
  }

  @Test
  public void testZstdCodecVeryLarge() throws Exception {
    codecVeryLargeTest(new ZstdCodec(), 3); // like text
  }

}
