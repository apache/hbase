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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.CompressionTestBase;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSnappyCodec extends CompressionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnappyCodec.class);

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeTrue(SnappyCodec.isLoaded());
  }

  @Test
  public void testSnappyCodecSmall() throws Exception {
    codecSmallTest(new SnappyCodec());
  }

  @Test
  public void testSnappyCodecLarge() throws Exception {
    codecLargeTest(new SnappyCodec(), 1.1); // poor compressability
    codecLargeTest(new SnappyCodec(), 2);
    codecLargeTest(new SnappyCodec(), 10); // very high compressability
  }

  @Test
  public void testSnappyCodecVeryLarge() throws Exception {
    codecVeryLargeTest(new SnappyCodec(), 3); // like text
  }

}
