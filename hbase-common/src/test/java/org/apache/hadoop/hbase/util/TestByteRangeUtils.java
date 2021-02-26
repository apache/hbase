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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestByteRangeUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestByteRangeUtils.class);

  @Test
  public void testNumEqualPrefixBytes() {
    assertEquals(0, ByteRangeUtils.numEqualPrefixBytes(
            new SimpleByteRange(new byte[]{1, 2, 3}),
            new SimpleByteRange(new byte[]{4, 5, 6}), 1));
    assertEquals(2, ByteRangeUtils.numEqualPrefixBytes(
            new SimpleByteRange(new byte[]{1, 2, 3}),
            new SimpleByteRange(new byte[]{0, 1, 2}), 1));
  }

  @Test
  public void testCopyToNewArrays() {
    assertEquals(new ArrayList<>(),
            ByteRangeUtils.copyToNewArrays(null));
    assertArrayEquals(new byte[]{1, 2, 3},
            ByteRangeUtils.copyToNewArrays(new ArrayList<>(Arrays.asList(
                    new SimpleByteRange(new byte[]{1, 2, 3}),
                    new SimpleByteRange(new byte[]{4, 5, 6})))).get(0));
    assertArrayEquals(new byte[]{4, 5, 6},
            ByteRangeUtils.copyToNewArrays(new ArrayList<>(Arrays.asList(
                    new SimpleByteRange(new byte[]{1, 2, 3}),
                    new SimpleByteRange(new byte[]{4, 5, 6})))).get(1));
  }

  @Test
  public void testFromArrays() {
    assertEquals(new ArrayList<>(), ByteRangeUtils.fromArrays(null));
    assertEquals(new ArrayList<>(Arrays.asList(
            new SimpleMutableByteRange(new byte[]{1, 2, 3}),
            new SimpleMutableByteRange(new byte[]{4, 5, 6}))),
            ByteRangeUtils.fromArrays(new ArrayList<>(
                    Arrays.asList(new byte[]{1, 2, 3}, new byte[]{4, 5, 6}))));
  }
}
