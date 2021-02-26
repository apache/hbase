/**
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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({FilterTests.class, SmallTests.class})
public class TestComparatorSerialization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestComparatorSerialization.class);

  @Test
  public void testBinaryComparator() throws Exception {
    BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("binaryComparator"));
    assertTrue(binaryComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(binaryComparator))));
  }

  @Test
  public void testBinaryPrefixComparator() throws Exception {
    BinaryPrefixComparator binaryPrefixComparator =
      new BinaryPrefixComparator(Bytes.toBytes("binaryPrefixComparator"));
    assertTrue(binaryPrefixComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(binaryPrefixComparator))));
  }

  @Test
  public void testBitComparator() throws Exception {
    BitComparator bitComparator =
      new BitComparator(Bytes.toBytes("bitComparator"), BitComparator.BitwiseOp.XOR);
    assertTrue(bitComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(bitComparator))));
  }

  @Test
  public void testNullComparator() throws Exception {
    NullComparator nullComparator = new NullComparator();
    assertTrue(nullComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(nullComparator))));
  }

  @Test
  public void testRegexStringComparator() throws Exception {
    // test without specifying flags
    RegexStringComparator regexStringComparator = new RegexStringComparator(".+-2");
    assertTrue(regexStringComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(regexStringComparator))));

    // test with specifying flags
    try {
      new RegexStringComparator("regex", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    } catch (Throwable t) {
      assertNull("Exception occurred while created the RegexStringComparator object", t);
    }
  }

  @Test
  public void testSubstringComparator() throws Exception {
    SubstringComparator substringComparator = new SubstringComparator("substr");
    assertTrue(substringComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(substringComparator))));
  }

  @Test
  public void testBigDecimalComparator() throws Exception {
    BigDecimal bigDecimal = new BigDecimal(Double.MIN_VALUE);
    BigDecimalComparator bigDecimalComparator = new BigDecimalComparator(bigDecimal);
    assertTrue(bigDecimalComparator.areSerializedFieldsEqual(ProtobufUtil.toComparator(ProtobufUtil
        .toComparator(bigDecimalComparator))));
  }

}
