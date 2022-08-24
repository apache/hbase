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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;

@RunWith(Parameterized.class)
@Category({ FilterTests.class, SmallTests.class })
public class TestComparatorSerialization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestComparatorSerialization.class);

  @Parameterized.Parameter(0)
  public boolean allowFastReflectionFallthrough;

  @Parameterized.Parameters(name = "{index}: allowFastReflectionFallthrough={0}")
  public static Iterable<Object[]> data() {
    return HBaseCommonTestingUtil.BOOLEAN_PARAMETERIZED;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // set back to true so that it doesn't affect any other tests
    ProtobufUtil.setAllowFastReflectionFallthrough(true);
  }

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
    assertTrue(bigDecimalComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(bigDecimalComparator))));
  }

  /**
   * Test that we can load and deserialize custom comparators. Good to have generally, but also
   * proves that this still works after HBASE-27276 despite not going through our fast function
   * caches.
   */
  @Test
  public void testCustomComparator() throws Exception {
    ByteArrayComparable baseFilter = new BinaryComparator("foo".getBytes());
    ComparatorProtos.Comparator proto = ProtobufUtil.toComparator(baseFilter);
    String className = "CustomLoadedComparator" + allowFastReflectionFallthrough;
    proto = proto.toBuilder().setName(className).build();

    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtil testUtil = new HBaseTestingUtil();
    String dataTestDir = testUtil.getDataTestDir().toString();

    // First make sure the test bed is clean, delete any pre-existing class.
    // Below toComparator call is expected to fail because the comparator is not loaded now
    ClassLoaderTestHelper.deleteClass(className, dataTestDir, conf);
    try {
      ProtobufUtil.toComparator(proto);
      fail("expected to fail");
    } catch (IOException e) {
      // do nothing, this is expected
    }

    // Write a jar to be loaded into the classloader
    String code = StringSubstitutor.replace(
      IOUtils.toString(getClass().getResourceAsStream("/CustomLoadedComparator.java"),
        Charset.defaultCharset()),
      Collections.singletonMap("suffix", allowFastReflectionFallthrough));
    ClassLoaderTestHelper.buildJar(dataTestDir, className, code,
      ClassLoaderTestHelper.localDirPath(conf));

    // Disallow fallthrough at first, we expect below to fail
    ProtobufUtil.setAllowFastReflectionFallthrough(false);
    try {
      ProtobufUtil.toComparator(proto);
      fail("expected to fail");
    } catch (IOException e) {
      // do nothing, this is expected
    }
    ProtobufUtil.setAllowFastReflectionFallthrough(true);
    // Now the deserialization should pass
    ProtobufUtil.toComparator(proto);
  }

}
