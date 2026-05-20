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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;

@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: allowFastReflectionFallthrough={0}")
public class TestComparatorSerialization {

  public boolean allowFastReflectionFallthrough;

  public TestComparatorSerialization(boolean allowFastReflectionFallthrough) {
    this.allowFastReflectionFallthrough = allowFastReflectionFallthrough;
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @AfterAll
  public static void afterClass() throws Exception {
    // set back to true so that it doesn't affect any other tests
    ProtobufUtil.setAllowFastReflectionFallthrough(true);
  }

  @TestTemplate
  public void testBinaryComparator() throws Exception {
    BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("binaryComparator"));
    assertTrue(binaryComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(binaryComparator))));
  }

  @TestTemplate
  public void testBinaryPrefixComparator() throws Exception {
    BinaryPrefixComparator binaryPrefixComparator =
      new BinaryPrefixComparator(Bytes.toBytes("binaryPrefixComparator"));
    assertTrue(binaryPrefixComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(binaryPrefixComparator))));
  }

  @TestTemplate
  public void testBitComparator() throws Exception {
    BitComparator bitComparator =
      new BitComparator(Bytes.toBytes("bitComparator"), BitComparator.BitwiseOp.XOR);
    assertTrue(bitComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(bitComparator))));
  }

  @TestTemplate
  public void testNullComparator() throws Exception {
    NullComparator nullComparator = new NullComparator();
    assertTrue(nullComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(nullComparator))));
  }

  @TestTemplate
  public void testRegexStringComparator() throws Exception {
    // test without specifying flags
    RegexStringComparator regexStringComparator = new RegexStringComparator(".+-2");
    assertTrue(regexStringComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(regexStringComparator))));

    // test with specifying flags
    try {
      new RegexStringComparator("regex", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    } catch (Throwable t) {
      assertNull(t, "Exception occurred while created the RegexStringComparator object");
    }
  }

  @TestTemplate
  public void testSubstringComparator() throws Exception {
    SubstringComparator substringComparator = new SubstringComparator("substr");
    assertTrue(substringComparator.areSerializedFieldsEqual(
      ProtobufUtil.toComparator(ProtobufUtil.toComparator(substringComparator))));
  }

  @TestTemplate
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
  @TestTemplate
  public void testCustomComparator() throws Exception {
    ByteArrayComparable baseFilter = new BinaryComparator("foo".getBytes());
    ComparatorProtos.Comparator proto = ProtobufUtil.toComparator(baseFilter);
    String suffix = "" + System.currentTimeMillis() + allowFastReflectionFallthrough;
    String className = "CustomLoadedComparator" + suffix;
    proto = proto.toBuilder().setName(className).build();

    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
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
    String code =
      IOUtils.toString(getClass().getResourceAsStream("/CustomLoadedComparator.java.template"),
        Charset.defaultCharset()).replaceAll("\\$\\{suffix\\}", suffix);
    ClassLoaderTestHelper.buildJar(dataTestDir, className, code,
      ClassLoaderTestHelper.localDirPath(conf));

    // Disallow fallthrough at first. We expect below to fail because the custom comparator is not
    // available at initialization so not in the cache.
    ProtobufUtil.setAllowFastReflectionFallthrough(false);
    try {
      ProtobufUtil.toComparator(proto);
      fail("expected to fail");
    } catch (IOException e) {
      // do nothing, this is expected
    }

    // Now the deserialization should pass with fallthrough enabled. This proves that custom
    // comparators can work despite not being supported by cache.
    ProtobufUtil.setAllowFastReflectionFallthrough(true);
    ProtobufUtil.toComparator(proto);
  }

}
