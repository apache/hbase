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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

@Tag(FilterTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: allowFastReflectionFallthrough={0}")
public class TestFilterSerialization {

  public boolean allowFastReflectionFallthrough;

  public TestFilterSerialization(boolean allowFastReflectionFallthrough) {
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
  public void testColumnCountGetFilter() throws Exception {
    ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(1);
    assertTrue(columnCountGetFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnCountGetFilter))));
  }

  @TestTemplate
  public void testColumnPaginationFilter() throws Exception {
    ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(1, 7);
    assertTrue(columnPaginationFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnPaginationFilter))));
  }

  @TestTemplate
  public void testColumnPrefixFilter() throws Exception {
    // empty string
    ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(""));
    assertTrue(columnPrefixFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnPrefixFilter))));

    // non-empty string
    columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(""));
    assertTrue(columnPrefixFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnPrefixFilter))));
  }

  @TestTemplate
  public void testColumnRangeFilter() throws Exception {
    // null columns
    ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(null, true, null, false);
    assertTrue(columnRangeFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnRangeFilter))));

    // non-null columns
    columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true);
    assertTrue(columnRangeFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnRangeFilter))));
  }

  @TestTemplate
  public void testDependentColumnFilter() throws Exception {
    // null column qualifier/family
    DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(null, null);
    assertTrue(dependentColumnFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(dependentColumnFilter))));

    // non-null column qualifier/family
    dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes("family"),
      Bytes.toBytes("qual"), true, CompareOperator.GREATER_OR_EQUAL,
      new BitComparator(Bytes.toBytes("bitComparator"), BitComparator.BitwiseOp.OR));
    assertTrue(dependentColumnFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(dependentColumnFilter))));
  }

  @TestTemplate
  public void testFamilyFilter() throws Exception {
    FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL,
      new BinaryPrefixComparator(Bytes.toBytes("testValueOne")));
    assertTrue(familyFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(familyFilter))));
  }

  @TestTemplate
  public void testFilterList() throws Exception {
    // empty filter list
    FilterList filterList = new FilterList(new LinkedList<>());
    assertTrue(filterList
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(filterList))));

    // non-empty filter list
    LinkedList<Filter> list = new LinkedList<>();
    list.add(new ColumnCountGetFilter(1));
    list.add(new RowFilter(CompareOperator.EQUAL, new SubstringComparator("testFilterList")));
    assertTrue(filterList
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(filterList))));
  }

  @TestTemplate
  public void testFilterWrapper() throws Exception {
    FilterWrapper filterWrapper =
      new FilterWrapper(new ColumnRangeFilter(Bytes.toBytes("e"), false, Bytes.toBytes("f"), true));
    assertTrue(filterWrapper
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(filterWrapper))));
  }

  @TestTemplate
  public void testFirstKeyOnlyFilter() throws Exception {
    FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
    assertTrue(firstKeyOnlyFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(firstKeyOnlyFilter))));
  }

  @TestTemplate
  public void testFuzzyRowFilter() throws Exception {
    LinkedList<Pair<byte[], byte[]>> fuzzyList = new LinkedList<>();
    fuzzyList.add(new Pair<>(Bytes.toBytes("999"), new byte[] { 0, 0, 1 }));
    fuzzyList.add(new Pair<>(Bytes.toBytes("abcd"), new byte[] { 1, 0, 1, 1 }));
    FuzzyRowFilter fuzzyRowFilter = new FuzzyRowFilter(fuzzyList);
    assertTrue(fuzzyRowFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(fuzzyRowFilter))));
  }

  @TestTemplate
  public void testInclusiveStopFilter() throws Exception {
    // InclusveStopFilter with null stopRowKey
    InclusiveStopFilter inclusiveStopFilter = new InclusiveStopFilter(null);
    assertTrue(inclusiveStopFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(inclusiveStopFilter))));

    // InclusveStopFilter with non-null stopRowKey
    inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes("inclusiveStopFilter"));
    assertTrue(inclusiveStopFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(inclusiveStopFilter))));
  }

  @TestTemplate
  public void testKeyOnlyFilter() throws Exception {
    // KeyOnlyFilter with lenAsVal
    KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter(true);
    assertTrue(keyOnlyFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(keyOnlyFilter))));

    // KeyOnlyFilter without lenAsVal
    keyOnlyFilter = new KeyOnlyFilter();
    assertTrue(keyOnlyFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(keyOnlyFilter))));
  }

  @TestTemplate
  public void testMultipleColumnPrefixFilter() throws Exception {
    // empty array
    byte[][] prefixes = null;
    MultipleColumnPrefixFilter multipleColumnPrefixFilter =
      new MultipleColumnPrefixFilter(prefixes);
    assertTrue(multipleColumnPrefixFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(multipleColumnPrefixFilter))));

    // non-empty array
    prefixes = new byte[2][];
    prefixes[0] = Bytes.toBytes("a");
    prefixes[1] = Bytes.toBytes("");
    multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
    assertTrue(multipleColumnPrefixFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(multipleColumnPrefixFilter))));
  }

  @TestTemplate
  public void testPageFilter() throws Exception {
    PageFilter pageFilter = new PageFilter(6);
    assertTrue(pageFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(pageFilter))));
  }

  @TestTemplate
  public void testPrefixFilter() throws Exception {
    // null prefix
    PrefixFilter prefixFilter = new PrefixFilter(null);
    assertTrue(prefixFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(prefixFilter))));

    // non-null prefix
    prefixFilter = new PrefixFilter(Bytes.toBytes("abc"));
    assertTrue(prefixFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(prefixFilter))));
  }

  @TestTemplate
  public void testQualifierFilter() throws Exception {
    QualifierFilter qualifierFilter =
      new QualifierFilter(CompareOperator.EQUAL, new NullComparator());
    assertTrue(qualifierFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(qualifierFilter))));
  }

  @TestTemplate
  public void testRandomRowFilter() throws Exception {
    RandomRowFilter randomRowFilter = new RandomRowFilter((float) 0.1);
    assertTrue(randomRowFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(randomRowFilter))));
  }

  @TestTemplate
  public void testRowFilter() throws Exception {
    RowFilter rowFilter =
      new RowFilter(CompareOperator.EQUAL, new SubstringComparator("testRowFilter"));
    assertTrue(
      rowFilter.areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(rowFilter))));
  }

  @TestTemplate
  public void testSingleColumnValueExcludeFilter() throws Exception {
    // null family/column SingleColumnValueExcludeFilter
    SingleColumnValueExcludeFilter singleColumnValueExcludeFilter =
      new SingleColumnValueExcludeFilter(null, null, CompareOperator.GREATER_OR_EQUAL,
        Bytes.toBytes("value"));
    assertTrue(singleColumnValueExcludeFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(singleColumnValueExcludeFilter))));

    // non-null family/column SingleColumnValueFilter
    singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("fam"),
      Bytes.toBytes("qual"), CompareOperator.LESS_OR_EQUAL, new NullComparator(), false, false);
    assertTrue(singleColumnValueExcludeFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(singleColumnValueExcludeFilter))));
  }

  @TestTemplate
  public void testSingleColumnValueFilter() throws Exception {
    // null family/column SingleColumnValueFilter
    SingleColumnValueFilter singleColumnValueFilter =
      new SingleColumnValueFilter(null, null, CompareOperator.LESS, Bytes.toBytes("value"));
    assertTrue(singleColumnValueFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(singleColumnValueFilter))));

    // non-null family/column SingleColumnValueFilter
    singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("family"),
      Bytes.toBytes("qualifier"), CompareOperator.NOT_EQUAL, new NullComparator(), true, true);
    assertTrue(singleColumnValueFilter.areSerializedFieldsEqual(
      ProtobufUtil.toFilter(ProtobufUtil.toFilter(singleColumnValueFilter))));
  }

  @TestTemplate
  public void testSkipFilter() throws Exception {
    SkipFilter skipFilter = new SkipFilter(new PageFilter(6));
    assertTrue(skipFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(skipFilter))));
  }

  @TestTemplate
  public void testTimestampsFilter() throws Exception {
    // Empty timestamp list
    TimestampsFilter timestampsFilter = new TimestampsFilter(new LinkedList<>());
    assertTrue(timestampsFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(timestampsFilter))));

    // Non-empty timestamp list
    LinkedList<Long> list = new LinkedList<>();
    list.add(EnvironmentEdgeManager.currentTime());
    list.add(EnvironmentEdgeManager.currentTime());
    timestampsFilter = new TimestampsFilter(list);
    assertTrue(timestampsFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(timestampsFilter))));
  }

  @TestTemplate
  public void testValueFilter() throws Exception {
    ValueFilter valueFilter =
      new ValueFilter(CompareOperator.NO_OP, new BinaryComparator(Bytes.toBytes("testValueOne")));
    assertTrue(valueFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(valueFilter))));
  }

  @TestTemplate
  public void testWhileMatchFilter() throws Exception {
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(
      new ColumnRangeFilter(Bytes.toBytes("c"), false, Bytes.toBytes("d"), true));
    assertTrue(whileMatchFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(whileMatchFilter))));
  }

  @TestTemplate
  public void testMultiRowRangeFilter() throws Exception {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter multiRowRangeFilter = new MultiRowRangeFilter(ranges);
    assertTrue(multiRowRangeFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(multiRowRangeFilter))));
  }

  @TestTemplate
  public void testColumnValueFilter() throws Exception {
    ColumnValueFilter columnValueFilter = new ColumnValueFilter(Bytes.toBytes("family"),
      Bytes.toBytes("qualifier"), CompareOperator.EQUAL, Bytes.toBytes("value"));
    assertTrue(columnValueFilter
      .areSerializedFieldsEqual(ProtobufUtil.toFilter(ProtobufUtil.toFilter(columnValueFilter))));
  }

  /**
   * Test that we can load and deserialize custom filters. Good to have generally, but also proves
   * that this still works after HBASE-27276 despite not going through our fast function caches.
   */
  @TestTemplate
  public void testCustomFilter() throws Exception {
    Filter baseFilter = new PrefixFilter("foo".getBytes());
    FilterProtos.Filter filterProto = ProtobufUtil.toFilter(baseFilter);
    String suffix = "" + System.currentTimeMillis() + allowFastReflectionFallthrough;
    String className = "CustomLoadedFilter" + suffix;
    filterProto = filterProto.toBuilder().setName(className).build();

    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtil testUtil = new HBaseTestingUtil();
    String dataTestDir = testUtil.getDataTestDir().toString();

    // First make sure the test bed is clean, delete any pre-existing class.
    // Below toComparator call is expected to fail because the comparator is not loaded now
    ClassLoaderTestHelper.deleteClass(className, dataTestDir, conf);
    try {
      Filter filter = ProtobufUtil.toFilter(filterProto);
      fail("expected to fail");
    } catch (DoNotRetryIOException e) {
      // do nothing, this is expected
    }

    // Write a jar to be loaded into the classloader
    String code = StringSubstitutor
      .replace(IOUtils.toString(getClass().getResourceAsStream("/CustomLoadedFilter.java.template"),
        Charset.defaultCharset()), Collections.singletonMap("suffix", suffix));
    ClassLoaderTestHelper.buildJar(dataTestDir, className, code,
      ClassLoaderTestHelper.localDirPath(conf));

    // Disallow fallthrough at first. We expect below to fail because the custom filter is not
    // available at initialization so not in the cache.
    ProtobufUtil.setAllowFastReflectionFallthrough(false);
    try {
      ProtobufUtil.toFilter(filterProto);
      fail("expected to fail");
    } catch (DoNotRetryIOException e) {
      // do nothing, this is expected
    }

    // Now the deserialization should pass with fallthrough enabled. This proves that custom
    // filters can work despite not being supported by cache.
    ProtobufUtil.setAllowFastReflectionFallthrough(true);
    ProtobufUtil.toFilter(filterProto);

  }

}
