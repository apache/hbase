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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This class tests ParseFilter.java
 * It tests the entire work flow from when a string is given by the user
 * and how it is parsed to construct the corresponding Filter object
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestParseFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestParseFilter.class);

  ParseFilter f;
  Filter filter;

  @Before
  public void setUp() throws Exception {
    f = new ParseFilter();
  }

  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void testKeyOnlyFilter() throws IOException {
    String filterString = "KeyOnlyFilter()";
    doTestFilter(filterString, KeyOnlyFilter.class);

    String filterString2 = "KeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testFirstKeyOnlyFilter() throws IOException {
    String filterString = " FirstKeyOnlyFilter( ) ";
    doTestFilter(filterString, FirstKeyOnlyFilter.class);

    String filterString2 = " FirstKeyOnlyFilter ('') ";
    byte [] filterStringAsByteArray2 = Bytes.toBytes(filterString2);
    try {
      filter = f.parseFilterString(filterStringAsByteArray2);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testPrefixFilter() throws IOException {
    String filterString = " PrefixFilter('row' ) ";
    PrefixFilter prefixFilter = doTestFilter(filterString, PrefixFilter.class);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals("row", new String(prefix, StandardCharsets.UTF_8));


    filterString = " PrefixFilter(row)";
    try {
      doTestFilter(filterString, PrefixFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testColumnPrefixFilter() throws IOException {
    String filterString = " ColumnPrefixFilter('qualifier' ) ";
    ColumnPrefixFilter columnPrefixFilter =
      doTestFilter(filterString, ColumnPrefixFilter.class);
    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals("qualifier", new String(columnPrefix, StandardCharsets.UTF_8));
  }

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {
    String filterString = " MultipleColumnPrefixFilter('qualifier1', 'qualifier2' ) ";
    MultipleColumnPrefixFilter multipleColumnPrefixFilter =
      doTestFilter(filterString, MultipleColumnPrefixFilter.class);
    byte [][] prefixes = multipleColumnPrefixFilter.getPrefix();
    assertEquals("qualifier1", new String(prefixes[0], StandardCharsets.UTF_8));
    assertEquals("qualifier2", new String(prefixes[1], StandardCharsets.UTF_8));
  }

  @Test
  public void testColumnCountGetFilter() throws IOException {
    String filterString = " ColumnCountGetFilter(4)";
    ColumnCountGetFilter columnCountGetFilter =
      doTestFilter(filterString, ColumnCountGetFilter.class);
    int limit = columnCountGetFilter.getLimit();
    assertEquals(4, limit);

    filterString = " ColumnCountGetFilter('abc')";
    try {
      doTestFilter(filterString, ColumnCountGetFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }

    filterString = " ColumnCountGetFilter(2147483648)";
    try {
      doTestFilter(filterString, ColumnCountGetFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testPageFilter() throws IOException {
    String filterString = " PageFilter(4)";
    PageFilter pageFilter =
      doTestFilter(filterString, PageFilter.class);
    long pageSize = pageFilter.getPageSize();
    assertEquals(4, pageSize);

    filterString = " PageFilter('123')";
    try {
      doTestFilter(filterString, PageFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("PageFilter needs an int as an argument");
    }
  }

  @Test
  public void testColumnPaginationFilter() throws IOException {
    String filterString = "ColumnPaginationFilter(4, 6)";
    ColumnPaginationFilter columnPaginationFilter =
      doTestFilter(filterString, ColumnPaginationFilter.class);
    int limit = columnPaginationFilter.getLimit();
    assertEquals(4, limit);
    int offset = columnPaginationFilter.getOffset();
    assertEquals(6, offset);

    filterString = " ColumnPaginationFilter('124')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '123a')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter needs two ints as arguments");
    }

    filterString = " ColumnPaginationFilter('4' , '-123')";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("ColumnPaginationFilter arguments should not be negative");
    }
  }

  @Test
  public void testInclusiveStopFilter() throws IOException {
    String filterString = "InclusiveStopFilter ('row 3')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals("row 3", new String(stopRowKey, StandardCharsets.UTF_8));
  }


  @Test
  public void testTimestampsFilter() throws IOException {
    String filterString = "TimestampsFilter(9223372036854775806, 6)";
    TimestampsFilter timestampsFilter =
      doTestFilter(filterString, TimestampsFilter.class);
    List<Long> timestamps = timestampsFilter.getTimestamps();
    assertEquals(2, timestamps.size());
    assertEquals(Long.valueOf(6), timestamps.get(0));

    filterString = "TimestampsFilter()";
    timestampsFilter = doTestFilter(filterString, TimestampsFilter.class);
    timestamps = timestampsFilter.getTimestamps();
    assertEquals(0, timestamps.size());

    filterString = "TimestampsFilter(9223372036854775808, 6)";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Long Argument was too large");
    }

    filterString = "TimestampsFilter(-45, 6)";
    try {
      doTestFilter(filterString, ColumnPaginationFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Timestamp Arguments should not be negative");
    }
  }

  @Test
  public void testRowFilter() throws IOException {
    String filterString = "RowFilter ( =,   'binary:regionse')";
    RowFilter rowFilter =
      doTestFilter(filterString, RowFilter.class);
    assertEquals(CompareOperator.EQUAL, rowFilter.getCompareOperator());
    assertTrue(rowFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) rowFilter.getComparator();
    assertEquals("regionse", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testFamilyFilter() throws IOException {
    String filterString = "FamilyFilter(>=, 'binaryprefix:pre')";
    FamilyFilter familyFilter =
      doTestFilter(filterString, FamilyFilter.class);
    assertEquals(CompareOperator.GREATER_OR_EQUAL, familyFilter.getCompareOperator());
    assertTrue(familyFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) familyFilter.getComparator();
    assertEquals("pre", new String(binaryPrefixComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testQualifierFilter() throws IOException {
    String filterString = "QualifierFilter(=, 'regexstring:pre*')";
    QualifierFilter qualifierFilter =
      doTestFilter(filterString, QualifierFilter.class);
    assertEquals(CompareOperator.EQUAL, qualifierFilter.getCompareOperator());
    assertTrue(qualifierFilter.getComparator() instanceof RegexStringComparator);
    RegexStringComparator regexStringComparator =
      (RegexStringComparator) qualifierFilter.getComparator();
    assertEquals("pre*", new String(regexStringComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testValueFilter() throws IOException {
    String filterString = "ValueFilter(!=, 'substring:pre')";
    ValueFilter valueFilter =
      doTestFilter(filterString, ValueFilter.class);
    assertEquals(CompareOperator.NOT_EQUAL, valueFilter.getCompareOperator());
    assertTrue(valueFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) valueFilter.getComparator();
    assertEquals("pre", new String(substringComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testColumnRangeFilter() throws IOException {
    String filterString = "ColumnRangeFilter('abc', true, 'xyz', false)";
    ColumnRangeFilter columnRangeFilter =
      doTestFilter(filterString, ColumnRangeFilter.class);
    assertEquals("abc", new String(columnRangeFilter.getMinColumn(), StandardCharsets.UTF_8));
    assertEquals("xyz", new String(columnRangeFilter.getMaxColumn(), StandardCharsets.UTF_8));
    assertTrue(columnRangeFilter.isMinColumnInclusive());
    assertFalse(columnRangeFilter.isMaxColumnInclusive());
  }

  @Test
  public void testDependentColumnFilter() throws IOException {
    String filterString = "DependentColumnFilter('family', 'qualifier', true, =, 'binary:abc')";
    DependentColumnFilter dependentColumnFilter =
      doTestFilter(filterString, DependentColumnFilter.class);
    assertEquals("family", new String(dependentColumnFilter.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier",
        new String(dependentColumnFilter.getQualifier(), StandardCharsets.UTF_8));
    assertTrue(dependentColumnFilter.getDropDependentColumn());
    assertEquals(CompareOperator.EQUAL, dependentColumnFilter.getCompareOperator());
    assertTrue(dependentColumnFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator)dependentColumnFilter.getComparator();
    assertEquals("abc", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testSingleColumnValueFilter() throws IOException {
    String filterString = "SingleColumnValueFilter " +
      "('family', 'qualifier', >=, 'binary:a', true, false)";
    SingleColumnValueFilter singleColumnValueFilter =
      doTestFilter(filterString, SingleColumnValueFilter.class);
    assertEquals("family", new String(singleColumnValueFilter.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier",
        new String(singleColumnValueFilter.getQualifier(), StandardCharsets.UTF_8));
    assertEquals(CompareOperator.GREATER_OR_EQUAL, singleColumnValueFilter.getCompareOperator());
    assertTrue(singleColumnValueFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) singleColumnValueFilter.getComparator();
    assertEquals("a", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
    assertTrue(singleColumnValueFilter.getFilterIfMissing());
    assertFalse(singleColumnValueFilter.getLatestVersionOnly());


    filterString = "SingleColumnValueFilter ('family', 'qualifier', >, 'binaryprefix:a')";
    singleColumnValueFilter = doTestFilter(filterString, SingleColumnValueFilter.class);
    assertEquals("family", new String(singleColumnValueFilter.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier",
        new String(singleColumnValueFilter.getQualifier(), StandardCharsets.UTF_8));
    assertEquals(CompareOperator.GREATER, singleColumnValueFilter.getCompareOperator());
    assertTrue(singleColumnValueFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) singleColumnValueFilter.getComparator();
    assertEquals("a", new String(binaryPrefixComparator.getValue(), StandardCharsets.UTF_8));
    assertFalse(singleColumnValueFilter.getFilterIfMissing());
    assertTrue(singleColumnValueFilter.getLatestVersionOnly());
  }

  @Test
  public void testSingleColumnValueExcludeFilter() throws IOException {
    String filterString =
      "SingleColumnValueExcludeFilter ('family', 'qualifier', <, 'binaryprefix:a')";
    SingleColumnValueExcludeFilter singleColumnValueExcludeFilter =
      doTestFilter(filterString, SingleColumnValueExcludeFilter.class);
    assertEquals(CompareOperator.LESS, singleColumnValueExcludeFilter.getCompareOperator());
    assertEquals("family",
        new String(singleColumnValueExcludeFilter.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier",
        new String(singleColumnValueExcludeFilter.getQualifier(), StandardCharsets.UTF_8));
    assertEquals("a", new String(singleColumnValueExcludeFilter.getComparator().getValue(),
        StandardCharsets.UTF_8));
    assertFalse(singleColumnValueExcludeFilter.getFilterIfMissing());
    assertTrue(singleColumnValueExcludeFilter.getLatestVersionOnly());

    filterString = "SingleColumnValueExcludeFilter " +
      "('family', 'qualifier', <=, 'binaryprefix:a', true, false)";
    singleColumnValueExcludeFilter =
      doTestFilter(filterString, SingleColumnValueExcludeFilter.class);
    assertEquals("family",
        new String(singleColumnValueExcludeFilter.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier",
        new String(singleColumnValueExcludeFilter.getQualifier(), StandardCharsets.UTF_8));
    assertEquals(CompareOperator.LESS_OR_EQUAL,
        singleColumnValueExcludeFilter.getCompareOperator());
    assertTrue(singleColumnValueExcludeFilter.getComparator() instanceof BinaryPrefixComparator);
    BinaryPrefixComparator binaryPrefixComparator =
      (BinaryPrefixComparator) singleColumnValueExcludeFilter.getComparator();
    assertEquals("a", new String(binaryPrefixComparator.getValue(), StandardCharsets.UTF_8));
    assertTrue(singleColumnValueExcludeFilter.getFilterIfMissing());
    assertFalse(singleColumnValueExcludeFilter.getLatestVersionOnly());
  }

  @Test
  public void testSkipFilter() throws IOException {
    String filterString = "SKIP ValueFilter( =,  'binary:0')";
    SkipFilter skipFilter =
      doTestFilter(filterString, SkipFilter.class);
    assertTrue(skipFilter.getFilter() instanceof ValueFilter);
    ValueFilter valueFilter = (ValueFilter) skipFilter.getFilter();

    assertEquals(CompareOperator.EQUAL, valueFilter.getCompareOperator());
    assertTrue(valueFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) valueFilter.getComparator();
    assertEquals("0", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testWhileFilter() throws IOException {
    String filterString = " WHILE   RowFilter ( !=, 'binary:row1')";
    WhileMatchFilter whileMatchFilter =
      doTestFilter(filterString, WhileMatchFilter.class);
    assertTrue(whileMatchFilter.getFilter() instanceof RowFilter);
    RowFilter rowFilter = (RowFilter) whileMatchFilter.getFilter();

    assertEquals(CompareOperator.NOT_EQUAL, rowFilter.getCompareOperator());
    assertTrue(rowFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) rowFilter.getComparator();
    assertEquals("row1", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testCompoundFilter1() throws IOException {
    String filterString = " (PrefixFilter ('realtime')AND  FirstKeyOnlyFilter())";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);
    PrefixFilter PrefixFilter = (PrefixFilter) filters.get(0);
    byte [] prefix = PrefixFilter.getPrefix();
    assertEquals("realtime", new String(prefix, StandardCharsets.UTF_8));
    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);
  }

  @Test
  public void testCompoundFilter2() throws IOException {
    String filterString = "(PrefixFilter('realtime') AND QualifierFilter (>=, 'binary:e'))" +
      "OR FamilyFilter (=, 'binary:qualifier') ";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
    ArrayList<Filter> filterListFilters = (ArrayList<Filter>) filterList.getFilters();
    assertTrue(filterListFilters.get(0) instanceof FilterList);
    assertTrue(filterListFilters.get(1) instanceof FamilyFilter);
    assertEquals(FilterList.Operator.MUST_PASS_ONE, filterList.getOperator());

    filterList = (FilterList) filterListFilters.get(0);
    FamilyFilter familyFilter = (FamilyFilter) filterListFilters.get(1);

    filterListFilters = (ArrayList<Filter>)filterList.getFilters();
    assertTrue(filterListFilters.get(0) instanceof PrefixFilter);
    assertTrue(filterListFilters.get(1) instanceof QualifierFilter);
    assertEquals(FilterList.Operator.MUST_PASS_ALL, filterList.getOperator());

    assertEquals(CompareOperator.EQUAL, familyFilter.getCompareOperator());
    assertTrue(familyFilter.getComparator() instanceof BinaryComparator);
    BinaryComparator binaryComparator = (BinaryComparator) familyFilter.getComparator();
    assertEquals("qualifier", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));

    PrefixFilter prefixFilter = (PrefixFilter) filterListFilters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals("realtime", new String(prefix, StandardCharsets.UTF_8));

    QualifierFilter qualifierFilter = (QualifierFilter) filterListFilters.get(1);
    assertEquals(CompareOperator.GREATER_OR_EQUAL, qualifierFilter.getCompareOperator());
    assertTrue(qualifierFilter.getComparator() instanceof BinaryComparator);
    binaryComparator = (BinaryComparator) qualifierFilter.getComparator();
    assertEquals("e", new String(binaryComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testCompoundFilter3() throws IOException {
    String filterString = " ColumnPrefixFilter ('realtime')AND  " +
      "FirstKeyOnlyFilter() OR SKIP FamilyFilter(=, 'substring:hihi')";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof SkipFilter);

    filterList = (FilterList) filters.get(0);
    SkipFilter skipFilter = (SkipFilter) filters.get(1);

    filters = (ArrayList<Filter>) filterList.getFilters();
    assertTrue(filters.get(0) instanceof ColumnPrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);

    ColumnPrefixFilter columnPrefixFilter = (ColumnPrefixFilter) filters.get(0);
    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals("realtime", new String(columnPrefix, StandardCharsets.UTF_8));

    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);

    assertTrue(skipFilter.getFilter() instanceof FamilyFilter);
    FamilyFilter familyFilter = (FamilyFilter) skipFilter.getFilter();

    assertEquals(CompareOperator.EQUAL, familyFilter.getCompareOperator());
    assertTrue(familyFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) familyFilter.getComparator();
    assertEquals("hihi", new String(substringComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testCompoundFilter4() throws IOException {
    String filterString = " ColumnPrefixFilter ('realtime') OR " +
      "FirstKeyOnlyFilter() OR SKIP FamilyFilter(=, 'substring:hihi')";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof ColumnPrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);
    assertTrue(filters.get(2) instanceof SkipFilter);

    ColumnPrefixFilter columnPrefixFilter = (ColumnPrefixFilter) filters.get(0);
    FirstKeyOnlyFilter firstKeyOnlyFilter = (FirstKeyOnlyFilter) filters.get(1);
    SkipFilter skipFilter = (SkipFilter) filters.get(2);

    byte [] columnPrefix = columnPrefixFilter.getPrefix();
    assertEquals("realtime", new String(columnPrefix, StandardCharsets.UTF_8));

    assertTrue(skipFilter.getFilter() instanceof FamilyFilter);
    FamilyFilter familyFilter = (FamilyFilter) skipFilter.getFilter();

    assertEquals(CompareOperator.EQUAL, familyFilter.getCompareOperator());
    assertTrue(familyFilter.getComparator() instanceof SubstringComparator);
    SubstringComparator substringComparator =
      (SubstringComparator) familyFilter.getComparator();
    assertEquals("hihi", new String(substringComparator.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testCompoundFilter5() throws IOException {
    String filterStr = "(ValueFilter(!=, 'substring:pre'))";
    ValueFilter valueFilter = doTestFilter(filterStr, ValueFilter.class);
    assertTrue(valueFilter.getComparator() instanceof SubstringComparator);

    filterStr = "(ValueFilter(>=,'binary:x') AND (ValueFilter(<=,'binary:y')))"
            + " OR ValueFilter(=,'binary:ab')";
    filter = f.parseFilterString(filterStr);
    assertTrue(filter instanceof FilterList);
    List<Filter> list = ((FilterList) filter).getFilters();
    assertEquals(2, list.size());
    assertTrue(list.get(0) instanceof FilterList);
    assertTrue(list.get(1) instanceof ValueFilter);
  }

  @Test
  public void testIncorrectCompareOperator() throws IOException {
    String filterString = "RowFilter ('>>' , 'binary:region')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect compare operator >>");
    }
  }

  @Test
  public void testIncorrectComparatorType() throws IOException {
    String  filterString = "RowFilter ('>=' , 'binaryoperator:region')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("Incorrect comparator type: binaryoperator");
    }

    filterString = "RowFilter ('>=' 'regexstring:pre*')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("RegexStringComparator can only be used with EQUAL or NOT_EQUAL");
    }

    filterString = "SingleColumnValueFilter" +
      " ('family', 'qualifier', '>=', 'substring:a', 'true', 'false')')";
    try {
      doTestFilter(filterString, RowFilter.class);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println("SubtringComparator can only be used with EQUAL or NOT_EQUAL");
    }
  }

  @Test
  public void testPrecedence1() throws IOException {
    String filterString = " (PrefixFilter ('realtime')AND  FirstKeyOnlyFilter()" +
      " OR KeyOnlyFilter())";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);

    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof KeyOnlyFilter);

    filterList = (FilterList) filters.get(0);
    filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof FirstKeyOnlyFilter);

    PrefixFilter prefixFilter = (PrefixFilter)filters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals("realtime", new String(prefix, StandardCharsets.UTF_8));
  }

  @Test
  public void testPrecedence2() throws IOException {
    String filterString = " PrefixFilter ('realtime')AND  SKIP FirstKeyOnlyFilter()" +
      "OR KeyOnlyFilter()";
    FilterList filterList =
      doTestFilter(filterString, FilterList.class);
    ArrayList<Filter> filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof FilterList);
    assertTrue(filters.get(1) instanceof KeyOnlyFilter);

    filterList = (FilterList) filters.get(0);
    filters = (ArrayList<Filter>) filterList.getFilters();

    assertTrue(filters.get(0) instanceof PrefixFilter);
    assertTrue(filters.get(1) instanceof SkipFilter);

    PrefixFilter prefixFilter = (PrefixFilter)filters.get(0);
    byte [] prefix = prefixFilter.getPrefix();
    assertEquals("realtime", new String(prefix, StandardCharsets.UTF_8));

    SkipFilter skipFilter = (SkipFilter)filters.get(1);
    assertTrue(skipFilter.getFilter() instanceof FirstKeyOnlyFilter);
  }

  @Test
  public void testUnescapedQuote1() throws IOException {
    String filterString = "InclusiveStopFilter ('row''3')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals("row'3", new String(stopRowKey, StandardCharsets.UTF_8));
  }

  @Test
  public void testUnescapedQuote2() throws IOException {
    String filterString = "InclusiveStopFilter ('row''3''')";
    InclusiveStopFilter inclusiveStopFilter =
      doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals("row'3'", new String(stopRowKey, StandardCharsets.UTF_8));
  }

  @Test
  public void testUnescapedQuote3() throws IOException {
    String filterString = " InclusiveStopFilter ('''')";
    InclusiveStopFilter inclusiveStopFilter = doTestFilter(filterString, InclusiveStopFilter.class);
    byte [] stopRowKey = inclusiveStopFilter.getStopRowKey();
    assertEquals("'", new String(stopRowKey, StandardCharsets.UTF_8));
  }

  @Test
  public void testIncorrectFilterString() throws IOException {
    String filterString = "()";
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    try {
      filter = f.parseFilterString(filterStringAsByteArray);
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testCorrectFilterString() throws IOException {
    String filterString = "(FirstKeyOnlyFilter())";
    FirstKeyOnlyFilter firstKeyOnlyFilter = doTestFilter(filterString, FirstKeyOnlyFilter.class);
  }

  @Test
  public void testRegisterFilter() {
    ParseFilter.registerFilter("MyFilter", "some.class");

    assertTrue(f.getSupportedFilters().contains("MyFilter"));
  }

  private <T extends Filter> T doTestFilter(String filterString, Class<T> clazz)
      throws IOException {
    byte [] filterStringAsByteArray = Bytes.toBytes(filterString);
    filter = f.parseFilterString(filterStringAsByteArray);
    assertEquals(clazz, filter.getClass());
    return clazz.cast(filter);
  }

  @Test
  public void testColumnValueFilter() throws IOException {
    String filterString = "ColumnValueFilter ('family', 'qualifier', <, 'binaryprefix:value')";
    ColumnValueFilter cvf = doTestFilter(filterString, ColumnValueFilter.class);
    assertEquals("family", new String(cvf.getFamily(), StandardCharsets.UTF_8));
    assertEquals("qualifier", new String(cvf.getQualifier(), StandardCharsets.UTF_8));
    assertEquals(CompareOperator.LESS, cvf.getCompareOperator());
    assertTrue(cvf.getComparator() instanceof BinaryPrefixComparator);
    assertEquals("value", new String(cvf.getComparator().getValue(), StandardCharsets.UTF_8));
  }
}
