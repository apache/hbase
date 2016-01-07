/**
 *
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Tests filter sets
 *
 */
@Category(SmallTests.class)
public class TestFilterList extends TestCase {
  static final int MAX_PAGES = 2;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static byte[] GOOD_BYTES = Bytes.toBytes("abc");
  static byte[] BAD_BYTES = Bytes.toBytes("def");

  /**
   * Test "must pass one"
   * @throws Exception
   */
  public void testMPONE() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPONE =
        new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPONE.reset();
    assertFalse(filterMPONE.filterAllRemaining());

    /* Will pass both */
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* Only pass PageFilter */
    rowkey = Bytes.toBytes("z");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
    assertFalse(filterMPONE.filterRow());

    /* reach MAX_PAGES already, should filter any rows */
    rowkey = Bytes.toBytes("yyy");
    assertTrue(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertFalse(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));

    /* We should filter any row */
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertTrue(filterMPONE.filterAllRemaining());
  }

  /**
   * Test "must pass all"
   * @throws Exception
   */
  public void testMPALL() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPALL.reset();
    assertFalse(filterMPALL.filterAllRemaining());
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPALL.filterKeyValue(kv));
    }
    filterMPALL.reset();
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
    // Should fail here; row should be filtered out.
    KeyValue kv = new KeyValue(rowkey, rowkey, rowkey, rowkey);
    assertTrue(Filter.ReturnCode.NEXT_ROW == filterMPALL.filterKeyValue(kv));
  }

  /**
   * Test list ordering
   * @throws Exception
   */
  public void testOrdering() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PrefixFilter(Bytes.toBytes("yyy")));
    filters.add(new PageFilter(MAX_PAGES));
    Filter filterMPONE =
        new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPONE.reset();
    assertFalse(filterMPONE.filterAllRemaining());

    /* We should be able to fill MAX_PAGES without incrementing page counter */
    byte [] rowkey = Bytes.toBytes("yyyyyyyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* Now let's fill the page filter */
    rowkey = Bytes.toBytes("xxxxxxx");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* We should still be able to include even though page filter is at max */
    rowkey = Bytes.toBytes("yyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }
  }

  private static class AlwaysNextColFilter extends FilterBase {
    public AlwaysNextColFilter() {
      super();
    }
    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
      return ReturnCode.NEXT_COL;
    }
    @Override
    public void readFields(DataInput arg0) throws IOException {}

    @Override
    public void write(DataOutput arg0) throws IOException {}
  }

  /**
   * When we do a "MUST_PASS_ONE" (a logical 'OR') of the two filters
   * we expect to get the same result as the inclusive stop result.
   * @throws Exception
   */
  public void testFilterListWithInclusiveStopFilteMustPassOne() throws Exception {
    byte[] r1 = Bytes.toBytes("Row1");
    byte[] r11 = Bytes.toBytes("Row11");
    byte[] r2 = Bytes.toBytes("Row2");

    FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new AlwaysNextColFilter());
    flist.addFilter(new InclusiveStopFilter(r1));
    flist.filterRowKey(r1, 0, r1.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r1,r1,r1)), ReturnCode.INCLUDE);
    assertEquals(flist.filterKeyValue(new KeyValue(r11,r11,r11)), ReturnCode.INCLUDE);

    flist.reset();
    flist.filterRowKey(r2, 0, r2.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r2,r2,r2)), ReturnCode.SKIP);
  }

  /**
   * When we do a "MUST_PASS_ONE" (a logical 'OR') of the above two filters
   * we expect to get the same result as the 'prefix' only result.
   * @throws Exception
   */
  public void testFilterListTwoFiltersMustPassOne() throws Exception {
  byte[] r1 = Bytes.toBytes("Row1");
    byte[] r11 = Bytes.toBytes("Row11");
    byte[] r2 = Bytes.toBytes("Row2");

    FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new PrefixFilter(r1));
    flist.filterRowKey(r1, 0, r1.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r1,r1,r1)), ReturnCode.INCLUDE);
    assertEquals(flist.filterKeyValue(new KeyValue(r11,r11,r11)), ReturnCode.INCLUDE);

    flist.reset();
    flist.filterRowKey(r2, 0, r2.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r2,r2,r2)), ReturnCode.SKIP);

    flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new AlwaysNextColFilter());
    flist.addFilter(new PrefixFilter(r1));
    flist.filterRowKey(r1, 0, r1.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r1,r1,r1)), ReturnCode.INCLUDE);
    assertEquals(flist.filterKeyValue(new KeyValue(r11,r11,r11)), ReturnCode.INCLUDE);

    flist.reset();
    flist.filterRowKey(r2, 0, r2.length);
    assertEquals(flist.filterKeyValue(new KeyValue(r2,r2,r2)), ReturnCode.SKIP);
  }

  /**
   * Test serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

    // Decompose filterMPALL to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filterMPALL.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();

    // Recompose filterMPALL.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    FilterList newFilter = new FilterList();
    newFilter.readFields(in);

    // TODO: Run TESTS!!!
  }

  /**
   * Test filterKeyValue logic.
   * @throws Exception
   */
  public void testFilterKeyValue() throws Exception {
    Filter includeFilter = new FilterBase() {
      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        return Filter.ReturnCode.INCLUDE;
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter alternateFilter = new FilterBase() {
      boolean returnInclude = true;

      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        Filter.ReturnCode returnCode = returnInclude ? Filter.ReturnCode.INCLUDE :
                                                       Filter.ReturnCode.SKIP;
        returnInclude = !returnInclude;
        return returnCode;
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter alternateIncludeFilter = new FilterBase() {
      boolean returnIncludeOnly = false;

      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        Filter.ReturnCode returnCode = returnIncludeOnly ? Filter.ReturnCode.INCLUDE :
                                                           Filter.ReturnCode.INCLUDE_AND_NEXT_COL;
        returnIncludeOnly = !returnIncludeOnly;
        return returnCode;
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    // Check must pass one filter.
    FilterList mpOnefilterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter[] { includeFilter, alternateIncludeFilter, alternateFilter }));
    // INCLUDE, INCLUDE, INCLUDE_AND_NEXT_COL.
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL, mpOnefilterList.filterKeyValue(null));
    // INCLUDE, SKIP, INCLUDE. 
    assertEquals(Filter.ReturnCode.INCLUDE, mpOnefilterList.filterKeyValue(null));

    // Check must pass all filter.
    FilterList mpAllfilterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter[] { includeFilter, alternateIncludeFilter, alternateFilter }));
    // INCLUDE, INCLUDE, INCLUDE_AND_NEXT_COL.
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL, mpAllfilterList.filterKeyValue(null));
    // INCLUDE, SKIP, INCLUDE. 
    assertEquals(Filter.ReturnCode.SKIP, mpAllfilterList.filterKeyValue(null));
  }

  /**
   * Test pass-thru of hints.
   */
  public void testHintPassThru() throws Exception {

    final KeyValue minKeyValue = new KeyValue(Bytes.toBytes(0L), null, null);
    final KeyValue maxKeyValue = new KeyValue(Bytes.toBytes(Long.MAX_VALUE),
        null, null);

    Filter filterNoHint = new FilterBase() {
      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter filterMinHint = new FilterBase() {
      @Override
      public ReturnCode filterKeyValue(KeyValue ignored) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return minKeyValue;
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter filterMaxHint = new FilterBase() {
      @Override
      public ReturnCode filterKeyValue(KeyValue ignored) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }

      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(Bytes.toBytes(Long.MAX_VALUE), null, null);
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    // MUST PASS ONE

    // Should take the min if given two hints
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    // Should have no hint if any filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(
            new Filter [] { filterMinHint, filterMaxHint, filterNoHint } ));
    assertNull(filterList.getNextKeyHint(null));
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    assertNull(filterList.getNextKeyHint(null));

    // Should give max hint if its the only one
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMaxHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // MUST PASS ALL

    // Should take the first hint
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    filterList.filterKeyValue(null);
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterMaxHint, filterMinHint } ));
    filterList.filterKeyValue(null);
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // Should have first hint even if a filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(
            new Filter [] { filterNoHint, filterMinHint, filterMaxHint } ));
    filterList.filterKeyValue(null);
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    filterList.filterKeyValue(null);
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));
    filterList.filterKeyValue(null);
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMinHint } ));
    filterList.filterKeyValue(null);
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));
  }

  /**
   * Tests the behavior of transform() in a hierarchical filter.
   *
   * transform() only applies after a filterKeyValue() whose return-code includes the KeyValue.
   * Lazy evaluation of AND
   */
  public void testTransformMPO() throws Exception {
    // Apply the following filter:
    //     (family=fam AND qualifier=qual1 AND KeyOnlyFilter)
    //  OR (family=fam AND qualifier=qual2)
    final FilterList flist = new FilterList(Operator.MUST_PASS_ONE, Lists.<Filter>newArrayList(
        new FilterList(Operator.MUST_PASS_ALL, Lists.<Filter>newArrayList(
            new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("fam"))),
            new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("qual1"))),
            new KeyOnlyFilter())),
        new FilterList(Operator.MUST_PASS_ALL, Lists.<Filter>newArrayList(
            new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("fam"))),
            new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("qual2")))))));

    final KeyValue kvQual1 = new KeyValue(
        Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("qual1"), Bytes.toBytes("value"));
    final KeyValue kvQual2 = new KeyValue(
        Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("qual2"), Bytes.toBytes("value"));
    final KeyValue kvQual3 = new KeyValue(
        Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("qual3"), Bytes.toBytes("value"));

    // Value for fam:qual1 should be stripped:
    assertEquals(Filter.ReturnCode.INCLUDE, flist.filterKeyValue(kvQual1));
    final KeyValue transformedQual1 = flist.transform(kvQual1);
    assertEquals(0, transformedQual1.getValue().length);

    // Value for fam:qual2 should not be stripped:
    assertEquals(Filter.ReturnCode.INCLUDE, flist.filterKeyValue(kvQual2));
    final KeyValue transformedQual2 = flist.transform(kvQual2);
    assertEquals("value", Bytes.toString(transformedQual2.getValue()));

    // Other keys should be skipped:
    assertEquals(Filter.ReturnCode.SKIP, flist.filterKeyValue(kvQual3));
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

