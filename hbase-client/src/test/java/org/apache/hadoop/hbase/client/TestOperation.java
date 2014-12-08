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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.experimental.categories.Category;

/**
 * Run tests that use the functionality of the Operation superclass for
 * Puts, Gets, Deletes, Scans, and MultiPuts.
 */
@Category(SmallTests.class)
public class TestOperation {
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  private static ObjectMapper mapper = new ObjectMapper();

  private static List<Long> TS_LIST = Arrays.asList(2L, 3L, 5L);
  private static TimestampsFilter TS_FILTER = new TimestampsFilter(TS_LIST);
  private static String STR_TS_FILTER =
      TS_FILTER.getClass().getSimpleName() + " (3/3): [2, 3, 5]";

  private static List<Long> L_TS_LIST =
      Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
  private static TimestampsFilter L_TS_FILTER =
      new TimestampsFilter(L_TS_LIST);
  private static String STR_L_TS_FILTER =
      L_TS_FILTER.getClass().getSimpleName() + " (5/11): [0, 1, 2, 3, 4]";

  private static String COL_NAME_1 = "col1";
  private static ColumnPrefixFilter COL_PRE_FILTER =
      new ColumnPrefixFilter(COL_NAME_1.getBytes());
  private static String STR_COL_PRE_FILTER =
      COL_PRE_FILTER.getClass().getSimpleName() + " " + COL_NAME_1;

  private static String COL_NAME_2 = "col2";
  private static ColumnRangeFilter CR_FILTER = new ColumnRangeFilter(
      COL_NAME_1.getBytes(), true, COL_NAME_2.getBytes(), false);
  private static String STR_CR_FILTER = CR_FILTER.getClass().getSimpleName()
      + " [" + COL_NAME_1 + ", " + COL_NAME_2 + ")";

  private static int COL_COUNT = 9;
  private static ColumnCountGetFilter CCG_FILTER =
      new ColumnCountGetFilter(COL_COUNT);
  private static String STR_CCG_FILTER =
      CCG_FILTER.getClass().getSimpleName() + " " + COL_COUNT;

  private static int LIMIT = 3;
  private static int OFFSET = 4;
  private static ColumnPaginationFilter CP_FILTER =
      new ColumnPaginationFilter(LIMIT, OFFSET);
  private static String STR_CP_FILTER = CP_FILTER.getClass().getSimpleName()
      + " (" + LIMIT + ", " + OFFSET + ")";

  private static String STOP_ROW_KEY = "stop";
  private static InclusiveStopFilter IS_FILTER =
      new InclusiveStopFilter(STOP_ROW_KEY.getBytes());
  private static String STR_IS_FILTER =
      IS_FILTER.getClass().getSimpleName() + " " + STOP_ROW_KEY;

  private static String PREFIX = "prefix";
  private static PrefixFilter PREFIX_FILTER =
      new PrefixFilter(PREFIX.getBytes());
  private static String STR_PREFIX_FILTER = "PrefixFilter " + PREFIX;

  private static byte[][] PREFIXES = {
      "0".getBytes(), "1".getBytes(), "2".getBytes()};
  private static MultipleColumnPrefixFilter MCP_FILTER =
      new MultipleColumnPrefixFilter(PREFIXES);
  private static String STR_MCP_FILTER =
      MCP_FILTER.getClass().getSimpleName() + " (3/3): [0, 1, 2]";

  private static byte[][] L_PREFIXES = {
    "0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
    "4".getBytes(), "5".getBytes(), "6".getBytes(), "7".getBytes()};
  private static MultipleColumnPrefixFilter L_MCP_FILTER =
      new MultipleColumnPrefixFilter(L_PREFIXES);
  private static String STR_L_MCP_FILTER =
      L_MCP_FILTER.getClass().getSimpleName() + " (5/8): [0, 1, 2, 3, 4]";

  private static int PAGE_SIZE = 9;
  private static PageFilter PAGE_FILTER = new PageFilter(PAGE_SIZE);
  private static String STR_PAGE_FILTER =
      PAGE_FILTER.getClass().getSimpleName() + " " + PAGE_SIZE;

  private static SkipFilter SKIP_FILTER = new SkipFilter(L_TS_FILTER);
  private static String STR_SKIP_FILTER =
      SKIP_FILTER.getClass().getSimpleName() + " " + STR_L_TS_FILTER;

  private static WhileMatchFilter WHILE_FILTER =
      new WhileMatchFilter(L_TS_FILTER);
  private static String STR_WHILE_FILTER =
      WHILE_FILTER.getClass().getSimpleName() + " " + STR_L_TS_FILTER;

  private static KeyOnlyFilter KEY_ONLY_FILTER = new KeyOnlyFilter();
  private static String STR_KEY_ONLY_FILTER =
      KEY_ONLY_FILTER.getClass().getSimpleName();

  private static FirstKeyOnlyFilter FIRST_KEY_ONLY_FILTER =
      new FirstKeyOnlyFilter();
  private static String STR_FIRST_KEY_ONLY_FILTER =
      FIRST_KEY_ONLY_FILTER.getClass().getSimpleName();

  private static CompareOp CMP_OP = CompareOp.EQUAL;
  private static byte[] CMP_VALUE = "value".getBytes();
  private static BinaryComparator BC = new BinaryComparator(CMP_VALUE);
  private static DependentColumnFilter DC_FILTER =
      new DependentColumnFilter(FAMILY, QUALIFIER, true, CMP_OP, BC);
  private static String STR_DC_FILTER = String.format(
      "%s (%s, %s, %s, %s, %s)", DC_FILTER.getClass().getSimpleName(),
      Bytes.toStringBinary(FAMILY), Bytes.toStringBinary(QUALIFIER), true,
      CMP_OP.name(), Bytes.toStringBinary(BC.getValue()));

  private static FamilyFilter FAMILY_FILTER = new FamilyFilter(CMP_OP, BC);
  private static String STR_FAMILY_FILTER =
      FAMILY_FILTER.getClass().getSimpleName() + " (EQUAL, value)";

  private static QualifierFilter QUALIFIER_FILTER =
      new QualifierFilter(CMP_OP, BC);
  private static String STR_QUALIFIER_FILTER =
      QUALIFIER_FILTER.getClass().getSimpleName() + " (EQUAL, value)";

  private static RowFilter ROW_FILTER = new RowFilter(CMP_OP, BC);
  private static String STR_ROW_FILTER =
      ROW_FILTER.getClass().getSimpleName() + " (EQUAL, value)";

  private static ValueFilter VALUE_FILTER = new ValueFilter(CMP_OP, BC);
  private static String STR_VALUE_FILTER =
      VALUE_FILTER.getClass().getSimpleName() + " (EQUAL, value)";

  private static SingleColumnValueFilter SCV_FILTER =
      new SingleColumnValueFilter(FAMILY, QUALIFIER, CMP_OP, CMP_VALUE);
  private static String STR_SCV_FILTER = String.format("%s (%s, %s, %s, %s)",
      SCV_FILTER.getClass().getSimpleName(), Bytes.toStringBinary(FAMILY),
      Bytes.toStringBinary(QUALIFIER), CMP_OP.name(),
      Bytes.toStringBinary(CMP_VALUE));

  private static SingleColumnValueExcludeFilter SCVE_FILTER =
      new SingleColumnValueExcludeFilter(FAMILY, QUALIFIER, CMP_OP, CMP_VALUE);
  private static String STR_SCVE_FILTER = String.format("%s (%s, %s, %s, %s)",
      SCVE_FILTER.getClass().getSimpleName(), Bytes.toStringBinary(FAMILY),
      Bytes.toStringBinary(QUALIFIER), CMP_OP.name(),
      Bytes.toStringBinary(CMP_VALUE));

  private static FilterList AND_FILTER_LIST = new FilterList(
      Operator.MUST_PASS_ALL, Arrays.asList((Filter) TS_FILTER, L_TS_FILTER,
          CR_FILTER));
  private static String STR_AND_FILTER_LIST = String.format(
      "%s AND (3/3): [%s, %s, %s]", AND_FILTER_LIST.getClass().getSimpleName(),
      STR_TS_FILTER, STR_L_TS_FILTER, STR_CR_FILTER);

  private static FilterList OR_FILTER_LIST = new FilterList(
      Operator.MUST_PASS_ONE, Arrays.asList((Filter) TS_FILTER, L_TS_FILTER,
          CR_FILTER));
  private static String STR_OR_FILTER_LIST = String.format(
      "%s OR (3/3): [%s, %s, %s]", AND_FILTER_LIST.getClass().getSimpleName(),
      STR_TS_FILTER, STR_L_TS_FILTER, STR_CR_FILTER);

  private static FilterList L_FILTER_LIST = new FilterList(
      Arrays.asList((Filter) TS_FILTER, L_TS_FILTER, CR_FILTER, COL_PRE_FILTER,
          CCG_FILTER, CP_FILTER, PREFIX_FILTER, PAGE_FILTER));
  private static String STR_L_FILTER_LIST = String.format(
      "%s AND (5/8): [%s, %s, %s, %s, %s, %s]",
      L_FILTER_LIST.getClass().getSimpleName(), STR_TS_FILTER, STR_L_TS_FILTER,
      STR_CR_FILTER, STR_COL_PRE_FILTER, STR_CCG_FILTER, STR_CP_FILTER);

  private static Filter[] FILTERS = {
    TS_FILTER,             // TimestampsFilter
    L_TS_FILTER,           // TimestampsFilter
    COL_PRE_FILTER,        // ColumnPrefixFilter
    CP_FILTER,             // ColumnPaginationFilter
    CR_FILTER,             // ColumnRangeFilter
    CCG_FILTER,            // ColumnCountGetFilter
    IS_FILTER,             // InclusiveStopFilter
    PREFIX_FILTER,         // PrefixFilter
    PAGE_FILTER,           // PageFilter
    SKIP_FILTER,           // SkipFilter
    WHILE_FILTER,          // WhileMatchFilter
    KEY_ONLY_FILTER,       // KeyOnlyFilter
    FIRST_KEY_ONLY_FILTER, // FirstKeyOnlyFilter
    MCP_FILTER,            // MultipleColumnPrefixFilter
    L_MCP_FILTER,          // MultipleColumnPrefixFilter
    DC_FILTER,             // DependentColumnFilter
    FAMILY_FILTER,         // FamilyFilter
    QUALIFIER_FILTER,      // QualifierFilter
    ROW_FILTER,            // RowFilter
    VALUE_FILTER,          // ValueFilter
    SCV_FILTER,            // SingleColumnValueFilter
    SCVE_FILTER,           // SingleColumnValueExcludeFilter
    AND_FILTER_LIST,       // FilterList
    OR_FILTER_LIST,        // FilterList
    L_FILTER_LIST,         // FilterList
  };

  private static String[] FILTERS_INFO = {
    STR_TS_FILTER,             // TimestampsFilter
    STR_L_TS_FILTER,           // TimestampsFilter
    STR_COL_PRE_FILTER,        // ColumnPrefixFilter
    STR_CP_FILTER,             // ColumnPaginationFilter
    STR_CR_FILTER,             // ColumnRangeFilter
    STR_CCG_FILTER,            // ColumnCountGetFilter
    STR_IS_FILTER,             // InclusiveStopFilter
    STR_PREFIX_FILTER,         // PrefixFilter
    STR_PAGE_FILTER,           // PageFilter
    STR_SKIP_FILTER,           // SkipFilter
    STR_WHILE_FILTER,          // WhileMatchFilter
    STR_KEY_ONLY_FILTER,       // KeyOnlyFilter
    STR_FIRST_KEY_ONLY_FILTER, // FirstKeyOnlyFilter
    STR_MCP_FILTER,            // MultipleColumnPrefixFilter
    STR_L_MCP_FILTER,          // MultipleColumnPrefixFilter
    STR_DC_FILTER,             // DependentColumnFilter
    STR_FAMILY_FILTER,         // FamilyFilter
    STR_QUALIFIER_FILTER,      // QualifierFilter
    STR_ROW_FILTER,            // RowFilter
    STR_VALUE_FILTER,          // ValueFilter
    STR_SCV_FILTER,            // SingleColumnValueFilter
    STR_SCVE_FILTER,           // SingleColumnValueExcludeFilter
    STR_AND_FILTER_LIST,       // FilterList
    STR_OR_FILTER_LIST,        // FilterList
    STR_L_FILTER_LIST,         // FilterList
  };

  static {
    assertEquals("The sizes of static arrays do not match: "
        + "[FILTERS: %d <=> FILTERS_INFO: %d]",
        FILTERS.length, FILTERS_INFO.length);
  }

  /**
   * Test the client Operations' JSON encoding to ensure that produced JSON is
   * parseable and that the details are present and not corrupted.
   * @throws IOException
   */
  @Test
  public void testOperationJSON()
      throws IOException {
    // produce a Scan Operation
    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    String json = scan.toJSON();
    Map<String, Object> parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("startRow incorrect in Scan.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("startRow"));
    // check for the family and the qualifier.
    List familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Scan.toJSON()", familyInfo);
    assertEquals("Qualifier absent in Scan.toJSON()", 1, familyInfo.size());
    assertEquals("Qualifier incorrect in Scan.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        familyInfo.get(0));

    // produce a Get Operation
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    json = get.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row incorrect in Get.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Get.toJSON()", familyInfo);
    assertEquals("Qualifier absent in Get.toJSON()", 1, familyInfo.size());
    assertEquals("Qualifier incorrect in Get.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        familyInfo.get(0));

    // produce a Put operation
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    // get its JSON representation, and parse it
    json = put.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row absent in Put.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Put.toJSON()", familyInfo);
    assertEquals("KeyValue absent in Put.toJSON()", 1, familyInfo.size());
    Map kvMap = (Map) familyInfo.get(0);
    assertEquals("Qualifier incorrect in Put.toJSON()",
        Bytes.toStringBinary(QUALIFIER),
        kvMap.get("qualifier"));
    assertEquals("Value length incorrect in Put.toJSON()",
        VALUE.length, kvMap.get("vlen"));

    // produce a Delete operation
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER);
    // get its JSON representation, and parse it
    json = delete.toJSON();
    parsedJSON = mapper.readValue(json, HashMap.class);
    // check for the row
    assertEquals("row absent in Delete.toJSON()",
        Bytes.toStringBinary(ROW), parsedJSON.get("row"));
    // check for the family and the qualifier.
    familyInfo = (List) ((Map) parsedJSON.get("families")).get(
        Bytes.toStringBinary(FAMILY));
    assertNotNull("Family absent in Delete.toJSON()", familyInfo);
    assertEquals("KeyValue absent in Delete.toJSON()", 1, familyInfo.size());
    kvMap = (Map) familyInfo.get(0);
    assertEquals("Qualifier incorrect in Delete.toJSON()",
        Bytes.toStringBinary(QUALIFIER), kvMap.get("qualifier"));
  }

  @Test
  public void testPutCreationWithByteBuffer() {
    Put p = new Put(ROW);
    List<Cell> c = p.get(FAMILY, QUALIFIER);
    Assert.assertEquals(0, c.size());
    Assert.assertEquals(HConstants.LATEST_TIMESTAMP, p.getTimeStamp());

    p.add(FAMILY, ByteBuffer.wrap(QUALIFIER), 1984L, ByteBuffer.wrap(VALUE));
    c = p.get(FAMILY, QUALIFIER);
    Assert.assertEquals(1, c.size());
    Assert.assertEquals(1984L, c.get(0).getTimestamp());
    Assert.assertArrayEquals(VALUE, CellUtil.cloneValue(c.get(0)));
    Assert.assertEquals(HConstants.LATEST_TIMESTAMP, p.getTimeStamp());
    Assert.assertEquals(0, KeyValue.COMPARATOR.compare(c.get(0), new KeyValue(c.get(0))));

    p = new Put(ROW);
    p.add(FAMILY, ByteBuffer.wrap(QUALIFIER), 2013L, null);
    c = p.get(FAMILY, QUALIFIER);
    Assert.assertEquals(1, c.size());
    Assert.assertEquals(2013L, c.get(0).getTimestamp());
    Assert.assertArrayEquals(new byte[]{}, CellUtil.cloneValue(c.get(0)));
    Assert.assertEquals(HConstants.LATEST_TIMESTAMP, p.getTimeStamp());
    Assert.assertEquals(0, KeyValue.COMPARATOR.compare(c.get(0), new KeyValue(c.get(0))));

    p = new Put(ByteBuffer.wrap(ROW));
    p.add(FAMILY, ByteBuffer.wrap(QUALIFIER), 2001L, null);
    c = p.get(FAMILY, QUALIFIER);
    Assert.assertEquals(1, c.size());
    Assert.assertEquals(2001L, c.get(0).getTimestamp());
    Assert.assertArrayEquals(new byte[]{}, CellUtil.cloneValue(c.get(0)));
    Assert.assertArrayEquals(ROW, CellUtil.cloneRow(c.get(0)));
    Assert.assertEquals(HConstants.LATEST_TIMESTAMP, p.getTimeStamp());
    Assert.assertEquals(0, KeyValue.COMPARATOR.compare(c.get(0), new KeyValue(c.get(0))));

    p = new Put(ByteBuffer.wrap(ROW), 1970L);
    p.add(FAMILY, ByteBuffer.wrap(QUALIFIER), 2001L, null);
    c = p.get(FAMILY, QUALIFIER);
    Assert.assertEquals(1, c.size());
    Assert.assertEquals(2001L, c.get(0).getTimestamp());
    Assert.assertArrayEquals(new byte[]{}, CellUtil.cloneValue(c.get(0)));
    Assert.assertArrayEquals(ROW, CellUtil.cloneRow(c.get(0)));
    Assert.assertEquals(1970L, p.getTimeStamp());
    Assert.assertEquals(0, KeyValue.COMPARATOR.compare(c.get(0), new KeyValue(c.get(0))));
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testOperationSubClassMethodsAreBuilderStyle() {
    /* All Operation subclasses should have a builder style setup where setXXX/addXXX methods
     * can be chainable together:
     * . For example:
     * Scan scan = new Scan()
     *     .setFoo(foo)
     *     .setBar(bar)
     *     .setBuz(buz)
     *
     * This test ensures that all methods starting with "set" returns the declaring object
     */

    // TODO: We should ensure all subclasses of Operation is checked.
    Class[] classes = new Class[] {
        Operation.class,
        OperationWithAttributes.class,
        Mutation.class,
        Query.class,
        Delete.class,
        Increment.class,
        Append.class,
        Put.class,
        Get.class,
        Scan.class};

    BuilderStyleTest.assertClassesAreBuilderStyle(classes);
  }

}

