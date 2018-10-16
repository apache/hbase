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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Test for the ColumnPaginationFilter, used mainly to test the successful serialization of the filter.
 * More test functionality can be found within {@link org.apache.hadoop.hbase.filter.TestFilter#testColumnPaginationFilter()}
 */
@Category({FilterTests.class, SmallTests.class})
public class TestColumnPaginationFilter
{

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestColumnPaginationFilter.class);

    private static final byte[] ROW = Bytes.toBytes("row_1_test");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
    private static final byte[] VAL_1 = Bytes.toBytes("a");
    private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("foo");

    private Filter columnPaginationFilterOffset;
    private Filter columnPaginationFilter;

    @Before
    public void setUp() throws Exception {
        columnPaginationFilter = getColumnPaginationFilter();
        columnPaginationFilterOffset = getColumnPaginationFilterOffset();
    }

    private Filter getColumnPaginationFilter() {
        return new ColumnPaginationFilter(1, 0);
    }

    private Filter getColumnPaginationFilterOffset() {
        return new ColumnPaginationFilter(1, COLUMN_QUALIFIER);
    }

    private Filter serializationTest(Filter filter) throws Exception {
      FilterProtos.Filter filterProto = ProtobufUtil.toFilter(filter);
      Filter newFilter = ProtobufUtil.toFilter(filterProto);

      return newFilter;
    }


    /**
     * The more specific functionality tests are contained within the TestFilters class.  This class is mainly for testing
     * serialization
     *
     * @param filter
     * @throws Exception
     */
    private void basicFilterTests(ColumnPaginationFilter filter) throws Exception
    {
      KeyValue c = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
      assertTrue("basicFilter1", filter.filterCell(c) == Filter.ReturnCode.INCLUDE_AND_NEXT_COL);
    }

    /**
     * Tests serialization
     * @throws Exception
     */
    @Test
    public void testSerialization() throws Exception {
      Filter newFilter = serializationTest(columnPaginationFilter);
      basicFilterTests((ColumnPaginationFilter)newFilter);

      Filter newFilterOffset = serializationTest(columnPaginationFilterOffset);
      basicFilterTests((ColumnPaginationFilter)newFilterOffset);
    }



}

