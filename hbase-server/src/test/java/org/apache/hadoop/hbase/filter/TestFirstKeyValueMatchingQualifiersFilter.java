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

import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestFirstKeyValueMatchingQualifiersFilter {

  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
  private static final byte[] COLUMN_QUALIFIER_1 = Bytes.toBytes("foo");
  private static final byte[] COLUMN_QUALIFIER_2 = Bytes.toBytes("foo_2");
  private static final byte[] COLUMN_QUALIFIER_3 = Bytes.toBytes("foo_3");
  private static final byte[] VAL_1 = Bytes.toBytes("a");

  /**
   * Test the functionality of
   * {@link FirstKeyValueMatchingQualifiersFilter#filterCell(org.apache.hadoop.hbase.Cell)}
   */
  @Test
  public void testFirstKeyMatchingQualifierFilter() throws Exception {
    Set<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    quals.add(COLUMN_QUALIFIER_1);
    quals.add(COLUMN_QUALIFIER_2);
    Filter filter = new FirstKeyValueMatchingQualifiersFilter(quals);

    // Match in first attempt
    KeyValue cell;
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_1, VAL_1);
    assertTrue(filter.filterCell(cell) == Filter.ReturnCode.INCLUDE, "includeAndSetFlag");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue(filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW, "flagIsSetSkipToNextRow");

    // A mismatch in first attempt and match in second attempt.
    filter.reset();
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_3, VAL_1);
    System.out.println(filter.filterCell(cell));
    assertTrue(filter.filterCell(cell) == Filter.ReturnCode.INCLUDE, "includeFlagIsUnset");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue(filter.filterCell(cell) == Filter.ReturnCode.INCLUDE, "includeAndSetFlag");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_1, VAL_1);
    assertTrue(filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW, "flagIsSetSkipToNextRow");
  }

}
