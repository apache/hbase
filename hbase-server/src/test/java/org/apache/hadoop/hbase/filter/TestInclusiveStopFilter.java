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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests the inclusive stop row filter
 */
@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestInclusiveStopFilter {

  private final byte[] STOP_ROW = Bytes.toBytes("stop_row");
  private final byte[] GOOD_ROW = Bytes.toBytes("good_row");
  private final byte[] PAST_STOP_ROW = Bytes.toBytes("zzzzzz");

  Filter mainFilter;

  @BeforeEach
  public void setUp() throws Exception {
    mainFilter = new InclusiveStopFilter(STOP_ROW);
  }

  /**
   * Tests identification of the stop row
   */
  @Test
  public void testStopRowIdentification() throws Exception {
    stopRowTests(mainFilter);
  }

  /**
   * Tests serialization
   */
  @Test
  public void testSerialization() throws Exception {
    // Decompose mainFilter to bytes.
    byte[] buffer = mainFilter.toByteArray();

    // Recompose mainFilter.
    Filter newFilter = InclusiveStopFilter.parseFrom(buffer);

    // Ensure the serialization preserved the filter by running a full test.
    stopRowTests(newFilter);
  }

  private void stopRowTests(Filter filter) throws Exception {
    assertFalse(filter.filterRowKey(KeyValueUtil.createFirstOnRow(GOOD_ROW)),
      "Filtering on " + Bytes.toString(GOOD_ROW));
    assertFalse(filter.filterRowKey(KeyValueUtil.createFirstOnRow(STOP_ROW)),
      "Filtering on " + Bytes.toString(STOP_ROW));
    assertTrue(filter.filterRowKey(KeyValueUtil.createFirstOnRow(PAST_STOP_ROW)),
      "Filtering on " + Bytes.toString(PAST_STOP_ROW));

    assertTrue(filter.filterAllRemaining(), "FilterAllRemaining");
    assertFalse(filter.filterRow(), "FilterNotNull");
  }

}
