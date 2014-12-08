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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
/**
 * Tests the inclusive stop row filter
 */
@Category(SmallTests.class)
public class TestInclusiveStopFilter {
  private final byte [] STOP_ROW = Bytes.toBytes("stop_row");
  private final byte [] GOOD_ROW = Bytes.toBytes("good_row");
  private final byte [] PAST_STOP_ROW = Bytes.toBytes("zzzzzz");

  Filter mainFilter;

  @Before
  public void setUp() throws Exception {
    mainFilter = new InclusiveStopFilter(STOP_ROW);
  }

  /**
   * Tests identification of the stop row
   * @throws Exception
   */
  @Test
  public void testStopRowIdentification() throws Exception {
    stopRowTests(mainFilter);
  }

  /**
   * Tests serialization
   * @throws Exception
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
    assertFalse("Filtering on " + Bytes.toString(GOOD_ROW),
      filter.filterRowKey(GOOD_ROW, 0, GOOD_ROW.length));
    assertFalse("Filtering on " + Bytes.toString(STOP_ROW),
      filter.filterRowKey(STOP_ROW, 0, STOP_ROW.length));
    assertTrue("Filtering on " + Bytes.toString(PAST_STOP_ROW),
      filter.filterRowKey(PAST_STOP_ROW, 0, PAST_STOP_ROW.length));

    assertTrue("FilterAllRemaining", filter.filterAllRemaining());
    assertFalse("FilterNotNull", filter.filterRow());

    assertFalse("Filter a null", filter.filterRowKey(null, 0, 0));
  }

}

