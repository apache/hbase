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
package org.apache.hadoop.hbase.client;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@Category({SmallTests.class})
public class TestRetriesExhaustedWithDetailsException {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRetriesExhaustedWithDetailsException.class);
  @Rule public TestName name = new TestName();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).
      withLookingForStuckThread(true).
      build();

  /**
   * Assert that a RetriesExhaustedException that has RegionTooBusyException outputs region name.
   */
  @Test
  public void testRegionTooBusyException() {
    List<Throwable> ts = new ArrayList<>(1);
    final String regionName = this.name.getMethodName();
    ts.add(new RegionTooBusyException(regionName));
    List<Row> rows = new ArrayList<>(1);
    rows.add(Mockito.mock(Row.class));
    List<String> hostAndPorts = new ArrayList<>(1);
    hostAndPorts.add("example.com:1234");
    RetriesExhaustedException ree =
        new RetriesExhaustedWithDetailsException(ts, rows, hostAndPorts);
    assertTrue(ree.toString().contains(regionName));
  }
}
