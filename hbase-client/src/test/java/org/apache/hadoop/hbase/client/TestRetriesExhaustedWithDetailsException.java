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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({SmallTests.class})
public class TestRetriesExhaustedWithDetailsException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRetriesExhaustedWithDetailsException.class);

  @Rule public TestName name = new TestName();

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
