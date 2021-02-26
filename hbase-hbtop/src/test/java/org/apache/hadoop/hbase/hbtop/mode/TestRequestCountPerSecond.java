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
package org.apache.hadoop.hbase.hbtop.mode;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class TestRequestCountPerSecond {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestCountPerSecond.class);

  @Test
  public void test() {
    RequestCountPerSecond requestCountPerSecond = new RequestCountPerSecond();

    requestCountPerSecond.refresh(1000, 300, 100, 200);
    assertThat(requestCountPerSecond.getRequestCountPerSecond(), is(0L));
    assertThat(requestCountPerSecond.getReadRequestCountPerSecond(), is(0L));
    assertThat(requestCountPerSecond.getWriteRequestCountPerSecond(), is(0L));
    assertThat(requestCountPerSecond.getFilteredReadRequestCountPerSecond(), is(0L));

    requestCountPerSecond.refresh(2000, 1300, 1100, 1200);
    assertThat(requestCountPerSecond.getRequestCountPerSecond(), is(2000L));
    assertThat(requestCountPerSecond.getReadRequestCountPerSecond(), is(1000L));
    assertThat(requestCountPerSecond.getFilteredReadRequestCountPerSecond(), is(1000L));
    assertThat(requestCountPerSecond.getWriteRequestCountPerSecond(), is(1000L));

    requestCountPerSecond.refresh(12000, 5300, 3100, 2200);
    assertThat(requestCountPerSecond.getRequestCountPerSecond(), is(500L));
    assertThat(requestCountPerSecond.getReadRequestCountPerSecond(), is(400L));
    assertThat(requestCountPerSecond.getFilteredReadRequestCountPerSecond(), is(200L));
    assertThat(requestCountPerSecond.getWriteRequestCountPerSecond(), is(100L));
  }
}
