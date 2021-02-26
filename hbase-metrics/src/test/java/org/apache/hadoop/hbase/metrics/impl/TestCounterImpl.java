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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link CounterImpl}.
 */
@Category(SmallTests.class)
public class TestCounterImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCounterImpl.class);

  private Counter counter;

  @Before public void setup() {
    this.counter = new CounterImpl();
  }

  @Test public void testCounting() {
    counter.increment();
    assertEquals(1L, counter.getCount());
    counter.increment();
    assertEquals(2L, counter.getCount());
    counter.increment(2L);
    assertEquals(4L, counter.getCount());
    counter.increment(-1L);
    assertEquals(3L, counter.getCount());

    counter.decrement();
    assertEquals(2L, counter.getCount());
    counter.decrement();
    assertEquals(1L, counter.getCount());
    counter.decrement(4L);
    assertEquals(-3L, counter.getCount());
    counter.decrement(-3L);
    assertEquals(0L, counter.getCount());
  }
}
