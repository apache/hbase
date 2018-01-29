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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link Gauge}.
 */
@Category(SmallTests.class)
public class TestGauge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGauge.class);

  @Test
  public void testGetValue() {
    SimpleGauge gauge = new SimpleGauge();

    assertEquals(0, (long)gauge.getValue());

    gauge.setValue(1000L);

    assertEquals(1000L, (long)gauge.getValue());
  }

  /**
   * Gauge implementation with a setter.
   */
  private static class SimpleGauge implements Gauge<Long> {

    private final AtomicLong value = new AtomicLong(0L);

    @Override public Long getValue() {
      return this.value.get();
    }

    public void setValue(long value) {
      this.value.set(value);
    }
  }
}
