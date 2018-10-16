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

import com.codahale.metrics.Meter;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test class for {@link DropwizardMeter}.
 */
@Category(SmallTests.class)
public class TestDropwizardMeter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDropwizardMeter.class);

  private Meter meter;

  @Before public void setup() {
    this.meter = Mockito.mock(Meter.class);
  }

  @Test public void test() {
    DropwizardMeter dwMeter = new DropwizardMeter(this.meter);

    dwMeter.mark();
    dwMeter.mark(10L);
    dwMeter.mark();
    dwMeter.mark();

    Mockito.verify(meter, Mockito.times(3)).mark();
    Mockito.verify(meter).mark(10L);
  }
}
