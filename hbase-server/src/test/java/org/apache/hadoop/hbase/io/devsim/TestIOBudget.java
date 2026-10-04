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
package org.apache.hadoop.hbase.io.devsim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestIOBudget {

  @Test
  public void testLowRateModeDoesNotDeadlock() {
    IOBudget budget = new IOBudget(2, 100);
    long t0 = System.currentTimeMillis();
    budget.consume(1);
    long t1 = System.currentTimeMillis();
    budget.consume(1);
    long elapsedSecondTokenMs = System.currentTimeMillis() - t1;

    // At 2 tokens/sec, second token should require waiting roughly 500ms.
    assertTrue(elapsedSecondTokenMs >= 300,
      "Expected low-rate budget to throttle second token, elapsed=" + elapsedSecondTokenMs);
    assertTrue(elapsedSecondTokenMs < 3000,
      "Unexpectedly long low-rate throttle delay, elapsed=" + elapsedSecondTokenMs);
    assertTrue(t1 >= t0, "Clock sanity check");
  }

  @Test
  public void testRegularWindowModeStillThrottles() {
    // 100 tokens/sec with 100ms windows -> 10 tokens/window
    IOBudget budget = new IOBudget(100, 100);
    long elapsedMs = budget.consume(15);
    assertTrue(elapsedMs > 0, "Expected consume to sleep when exceeding window budget");
  }
}
