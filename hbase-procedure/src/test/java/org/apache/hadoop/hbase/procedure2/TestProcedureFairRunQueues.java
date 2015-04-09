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

package org.apache.hadoop.hbase.procedure2;

import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestProcedureFairRunQueues {
  private static class TestRunQueue implements ProcedureFairRunQueues.FairObject {
    private final int priority;
    private final String name;

    private boolean available = true;

    public TestRunQueue(String name, int priority) {
      this.name = name;
      this.priority = priority;
    }

    @Override
    public String toString() {
      return name;
    }

    private void setAvailable(boolean available) {
      this.available = available;
    }

    @Override
    public boolean isAvailable() {
      return available;
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

  @Test
  public void testEmptyFairQueues() throws Exception {
    ProcedureFairRunQueues<String, TestRunQueue> fairq
      = new ProcedureFairRunQueues<String, TestRunQueue>(1);
    for (int i = 0; i < 3; ++i) {
      assertEquals(null, fairq.poll());
    }
  }

  @Test
  public void testFairQueues() throws Exception {
    ProcedureFairRunQueues<String, TestRunQueue> fairq
      = new ProcedureFairRunQueues<String, TestRunQueue>(1);
    TestRunQueue a = fairq.add("A", new TestRunQueue("A", 1));
    TestRunQueue b = fairq.add("B", new TestRunQueue("B", 1));
    TestRunQueue m = fairq.add("M", new TestRunQueue("M", 2));

    for (int i = 0; i < 3; ++i) {
      assertEquals(a, fairq.poll());
      assertEquals(b, fairq.poll());
      assertEquals(m, fairq.poll());
      assertEquals(m, fairq.poll());
    }
  }

  @Test
  public void testFairQueuesNotAvailable() throws Exception {
    ProcedureFairRunQueues<String, TestRunQueue> fairq
      = new ProcedureFairRunQueues<String, TestRunQueue>(1);
    TestRunQueue a = fairq.add("A", new TestRunQueue("A", 1));
    TestRunQueue b = fairq.add("B", new TestRunQueue("B", 1));
    TestRunQueue m = fairq.add("M", new TestRunQueue("M", 2));

    // m is not available
    m.setAvailable(false);
    for (int i = 0; i < 3; ++i) {
      assertEquals(a, fairq.poll());
      assertEquals(b, fairq.poll());
    }

    // m is available
    m.setAvailable(true);
    for (int i = 0; i < 3; ++i) {
      assertEquals(m, fairq.poll());
      assertEquals(m, fairq.poll());
      assertEquals(a, fairq.poll());
      assertEquals(b, fairq.poll());
    }

    // b is not available
    b.setAvailable(false);
    for (int i = 0; i < 3; ++i) {
      assertEquals(m, fairq.poll());
      assertEquals(m, fairq.poll());
      assertEquals(a, fairq.poll());
    }

    assertEquals(m, fairq.poll());
    m.setAvailable(false);
    // m should be fetched next, but is no longer available
    assertEquals(a, fairq.poll());
    assertEquals(a, fairq.poll());
    b.setAvailable(true);
    for (int i = 0; i < 3; ++i) {
      assertEquals(b, fairq.poll());
      assertEquals(a, fairq.poll());
    }
  }

  @Test
  public void testFairQueuesDelete() throws Exception {
    ProcedureFairRunQueues<String, TestRunQueue> fairq
      = new ProcedureFairRunQueues<String, TestRunQueue>(1);
    TestRunQueue a = fairq.add("A", new TestRunQueue("A", 1));
    TestRunQueue b = fairq.add("B", new TestRunQueue("B", 1));
    TestRunQueue m = fairq.add("M", new TestRunQueue("M", 2));

    // Fetch A and then remove it
    assertEquals(a, fairq.poll());
    assertEquals(a, fairq.remove("A"));

    // Fetch B and then remove it
    assertEquals(b, fairq.poll());
    assertEquals(b, fairq.remove("B"));

    // Fetch M and then remove it
    assertEquals(m, fairq.poll());
    assertEquals(m, fairq.remove("M"));

    // nothing left
    assertEquals(null, fairq.poll());
  }
}
