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
package org.apache.hadoop.hbase.replication.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestLogCleanerBarrier {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogCleanerBarrier.class);

  @Test
  public void test() {
    ReplicationLogCleanerBarrier barrier = new ReplicationLogCleanerBarrier();
    assertThrows(IllegalStateException.class, () -> barrier.stop());
    assertThrows(IllegalStateException.class, () -> barrier.enable());
    assertTrue(barrier.start());
    assertThrows(IllegalStateException.class, () -> barrier.start());
    assertThrows(IllegalStateException.class, () -> barrier.enable());
    assertFalse(barrier.disable());
    assertThrows(IllegalStateException.class, () -> barrier.enable());
    barrier.stop();

    for (int i = 0; i < 3; i++) {
      assertTrue(barrier.disable());
      assertFalse(barrier.start());
    }
    for (int i = 0; i < 3; i++) {
      assertFalse(barrier.start());
      barrier.enable();
    }
    assertTrue(barrier.start());
  }
}
