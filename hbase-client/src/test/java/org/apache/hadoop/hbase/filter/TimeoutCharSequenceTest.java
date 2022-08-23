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
package org.apache.hadoop.hbase.filter;

import static org.apache.hadoop.hbase.filter.TimeoutCharSequence.DEFAULT_CHECK_POINT;

import org.apache.hadoop.hbase.DoNotRetryUncheckedIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TimeoutCharSequenceTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TimeoutCharSequenceTest.class);

  @Test(expected = DoNotRetryUncheckedIOException.class)
  public void testCharAtTimeout() {
    final IncrementingEnvironmentEdge environmentEdge = new IncrementingEnvironmentEdge(7);
    EnvironmentEdgeManager.injectEdge(environmentEdge);

    final TimeoutCharSequence chars = new TimeoutCharSequence("test", 0, 8, 1);
    for (int i = 0; i < 3; i++) {
      chars.charAt(3);
    }
  }

  @Test
  public void testCheckPoint() {
    final long initialAmount = 10;
    final IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge(initialAmount);
    EnvironmentEdgeManager.injectEdge(edge);

    final TimeoutCharSequence chars = new TimeoutCharSequence("test", 0, 8);

    boolean thrownException = false;
    for (int i = 0; i < DEFAULT_CHECK_POINT; i++) {
      try {
        chars.charAt(3);
      } catch (DoNotRetryUncheckedIOException e) {
        thrownException = true;
      }
    }

    Assert.assertTrue(thrownException);
    Assert.assertEquals(edge.currentTime(), initialAmount + 1);
  }

  @Test
  public void testCharAt() {
    final IncrementingEnvironmentEdge environmentEdge = new IncrementingEnvironmentEdge(1);
    EnvironmentEdgeManager.injectEdge(environmentEdge); // next currentTime: 1

    final TimeoutCharSequence chars = new TimeoutCharSequence("test", 0, 10);
    Assert.assertEquals('t', chars.charAt(0));
    Assert.assertEquals('e', chars.charAt(1));
  }
}
