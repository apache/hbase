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
package org.apache.hadoop.hbase.util;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests to make sure that the default environment edge conforms to appropriate behaviour.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestDefaultEnvironmentEdge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultEnvironmentEdge.class);

  @Test
  public void testGetCurrentTimeUsesSystemClock() {
    DefaultEnvironmentEdge edge = new DefaultEnvironmentEdge();
    long systemTime = System.currentTimeMillis();
    long edgeTime = edge.currentTime();
    assertTrue("System time must be either the same or less than the edge time",
      systemTime < edgeTime || systemTime == edgeTime);
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
    long secondEdgeTime = edge.currentTime();
    assertTrue("Second time must be greater than the first", secondEdgeTime > edgeTime);
  }
}
