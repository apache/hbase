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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestEnvironmentEdgeManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEnvironmentEdgeManager.class);

  @Test
  public void testManageSingleton() {
    EnvironmentEdgeManager.reset();
    EnvironmentEdge edge = EnvironmentEdgeManager.getDelegate();
    assertNotNull(edge);
    assertTrue(edge instanceof DefaultEnvironmentEdge);
    EnvironmentEdgeManager.reset();
    EnvironmentEdge edge2 = EnvironmentEdgeManager.getDelegate();
    assertFalse(edge == edge2);
    IncrementingEnvironmentEdge newEdge = new IncrementingEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(newEdge);
    assertEquals(newEdge, EnvironmentEdgeManager.getDelegate());

    // injecting null will result in default being assigned.
    EnvironmentEdgeManager.injectEdge(null);
    EnvironmentEdge nullResult = EnvironmentEdgeManager.getDelegate();
    assertTrue(nullResult instanceof DefaultEnvironmentEdge);
  }

  @Test
  public void testCurrentTimeInMillis() {
    EnvironmentEdge mock = mock(EnvironmentEdge.class);
    EnvironmentEdgeManager.injectEdge(mock);
    long expectation = 3456;
    when(mock.currentTime()).thenReturn(expectation);
    long result = EnvironmentEdgeManager.currentTime();
    verify(mock).currentTime();
    assertEquals(expectation, result);
  }
}
