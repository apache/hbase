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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the admin operations for Balancer, Normalizer, CleanerChore, and CatalogJanitor.
 */
@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncToolAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncToolAdminApi.class);

  @Test
  public void testBalancer() throws Exception {
    boolean initialState = admin.isBalancerEnabled().get();

    // Start the balancer, wait for it.
    boolean prevState = admin.balancerSwitch(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isBalancerEnabled().get());

    // Reset it back to what it was
    prevState = admin.balancerSwitch(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isBalancerEnabled().get());
  }

  @Test
  public void testNormalizer() throws Exception {
    boolean initialState = admin.isNormalizerEnabled().get();

    // flip state
    boolean prevState = admin.normalizerSwitch(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isNormalizerEnabled().get());

    // Reset it back to what it was
    prevState = admin.normalizerSwitch(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isNormalizerEnabled().get());
  }

  @Test
  public void testCleanerChore() throws Exception {
    boolean initialState = admin.isCleanerChoreEnabled().get();

    // flip state
    boolean prevState = admin.cleanerChoreSwitch(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isCleanerChoreEnabled().get());

    // Reset it back to what it was
    prevState = admin.cleanerChoreSwitch(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isCleanerChoreEnabled().get());
  }

  @Test
  public void testCatalogJanitor() throws Exception {
    boolean initialState = admin.isCatalogJanitorEnabled().get();

    // flip state
    boolean prevState = admin.catalogJanitorSwitch(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isCatalogJanitorEnabled().get());

    // Reset it back to what it was
    prevState = admin.catalogJanitorSwitch(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isCatalogJanitorEnabled().get());
  }
}
