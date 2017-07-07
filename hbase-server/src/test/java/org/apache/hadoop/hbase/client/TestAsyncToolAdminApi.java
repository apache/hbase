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

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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

  @Test
  public void testBalancer() throws Exception {
    boolean initialState = admin.isBalancerOn().get();

    // Start the balancer, wait for it.
    boolean prevState = admin.setBalancerOn(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isBalancerOn().get());

    // Reset it back to what it was
    prevState = admin.setBalancerOn(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isBalancerOn().get());
  }

  @Test
  public void testNormalizer() throws Exception {
    boolean initialState = admin.isNormalizerOn().get();

    // flip state
    boolean prevState = admin.setNormalizerOn(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isNormalizerOn().get());

    // Reset it back to what it was
    prevState = admin.setNormalizerOn(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isNormalizerOn().get());
  }

  @Test
  public void testCleanerChore() throws Exception {
    boolean initialState = admin.isCleanerChoreOn().get();

    // flip state
    boolean prevState = admin.setCleanerChoreOn(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isCleanerChoreOn().get());

    // Reset it back to what it was
    prevState = admin.setCleanerChoreOn(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isCleanerChoreOn().get());
  }

  @Test
  public void testCatalogJanitor() throws Exception {
    boolean initialState = admin.isCatalogJanitorOn().get();

    // flip state
    boolean prevState = admin.setCatalogJanitorOn(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isCatalogJanitorOn().get());

    // Reset it back to what it was
    prevState = admin.setCatalogJanitorOn(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);

    // Current state should be the original state again
    assertEquals(initialState, admin.isCatalogJanitorOn().get());
  }
}
