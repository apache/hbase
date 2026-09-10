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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

/**
 * Test the admin operations for Balancer, Normalizer, CleanerChore, and CatalogJanitor.
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: policy = {0}")
public class TestAsyncToolAdminApi extends TestAsyncAdminBase {

  public TestAsyncToolAdminApi(Supplier<AsyncAdmin> admin) {
    super(admin);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TestAsyncAdminBase.setUpBeforeClass();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TestAsyncAdminBase.tearDownAfterClass();
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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
