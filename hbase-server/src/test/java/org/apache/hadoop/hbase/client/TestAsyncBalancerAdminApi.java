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

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncBalancerAdminApi extends TestAsyncAdminBase {

  @Test
  public void testBalancer() throws Exception {
    boolean initialState = admin.isBalancerEnabled().get();

    // Start the balancer, wait for it.
    boolean prevState = admin.setBalancerRunning(!initialState).get();

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, admin.isBalancerEnabled().get());

    // Reset it back to what it was
    prevState = admin.setBalancerRunning(initialState).get();

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);
    // Current state should be the original state again
    assertEquals(initialState, admin.isBalancerEnabled().get());
  }
}
