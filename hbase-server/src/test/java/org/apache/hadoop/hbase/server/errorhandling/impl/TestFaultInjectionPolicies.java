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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the fault injection policies and combinations
 */
@Category(SmallTests.class)
public class TestFaultInjectionPolicies {

  @Test
  public void testAndCombination() {
    FaultInjectionPolicy alwaysFalse = new FaultInjectionPolicy();
    assertFalse("Default policy isn't false", alwaysFalse.shouldFault(null));

    FaultInjectionPolicy alwaysTrue = new AlwaysTrue();
    FaultInjectionPolicy andTrue = new AlwaysTrue().or(alwaysTrue).or(alwaysTrue);
    assertTrue("And True isn't always returning true", andTrue.shouldFault(null));

    FaultInjectionPolicy andFalse = new FaultInjectionPolicy().and(alwaysTrue);
    assertFalse("false AND true", andFalse.shouldFault(null));
    assertFalse("true AND false", alwaysTrue.and(alwaysFalse).shouldFault(null));
    assertFalse("true AND (false AND true)",
      new AlwaysTrue().and(new FaultInjectionPolicy().and(new AlwaysTrue())).shouldFault(null));
    assertFalse("(true AND false AND true)",
      new AlwaysTrue().and(new FaultInjectionPolicy()).and(new AlwaysTrue()).shouldFault(null));
  }

  @Test
  public void testORCombination() {
    FaultInjectionPolicy alwaysTrue = new AlwaysTrue();

    FaultInjectionPolicy andTrue = new AlwaysTrue().or(alwaysTrue).or(alwaysTrue);
    assertTrue("OR True isn't always returning true", andTrue.shouldFault(null));

    FaultInjectionPolicy andFalse = new FaultInjectionPolicy().or(alwaysTrue);
    assertTrue("Combination of true OR false should be true", andFalse.shouldFault(null));
    assertTrue("Combining multiple ands isn't correct",
      new FaultInjectionPolicy().or(andTrue).or(andFalse).shouldFault(null));
  }

  @Test
  public void testMixedAndOr() {
    assertTrue("true AND (false OR true)",
      new AlwaysTrue().and(new FaultInjectionPolicy().or(new AlwaysTrue())).shouldFault(null));
    assertTrue("(true AND false) OR true",
      new AlwaysTrue().or(new AlwaysTrue().and(new FaultInjectionPolicy())).shouldFault(null));
    assertFalse(
      "(true AND false) OR false",
      new FaultInjectionPolicy().or(new AlwaysTrue().and(new FaultInjectionPolicy())).shouldFault(
        null));
  }

  private static class AlwaysTrue extends FaultInjectionPolicy {

    protected boolean checkForFault(StackTraceElement[] stack) {
      return true;
    }
  }

  public static class SimplePolicyFaultInjector extends PoliciedFaultInjector<Exception> {

    public SimplePolicyFaultInjector(FaultInjectionPolicy policy) {
      super(policy);
    }

    @Override
    protected Pair<Exception, Object[]> getInjectedError(StackTraceElement[] trace) {
      return new Pair<Exception, Object[]>(new RuntimeException("error"), null);
    }
  }
}