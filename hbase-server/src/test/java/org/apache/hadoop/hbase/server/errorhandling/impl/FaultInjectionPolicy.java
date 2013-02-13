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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;

/**
 * Decoratable policy for if a fault should be injected for a given stack trace.
 * <p>
 * Passed in policies are combined with the current policy via a {@link PolicyCombination}.
 * <p>
 * Using {@link PolicyCombination#AND} means that <i>all</i> policies must agree to inject a fault
 * (including the current implemented) before a fault is injected.
 * <p>
 * Using {@link PolicyCombination#OR} means that if <i>any</i> of the policies may assert that a
 * fault be injected, in which case all remaining injectors will be ignored.
 * <p>
 * Order of operations occurs in reverse order of the operations added via
 * {@link #and(FaultInjectionPolicy)} or {@link #or(FaultInjectionPolicy)}. For example, if this is
 * the default policy 'a', which we {@link #and(FaultInjectionPolicy)} with 'b' and then 'c', we get
 * the following policy chain:
 * <p>
 * a && (b && c)
 * <p>
 * Similarly, if this is the default policy 'a', which we {@link #or(FaultInjectionPolicy)} with 'b'
 * and then 'c', we get the following policy chain:
 * <p>
 * a || (b || c).
 * <p>
 * Naturally, more complex policies can then be built using this style. Suppose we have policy A,
 * which is actually the 'and' of two policies, a and b:
 * <p>
 * A = a && b
 * <p>
 * and similarly we also have B which is an 'or' of c and d:
 * <p>
 * B = c || d
 * <p>
 * then we could combine the two by calling A {@link #and(FaultInjectionPolicy)} B, to get:
 * <p>
 * A && B = (a && b) && (c || d)
 */
public class FaultInjectionPolicy {

  public enum PolicyCombination {
    AND, OR;
    /**
     * Apply the combination to the policy outputs
     * @param current current policy value
     * @param next next policy to value to consider
     * @return <tt>true</tt> if the logical combination is valid, <tt>false</tt> otherwise
     */
    public boolean apply(boolean current, boolean next) {
      switch (this) {
      case AND:
        return current && next;
      case OR:
        return current || next;
      default:
        throw new IllegalArgumentException("Unrecognized policy!" + this);
      }
    }
  }

  private List<Pair<PolicyCombination, FaultInjectionPolicy>> policies = new ArrayList<Pair<PolicyCombination, FaultInjectionPolicy>>();

  /**
   * And the current chain with another policy.
   * <p>
   * For example, if this is the default policy 'a', which we {@link #and(FaultInjectionPolicy)}
   * with 'b' and then 'c', we get the following policy chain:
   * <p>
   * a && (b && c)
   * @param policy policy to logical AND with the current policies
   * @return <tt>this</tt> for chaining
   */
  public FaultInjectionPolicy and(FaultInjectionPolicy policy) {
    return addPolicy(PolicyCombination.AND, policy);
  }

  /**
   * And the current chain with another policy.
   * <p>
   * For example, if this is the default policy 'a', which we {@link #or(FaultInjectionPolicy)} with
   * 'b' and then 'c', we get the following policy chain:
   * <p>
   * a || (b || c)
   * @param policy policy to logical OR with the current policies
   * @return <tt>this</tt> for chaining
   */
  public FaultInjectionPolicy or(FaultInjectionPolicy policy) {
    return addPolicy(PolicyCombination.OR, policy);
  }

  private FaultInjectionPolicy addPolicy(PolicyCombination combinator, FaultInjectionPolicy policy) {
    policies.add(new Pair<PolicyCombination, FaultInjectionPolicy>(combinator, policy));
    return this;
  }

  /**
   * Check to see if this, or any of the policies this decorates, find that a fault should be
   * injected .
   * @param stack
   * @return <tt>true</tt> if a fault should be injected, <tt>false</tt> otherwise.
   */
  public final boolean shouldFault(StackTraceElement[] stack) {
    boolean current = checkForFault(stack);
    return eval(current, policies, stack);
  }

  /**
   * @param current
   * @param policies2
   * @param stack
   * @return
   */
  private boolean eval(boolean current,
      List<Pair<PolicyCombination, FaultInjectionPolicy>> policies, StackTraceElement[] stack) {
    // base condition: if there are no more to evaluate, the comparison is the last
    if (policies.size() == 0) return current;

    // otherwise we have to evaluate the rest of chain
    Pair<PolicyCombination, FaultInjectionPolicy> policy = policies.get(0);
    boolean next = policy.getSecond().shouldFault(stack);
    return policy.getFirst()
        .apply(current, eval(next, policies.subList(1, policies.size()), stack));
  }

  /**
   * Check to see if we should generate a fault for the given stacktrace.
   * <p>
   * Subclass hook for providing custom fault checking behavior
   * @param stack
   * @return if a fault should be injected for this error check request. <tt>false</tt> by default
   */
  protected boolean checkForFault(StackTraceElement[] stack) {
    return false;
  }
}