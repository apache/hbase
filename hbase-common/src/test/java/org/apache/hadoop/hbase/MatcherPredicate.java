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
package org.apache.hadoop.hbase;

import java.util.function.Supplier;
import org.apache.yetus.audience.InterfaceAudience;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * An implementation of {@link Waiter.ExplainingPredicate} that uses Hamcrest {@link Matcher} for
 * both predicate evaluation and explanation.
 *
 * @param <T> The type of value to be evaluated via {@link Matcher}.
 */
@InterfaceAudience.Private
public class MatcherPredicate<T> implements Waiter.ExplainingPredicate<RuntimeException> {

  private final String reason;
  private final Supplier<T> supplier;
  private final Matcher<? super T> matcher;
  private T currentValue;

  public MatcherPredicate(final Supplier<T> supplier, final Matcher<? super T> matcher) {
    this("", supplier, matcher);
  }

  public MatcherPredicate(final String reason, final Supplier<T> supplier,
    final Matcher<? super T> matcher) {
    this.reason = reason;
    this.supplier = supplier;
    this.matcher = matcher;
    this.currentValue = null;
  }

  @Override public boolean evaluate() {
    currentValue = supplier.get();
    return matcher.matches(currentValue);
  }

  @Override public String explainFailure() {
    final Description description = new StringDescription()
      .appendText(reason)
      .appendText("\nExpected: ").appendDescriptionOf(matcher)
      .appendText("\n     but: ");
    matcher.describeMismatch(currentValue, description);
    return description.toString();
  }
}
