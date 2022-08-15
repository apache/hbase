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
package org.apache.hadoop.hbase.client.trace.hamcrest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.Arrays;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Helper methods for matching against instances of {@link io.opentelemetry.api.common.Attributes}.
 */
public final class AttributesMatchers {

  private AttributesMatchers() {
  }

  public static <T> Matcher<Attributes> containsEntry(Matcher<AttributeKey<? super T>> keyMatcher,
    Matcher<? super T> valueMatcher) {
    return new IsAttributesContaining<>(keyMatcher, valueMatcher);
  }

  public static <T> Matcher<Attributes> containsEntry(AttributeKey<T> key, T value) {
    return containsEntry(equalTo(key), equalTo(value));
  }

  public static <T> Matcher<Attributes> containsEntry(AttributeKey<T> key,
    Matcher<? super T> matcher) {
    return containsEntry(equalTo(key), matcher);
  }

  public static Matcher<Attributes> containsEntry(String key, String value) {
    return containsEntry(AttributeKey.stringKey(key), value);
  }

  public static Matcher<Attributes> containsEntry(String key, long value) {
    return containsEntry(AttributeKey.longKey(key), value);
  }

  public static Matcher<Attributes> containsEntryWithStringValuesOf(String key, String... values) {
    return containsEntry(AttributeKey.stringArrayKey(key), Arrays.asList(values));
  }

  public static Matcher<Attributes> containsEntryWithStringValuesOf(String key,
    Matcher<Iterable<? extends String>> matcher) {
    return new IsAttributesContaining<>(equalTo(AttributeKey.stringArrayKey(key)), matcher);
  }

  public static Matcher<Attributes> isEmpty() {
    return hasProperty("empty", is(true));
  }

  private static final class IsAttributesContaining<T> extends TypeSafeMatcher<Attributes> {
    private final Matcher<AttributeKey<? super T>> keyMatcher;
    private final Matcher<? super T> valueMatcher;

    private IsAttributesContaining(final Matcher<AttributeKey<? super T>> keyMatcher,
      final Matcher<? super T> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    protected boolean matchesSafely(Attributes item) {
      return item.asMap().entrySet().stream().anyMatch(
        e -> allOf(hasProperty("key", keyMatcher), hasProperty("value", valueMatcher)).matches(e));
    }

    @Override
    public void describeMismatchSafely(Attributes item, Description mismatchDescription) {
      mismatchDescription.appendText("Attributes was ").appendValueList("[", ", ", "]",
        item.asMap().entrySet());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Attributes containing [").appendDescriptionOf(keyMatcher)
        .appendText("->").appendDescriptionOf(valueMatcher).appendText("]");
    }
  }
}
