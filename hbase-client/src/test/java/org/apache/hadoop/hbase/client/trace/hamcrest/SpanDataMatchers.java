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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.util.Objects;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Helper methods for matching against instances of {@link SpanData}.
 */
public final class SpanDataMatchers {

  private SpanDataMatchers() { }

  public static Matcher<SpanData> hasAttributes(Matcher<Attributes> matcher) {
    return new TypeSafeMatcher<SpanData>() {
      @Override protected boolean matchesSafely(SpanData item) {
        final Attributes attributes = item.getAttributes();
        return attributes != null && matcher.matches(attributes);
      }
      @Override public void describeTo(Description description) {
        description.appendText("SpanData having ").appendDescriptionOf(matcher);
      }
    };
  }

  public static Matcher<SpanData> hasEnded() {
    return new TypeSafeMatcher<SpanData>() {
      @Override protected boolean matchesSafely(SpanData item) {
        return item.hasEnded();
      }
      @Override public void describeTo(Description description) {
        description.appendText("SpanData that hasEnded");
      }
    };
  }

  public static Matcher<SpanData> hasKind(SpanKind kind) {
    return new TypeSafeMatcher<SpanData>() {
      @Override protected boolean matchesSafely(SpanData item) {
        return Objects.equals(item.getKind(), kind);
      }
      @Override public void describeTo(Description description) {
        description.appendText("SpanData with kind of ").appendValue(kind);
      }
    };
  }

  public static Matcher<SpanData> hasName(String name) {
    return hasName(equalTo(name));
  }

  public static Matcher<SpanData> hasName(Matcher<String> matcher) {
    return new TypeSafeMatcher<SpanData>() {
      @Override protected boolean matchesSafely(SpanData item) {
        final String name = item.getName();
        return name != null && matcher.matches(name);
      }
      @Override public void describeTo(Description description) {
        description.appendText("SpanData with a name that ").appendDescriptionOf(matcher);
      }
    };
  }

  public static Matcher<SpanData> hasStatusWithCode(StatusCode statusCode) {
    final Matcher<StatusCode> matcher = is(equalTo(statusCode));
    return new TypeSafeMatcher<SpanData>() {
      @Override protected boolean matchesSafely(SpanData item) {
        final StatusData statusData = item.getStatus();
        return statusData != null
          && statusData.getStatusCode() != null
          && matcher.matches(statusData.getStatusCode());
      }
      @Override public void describeTo(Description description) {
        description.appendText("SpanData with StatusCode that ").appendDescriptionOf(matcher);
      }
    };
  }
}
