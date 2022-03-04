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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.EventData;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * Helper methods for matching against instances of {@link EventData}.
 */
public final class EventMatchers {

  private EventMatchers() { }

  public static Matcher<EventData> hasAttributes(Matcher<Attributes> matcher) {
    return new FeatureMatcher<EventData, Attributes>(
      matcher, "EventData having attributes that ", "attributes") {
      @Override protected Attributes featureValueOf(EventData actual) {
        return actual.getAttributes();
      }
    };
  }

  public static Matcher<EventData> hasName(String name) {
    return hasName(equalTo(name));
  }

  public static Matcher<EventData> hasName(Matcher<String> matcher) {
    return new FeatureMatcher<EventData, String>(matcher, "EventData with a name that ", "name") {
      @Override protected String featureValueOf(EventData actual) {
        return actual.getName();
      }
    };
  }
}
