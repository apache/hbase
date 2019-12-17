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
package org.apache.hadoop.hbase.client.hamcrest;

import static org.hamcrest.core.Is.is;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Helper methods for matching against values passed through the helper methods of {@link Bytes}.
 */
public final class BytesMatchers {

  private BytesMatchers() {}

  public static Matcher<byte[]> bytesAsStringBinary(final String binary) {
    return bytesAsStringBinary(is(binary));
  }

  public static Matcher<byte[]> bytesAsStringBinary(final Matcher<String> matcher) {
    return new TypeSafeDiagnosingMatcher<byte[]>() {
      @Override protected boolean matchesSafely(byte[] item, Description mismatchDescription) {
        final String binary = Bytes.toStringBinary(item);
        if (matcher.matches(binary)) {
          return true;
        }
        mismatchDescription.appendText("was a byte[] with a Bytes.toStringBinary value ");
        matcher.describeMismatch(binary, mismatchDescription);
        return false;
      }

      @Override public void describeTo(Description description) {
        description
          .appendText("has a byte[] with a Bytes.toStringBinary value that ")
          .appendDescriptionOf(matcher);
      }
    };
  }
}
