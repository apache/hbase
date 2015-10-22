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

import java.lang.annotation.Annotation;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

/**
 * Set a test method timeout based off the test categories small, medium, large.
 * Based on junit Timeout TestRule; see https://github.com/junit-team/junit/wiki/Rules
 */
public class CategoryBasedTimeout extends Timeout {

  @Deprecated
  public CategoryBasedTimeout(int millis) {
    super(millis);
  }

  public CategoryBasedTimeout(long timeout, TimeUnit timeUnit) {
    super(timeout, timeUnit);
  }

  protected CategoryBasedTimeout(Builder builder) {
    super(builder);
  }

  public static Builder builder() {
    return new CategoryBasedTimeout.Builder();
  }

  public static class Builder extends Timeout.Builder {
    public Timeout.Builder withTimeout(Class<?> clazz) {
      Annotation annotation = clazz.getAnnotation(Category.class);
      if (annotation != null) {
        Category category = (Category)annotation;
        for (Class<?> c: category.value()) {
          if (c == SmallTests.class) {
            // See SmallTests. Supposed to run 15 seconds.
            return withTimeout(30, TimeUnit.SECONDS);
          } else if (c == MediumTests.class) {
            // See MediumTests. Supposed to run 50 seconds.
            return withTimeout(180, TimeUnit.SECONDS);
          } else if (c == LargeTests.class) {
            // Let large tests have a ten minute timeout.
            return withTimeout(10, TimeUnit.MINUTES);
          }
        }
      }
      return this;
    }
  }
}
