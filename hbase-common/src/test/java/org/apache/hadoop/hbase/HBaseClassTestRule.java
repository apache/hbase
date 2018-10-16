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
package org.apache.hadoop.hbase;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * The class level TestRule for all the tests. Every test class should have a {@code ClassRule} with
 * it.
 * <p>
 * For now it only sets a test method timeout based off the test categories small, medium, large.
 * Based on junit Timeout TestRule; see https://github.com/junit-team/junit/wiki/Rules
 */
@InterfaceAudience.Private
public final class HBaseClassTestRule implements TestRule {

  private final Class<?> clazz;

  private final Timeout timeout;

  private HBaseClassTestRule(Class<?> clazz, Timeout timeout) {
    this.clazz = clazz;
    this.timeout = timeout;
  }

  /**
   * Mainly used for {@link HBaseClassTestRuleChecker} to confirm that we use the correct
   * class to generate timeout ClassRule.
   */
  public Class<?> getClazz() {
    return clazz;
  }

  private static long getTimeoutInSeconds(Class<?> clazz) {
    Category[] categories = clazz.getAnnotationsByType(Category.class);
    for (Class<?> c : categories[0].value()) {
      if (c == SmallTests.class || c == MediumTests.class || c == LargeTests.class) {
        // All tests have a 10minute timeout.
        return TimeUnit.MINUTES.toSeconds(13);
      }
      if (c == IntegrationTests.class) {
        return TimeUnit.MINUTES.toSeconds(Long.MAX_VALUE);
      }
    }
    throw new IllegalArgumentException(
        clazz.getName() + " does not have SmallTests/MediumTests/LargeTests in @Category");
  }

  public static HBaseClassTestRule forClass(Class<?> clazz) {
    return new HBaseClassTestRule(clazz, Timeout.builder().withLookingForStuckThread(true)
        .withTimeout(getTimeoutInSeconds(clazz), TimeUnit.SECONDS).build());
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return timeout.apply(base, description);
  }
}
