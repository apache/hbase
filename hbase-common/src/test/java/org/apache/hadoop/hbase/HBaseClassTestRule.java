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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * The class level TestRule for all the tests. Every test class should have a {@code ClassRule} with
 * it.
 * <p>
 * For now it only sets a test method timeout based off the test categories small, medium, large.
 * Based on junit Timeout TestRule; see https://github.com/junit-team/junit/wiki/Rules
 */
@InterfaceAudience.Private
public final class HBaseClassTestRule implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClassTestRule.class);
  public static final Set<Class<?>> UNIT_TEST_CLASSES = Collections.unmodifiableSet(
      Sets.<Class<?>> newHashSet(SmallTests.class, MediumTests.class, LargeTests.class));

  // Each unit test has this timeout.
  private static long PER_UNIT_TEST_TIMEOUT_MINS = 13;

  private final Class<?> clazz;

  private final Timeout timeout;

  private final SystemExitRule systemExitRule = new SystemExitRule();

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
    // Starting JUnit 4.13, it appears that the timeout is applied across all the parameterized
    // runs. So the timeout is multiplied by number of parameterized runs.
    int numParams = getNumParameters(clazz);
    // @Category is not repeatable -- it is only possible to get an array of length zero or one.
    if (categories.length == 1) {
      for (Class<?> c : categories[0].value()) {
        if (UNIT_TEST_CLASSES.contains(c)) {
          long timeout = numParams * PER_UNIT_TEST_TIMEOUT_MINS;
          LOG.info("Test {} timeout: {} mins", clazz, timeout);
          return TimeUnit.MINUTES.toSeconds(timeout);
        }
        if (c == IntegrationTests.class) {
          return TimeUnit.MINUTES.toSeconds(Long.MAX_VALUE);
        }
      }
    }
    throw new IllegalArgumentException(
        clazz.getName() + " does not have SmallTests/MediumTests/LargeTests in @Category");
  }

  /**
   * @param clazz Test class that is running.
   * @return the number of parameters for this given test class. If the test is not parameterized or
   *   if there is any issue determining the number of parameters, returns 1.
   */
  static int getNumParameters(Class<?> clazz) {
    RunWith[] runWiths = clazz.getAnnotationsByType(RunWith.class);
    boolean testParameterized = runWiths != null && Arrays.stream(runWiths).anyMatch(
      (r) -> r.value().equals(Parameterized.class));
    if (!testParameterized) {
      return 1;
    }
    for (Method method : clazz.getMethods()) {
      if (!isParametersMethod(method)) {
        continue;
      }
      // Found the parameters method. Figure out the number of parameters.
      Object parameters;
      try {
        parameters = method.invoke(clazz);
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOG.warn("Error invoking parameters method {} in test class {}",
            method.getName(), clazz, e);
        continue;
      }
      if (parameters instanceof List) {
        return  ((List) parameters).size();
      } else if (parameters instanceof Collection) {
        return  ((Collection) parameters).size();
      } else if (parameters instanceof Iterable) {
        return Iterables.size((Iterable) parameters);
      } else if (parameters instanceof Object[]) {
        return ((Object[]) parameters).length;
      }
    }
    LOG.warn("Unable to determine parameters size. Returning the default of 1.");
    return 1;
  }

  /**
   * Helper method that checks if the input method is a valid JUnit @Parameters method.
   * @param method Input method.
   * @return true if the method is a valid JUnit parameters method, false otherwise.
   */
  private static boolean isParametersMethod(@NonNull Method method) {
    // A valid parameters method is public static and with @Parameters annotation.
    boolean methodPublicStatic = Modifier.isPublic(method.getModifiers()) &&
        Modifier.isStatic(method.getModifiers());
    Parameters[] params = method.getAnnotationsByType(Parameters.class);
    return methodPublicStatic && (params != null && params.length > 0);
  }

  public static HBaseClassTestRule forClass(Class<?> clazz) {
    return new HBaseClassTestRule(clazz, Timeout.builder().withLookingForStuckThread(true)
        .withTimeout(getTimeoutInSeconds(clazz), TimeUnit.SECONDS).build());
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return timeout.apply(systemExitRule.apply(base, description), description);
  }

}
