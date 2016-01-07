/*
 *
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


/**
 * Class that implements a JUnit rule to be called before and after each
 *  test method to check the resources used:
 *   - file descriptors
 *   - threads
 *  @see ResourceChecker
 */
public class ResourceCheckerJUnitRule extends org.junit.rules.TestWatcher {
  private ResourceChecker cu;
  private boolean endDone;

  /**
   * To be called before the test methods
   * @param testName
   */
  private void start(String testName) {
    cu = new ResourceChecker("before "+testName);
    endDone = false;
  }

  /**
   * To be called after the test methods
   * @param testName
   */
  private void end(String testName) {
    if (!endDone) {
      endDone = true;
      cu.logInfo(ResourceChecker.Phase.END, "after " + testName);
      cu.check("after "+testName);
    }
  }

  /**
   * Get the test name from the JUnit Description
   * @param description
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(
    org.junit.runner.Description description) {
    final int toRemove = "org.apache.hadoop.hbase.".length();
    return description.getTestClass().getName().substring(toRemove) +
      "#" + description.getMethodName();
  }

  @Override
  protected void succeeded(org.junit.runner.Description description) {
    end(descriptionToShortTestName(description));
  }

  @Override
  protected void failed(java.lang.Throwable e, org.junit.runner.Description description) {
    end(descriptionToShortTestName(description));
  }

  @Override
  protected void starting(org.junit.runner.Description description) {
    start(descriptionToShortTestName(description));
  }

  @Override
  protected void finished(org.junit.runner.Description description) {
    end(descriptionToShortTestName(description));
  }
}

