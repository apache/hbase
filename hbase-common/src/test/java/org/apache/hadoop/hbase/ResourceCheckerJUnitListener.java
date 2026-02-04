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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.runner.notification.RunListener;

/**
 * Listen to the test progress and check the usage of:
 * <ul>
 * <li>threads</li>
 * <li>open file descriptor</li>
 * <li>max open file descriptor</li>
 * </ul>
 * <p>
 * When surefire forkMode=once/always/perthread, this code is executed on the forked process.
 */
public class ResourceCheckerJUnitListener extends RunListener {

  private final Map<String, ResourceChecker> rcs = new ConcurrentHashMap<>();

  /**
   * To be implemented by sub classes if they want to add specific ResourceAnalyzer.
   */
  protected void addResourceAnalyzer(ResourceChecker rc) {
  }

  private void start(String testName) {
    ResourceChecker rc = new ResourceChecker(testName);
    JUnitResourceCheckers.addResourceAnalyzer(rc);
    addResourceAnalyzer(rc);
    rcs.put(testName, rc);
    rc.start();
  }

  private void end(String testName) {
    ResourceChecker rc = rcs.remove(testName);
    assert rc != null;
    rc.end();
  }

  /**
   * Get the test name from the JUnit Description
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(org.junit.runner.Description description) {
    final int toRemove = "org.apache.hadoop.hbase.".length();
    return description.getTestClass().getName().substring(toRemove) + "#"
      + description.getMethodName();
  }

  @Override
  public void testStarted(org.junit.runner.Description description) throws java.lang.Exception {
    start(descriptionToShortTestName(description));
  }

  @Override
  public void testFinished(org.junit.runner.Description description) throws java.lang.Exception {
    end(descriptionToShortTestName(description));
  }
}
