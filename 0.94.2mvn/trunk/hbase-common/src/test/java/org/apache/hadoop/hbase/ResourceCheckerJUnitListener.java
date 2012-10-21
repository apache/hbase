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


import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.runner.notification.RunListener;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listen to the test progress and check the usage of:
 * - threads
 * - open file descriptor
 * - max open file descriptor
 * <p/>
 * When surefire forkMode=once/always/perthread, this code is executed on the forked process.
 */
public class ResourceCheckerJUnitListener extends RunListener {
  private Map<String, ResourceChecker> rcs = new ConcurrentHashMap<String, ResourceChecker>();

  static class ThreadResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal() {
      return Thread.getAllStackTraces().size();
    }

    @Override
    public int getMax() {
      return 500;
    }
  }

  /**
   * On unix, we know how to get the number of open file descriptor. This class allow to share
   *  the MXBeans code.
   */
  abstract static class OSResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    protected static final OperatingSystemMXBean osStats;
    protected static final UnixOperatingSystemMXBean unixOsStats;

    static {
      osStats = ManagementFactory.getOperatingSystemMXBean();
      if (osStats instanceof UnixOperatingSystemMXBean) {
        unixOsStats = (UnixOperatingSystemMXBean) osStats;
      } else {
        unixOsStats = null;
      }
    }
  }

  static class OpenFileDescriptorResourceAnalyzer extends OSResourceAnalyzer {
    @Override
    public int getVal() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return (int) unixOsStats.getOpenFileDescriptorCount();
      }
    }

    @Override
    public int getMax() {
      return 1024;
    }
  }

  static class MaxFileDescriptorResourceAnalyzer extends OSResourceAnalyzer {
    @Override
    public int getVal() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return (int) unixOsStats.getMaxFileDescriptorCount();
      }
    }
  }


  /**
   * To be implemented by sub classes if they want to add specific ResourceAnalyzer.
   */
  protected void addResourceAnalyzer(ResourceChecker rc) {
  }


  private void start(String testName) {
    ResourceChecker rc = new ResourceChecker(testName);
    rc.addResourceAnalyzer(new ThreadResourceAnalyzer());
    rc.addResourceAnalyzer(new OpenFileDescriptorResourceAnalyzer());
    rc.addResourceAnalyzer(new MaxFileDescriptorResourceAnalyzer());

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
   *
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(
      org.junit.runner.Description description) {
    final int toRemove = "org.apache.hadoop.hbase.".length();
    return description.getTestClass().getName().substring(toRemove) +
        "#" + description.getMethodName();
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

