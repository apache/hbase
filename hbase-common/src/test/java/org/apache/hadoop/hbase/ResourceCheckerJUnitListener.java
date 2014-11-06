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



import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.ResourceChecker.Phase;
import org.apache.hadoop.hbase.util.JVM;
import org.junit.runner.notification.RunListener;

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
    private static Set<String> initialThreadNames = new HashSet<String>();
    private static List<String> stringsToLog = null;

    @Override
    public int getVal(Phase phase) {
      Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
      if (phase == Phase.INITIAL) {
        stringsToLog = null;
        for (Thread t : stackTraces.keySet()) {
          initialThreadNames.add(t.getName());
        }
      } else if (phase == Phase.END) {
        if (stackTraces.size() > initialThreadNames.size()) {
          stringsToLog = new ArrayList<String>();
          for (Thread t : stackTraces.keySet()) {
            if (!initialThreadNames.contains(t.getName())) {
              stringsToLog.add("\nPotentially hanging thread: " + t.getName() + "\n");
              StackTraceElement[] stackElements = stackTraces.get(t);
              for (StackTraceElement ele : stackElements) {
                stringsToLog.add("\t" + ele + "\n");
              }
            }
          }
        }
      }
      return stackTraces.size();
    }

    @Override
    public int getMax() {
      return 500;
    }
    
    @Override
    public List<String> getStringsToLog() {
      return stringsToLog;
    }
  }


  static class OpenFileDescriptorResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) return 0;
      JVM jvm = new JVM();
      return (int)jvm.getOpenFileDescriptorCount();
    }

    @Override
    public int getMax() {
      return 1024;
    }
  }

  static class MaxFileDescriptorResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) return 0;
      JVM jvm = new JVM();
      return (int)jvm.getMaxFileDescriptorCount();
     } 
   }

  static class SystemLoadAverageResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) return 0;
      return (int)(new JVM().getSystemLoadAverage()*100);
    }
  }

  static class ProcessCountResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) return 0;
      return new JVM().getNumberOfRunningProcess();
    }
  }

  static class AvailableMemoryMBResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) return 0;
      return (int) (new JVM().getFreeMemory() / (1024L * 1024L));
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
    rc.addResourceAnalyzer(new SystemLoadAverageResourceAnalyzer());
    rc.addResourceAnalyzer(new ProcessCountResourceAnalyzer());
    rc.addResourceAnalyzer(new AvailableMemoryMBResourceAnalyzer());

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

