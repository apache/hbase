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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.ResourceChecker.Phase;
import org.apache.hadoop.hbase.util.JVM;

/**
 * ResourceCheckers when running JUnit tests.
 */
public final class JUnitResourceCheckers {

  private JUnitResourceCheckers() {
  }

  private static class ThreadResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    private Set<String> initialThreadNames = new HashSet<>();
    private List<String> stringsToLog = null;

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
          stringsToLog = new ArrayList<>();
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

  private static class OpenFileDescriptorResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) {
        return 0;
      }
      JVM jvm = new JVM();
      return (int) jvm.getOpenFileDescriptorCount();
    }

    @Override
    public int getMax() {
      return 1024;
    }
  }

  private static class MaxFileDescriptorResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) {
        return 0;
      }
      JVM jvm = new JVM();
      return (int) jvm.getMaxFileDescriptorCount();
    }
  }

  private static class SystemLoadAverageResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) {
        return 0;
      }
      return (int) (new JVM().getSystemLoadAverage() * 100);
    }
  }

  private static class ProcessCountResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) {
        return 0;
      }
      return new JVM().getNumberOfRunningProcess();
    }
  }

  private static class AvailableMemoryMBResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      if (!JVM.isUnix()) {
        return 0;
      }
      return (int) (new JVM().getFreeMemory() / (1024L * 1024L));
    }
  }

  public static void addResourceAnalyzer(ResourceChecker rc) {
    rc.addResourceAnalyzer(new ThreadResourceAnalyzer());
    rc.addResourceAnalyzer(new OpenFileDescriptorResourceAnalyzer());
    rc.addResourceAnalyzer(new MaxFileDescriptorResourceAnalyzer());
    rc.addResourceAnalyzer(new SystemLoadAverageResourceAnalyzer());
    rc.addResourceAnalyzer(new ProcessCountResourceAnalyzer());
    rc.addResourceAnalyzer(new AvailableMemoryMBResourceAnalyzer());
  }
}
