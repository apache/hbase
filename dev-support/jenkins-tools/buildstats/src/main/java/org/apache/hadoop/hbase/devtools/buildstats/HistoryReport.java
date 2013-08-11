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
package org.apache.hadoop.hbase.devtools.buildstats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Set;

public class HistoryReport {
  private List<Integer> buildsWithTestResults;
  private Map<String, int[]> historyResults;
  private Map<Integer, Set<String>> skippedTests;

  public HistoryReport() {
    buildsWithTestResults = new ArrayList<Integer>();
    this.historyResults = new HashMap<String, int[]>();
  }

  public Map<String, int[]> getHistoryResults() {
    return this.historyResults;
  }

  public Map<Integer, Set<String>> getSkippedTests() {
    return this.skippedTests;
  }

  public List<Integer> getBuildsWithTestResults() {
    return this.buildsWithTestResults;
  }

  public void setBuildsWithTestResults(List<Integer> src) {
    this.buildsWithTestResults = src;
  }

  public void setHistoryResults(Map<String, int[]> src, Map<Integer, Set<String>> skippedTests) {
    this.skippedTests = skippedTests;
    this.historyResults = src;
  }

  public void printReport() {
    System.out.printf("%-30s", "Failed Test Cases Stats");
    for (Integer i : getBuildsWithTestResults()) {
      System.out.printf("%5d", i);
    }
    System.out.println("\n========================================================");
    SortedSet<String> keys = new TreeSet<String>(getHistoryResults().keySet());
    for (String failedTestCase : keys) {
      System.out.println();
      int[] resultHistory = getHistoryResults().get(failedTestCase);
      System.out.print(failedTestCase);
      for (int i = 0; i < resultHistory.length; i++) {
        System.out.printf("%5d", resultHistory[i]);
      }
    }
    System.out.println();

    if (skippedTests == null) return;

    System.out.printf("\n%-30s\n", "Skipped Test Cases Stats");
    for (Integer i : getBuildsWithTestResults()) {
      Set<String> tmpSkippedTests = skippedTests.get(i);
      if (tmpSkippedTests == null || tmpSkippedTests.isEmpty()) continue;
      System.out.printf("======= %d skipped(Or don't have) following test suites =======\n", i);
      for (String skippedTestcase : tmpSkippedTests) {
        System.out.println(skippedTestcase);
      }
    }
  }
}